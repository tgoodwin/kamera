package explore

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/interactive"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
)

type ParallelOptions struct {
	MaxParallel int
	DumpDir     string
}

type processJob struct {
	JobIndex     int
	InputIndex   int
	TrialIndex   int
	ScenarioName string
}

type childProcessRequest struct {
	JobIndex   int
	InputIndex int
	TrialIndex int
	CWD        string
	Args       []string
}

type childProcessResult struct {
	JobIndex   int
	InputIndex int
	TrialIndex int
	Stdout     string
	Stderr     string
	Err        error
}

type ParallelRunner struct {
	builder *tracecheck.ExplorerBuilder

	loadInputsFn     func(path string) ([]coverage.Input, error)
	parentArgsFn     func() []string
	cwdFn            func() (string, error)
	checkModuleDirFn func(cwd string) error
	spawnChildFn     func(ctx context.Context, req childProcessRequest) childProcessResult
}

// NewParallelRunner constructs a runner that can execute scenarios concurrently.
func NewParallelRunner(builder *tracecheck.ExplorerBuilder) (*ParallelRunner, error) {
	if builder == nil {
		return nil, fmt.Errorf("builder is nil")
	}
	return &ParallelRunner{
		builder:          builder,
		loadInputsFn:     coverage.LoadInputs,
		parentArgsFn:     func() []string { return os.Args },
		cwdFn:            os.Getwd,
		checkModuleDirFn: ensureGoRunModuleDir,
		spawnChildFn:     spawnGoRunChild,
	}, nil
}

type scenarioJob struct {
	idx      int
	scenario Scenario
}

type scenarioResult struct {
	idx    int
	result ScenarioResult
}

// RunAll executes scenarios in parallel, returning results in the same order as input.
func (r *ParallelRunner) RunAll(ctx context.Context, scenarios []Scenario, opts ParallelOptions) ([]ScenarioResult, error) {
	if r == nil || r.builder == nil {
		return nil, fmt.Errorf("parallel runner: builder is required")
	}
	if ParallelChildIndex() >= 0 && !ParallelProcessesEnabled() {
		return nil, fmt.Errorf("--parallel-child-index requires --parallel-processes")
	}
	if ParallelChildTrialIndex() != 0 && !ParallelProcessesEnabled() {
		return nil, fmt.Errorf("--parallel-child-trial-index requires --parallel-processes")
	}
	if ParallelChildJobIndex() >= 0 && !ParallelProcessesEnabled() {
		return nil, fmt.Errorf("--parallel-child-job-index requires --parallel-processes")
	}
	// the simclock package is not thread-safe, so for use cases that rely on simclock, we support
	// a process-isolation mode that launches separate executions for each scenario and aggregates results via disk.
	if ParallelProcessesEnabled() {
		if strings.TrimSpace(InputsPath()) == "" {
			return nil, fmt.Errorf("--parallel-processes requires explicit --inputs <file>")
		}
		if ParallelChildIndex() >= 0 {
			return r.runChildMode(ctx, scenarios, opts)
		}
		return r.runSupervisorMode(ctx, scenarios, opts)
	}
	return r.runInProcess(ctx, scenarios, opts)
}

// runSupervisorMode launches child processes for each scenario and aggregates results.
// it expects the child processes to write results to disk and does not enforce any ordering guarantees on completion.
func (r *ParallelRunner) runSupervisorMode(ctx context.Context, scenarios []Scenario, opts ParallelOptions) ([]ScenarioResult, error) {
	if err := ensureParallelOutputDirs(opts); err != nil {
		return nil, err
	}

	inputs, err := r.loadInputsFn(InputsPath())
	if err != nil {
		return nil, fmt.Errorf("load inputs for process supervisor: %w", err)
	}
	if len(inputs) == 0 {
		return nil, fmt.Errorf("inputs file contains no scenarios")
	}

	cwd, err := r.cwdFn()
	if err != nil {
		return nil, fmt.Errorf("resolve working directory: %w", err)
	}
	if err := r.checkModuleDirFn(cwd); err != nil {
		return nil, err
	}

	childArgsBase, err := buildSupervisorChildArgs(r.parentArgsFn())
	if err != nil {
		return nil, err
	}

	jobs, err := buildProcessJobs(inputs, scenarios, InputsPath())
	if err != nil {
		return nil, fmt.Errorf("build process jobs: %w", err)
	}

	total := len(jobs)
	if total == 0 {
		return []ScenarioResult{}, nil
	}
	results := make([]ScenarioResult, total)

	maxParallel := opts.MaxParallel
	if maxParallel <= 0 {
		maxParallel = runtime.GOMAXPROCS(0)
	}
	if maxParallel > total {
		maxParallel = total
	}

	fmt.Printf("parallel-processes supervisor: starting %d child process(es) (max_parallel=%d)\n", total, maxParallel)

	jobCh := make(chan processJob)
	resCh := make(chan childProcessResult, total)

	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for job := range jobCh {
			args := append([]string{}, childArgsBase...)
			args = append(args, fmt.Sprintf("--parallel-child-index=%d", job.InputIndex))
			args = append(args, fmt.Sprintf("--parallel-child-trial-index=%d", job.TrialIndex))
			args = append(args, fmt.Sprintf("--parallel-child-job-index=%d", job.JobIndex))
			resCh <- r.spawnChildFn(ctx, childProcessRequest{
				JobIndex:   job.JobIndex,
				InputIndex: job.InputIndex,
				TrialIndex: job.TrialIndex,
				CWD:        cwd,
				Args:       args,
			})
		}
	}

	for i := 0; i < maxParallel; i++ {
		wg.Add(1)
		go worker()
	}

	go func() {
		started := 0
		for _, job := range jobs {
			started++
			fmt.Printf(
				"parallel-processes supervisor: start child job=%d input=%d trial=%d (%d/%d)\n",
				job.JobIndex,
				job.InputIndex,
				job.TrialIndex,
				started,
				total,
			)
			jobCh <- job
		}
		close(jobCh)
	}()

	go func() {
		wg.Wait()
		close(resCh)
	}()

	completed := 0
	failed := 0
	failedIdx := make([]int, 0)
	for item := range resCh {
		completed++
		if item.JobIndex < 0 || item.JobIndex >= len(jobs) {
			failed++
			failedIdx = append(failedIdx, item.JobIndex)
			fmt.Printf("parallel-processes supervisor: child job=%d returned invalid job index (%d/%d)\n", item.JobIndex, completed, total)
			continue
		}
		job := jobs[item.JobIndex]
		name := strings.TrimSpace(job.ScenarioName)
		if name == "" {
			name = fallbackInputName(inputs, job.InputIndex)
		}
		if job.TrialIndex > 0 {
			name = fmt.Sprintf("%s#trial-%d", name, job.TrialIndex)
		}
		result := ScenarioResult{
			Name: name,
			Err:  item.Err,
		}
		results[item.JobIndex] = result

		if item.Err != nil {
			failed++
			failedIdx = append(failedIdx, item.JobIndex)
			fmt.Printf(
				"parallel-processes supervisor: child job=%d input=%d trial=%d failed (%d/%d): %s\n",
				item.JobIndex,
				job.InputIndex,
				job.TrialIndex,
				completed,
				total,
				summarizeChildFailure(item),
			)
			continue
		}

		fmt.Printf(
			"parallel-processes supervisor: child job=%d input=%d trial=%d completed (%d/%d)\n",
			item.JobIndex,
			job.InputIndex,
			job.TrialIndex,
			completed,
			total,
		)
	}

	if failed > 0 {
		sort.Ints(failedIdx)
		return results, fmt.Errorf(
			"parallel-processes supervisor: %d/%d child runs failed; failed child indices: %s",
			failed,
			total,
			joinIntList(failedIdx),
		)
	}

	fmt.Printf("parallel-processes supervisor: all children completed (%d/%d)\n", completed, total)
	_ = scenarios // reserved for future parity checks
	return results, nil
}

// runChildMode executes a single scenario based on the child index
func (r *ParallelRunner) runChildMode(ctx context.Context, scenarios []Scenario, opts ParallelOptions) ([]ScenarioResult, error) {
	opts = childParallelOptions(opts)
	if err := ensureParallelOutputDirs(opts); err != nil {
		return nil, err
	}

	inputs, err := r.loadInputsFn(InputsPath())
	if err != nil {
		return nil, fmt.Errorf("parallel child load inputs: %w", err)
	}

	childIdx := ParallelChildIndex()
	trialIdx := ParallelChildTrialIndex()
	jobIdx := ParallelChildJobIndex()
	if trialIdx < 0 {
		return nil, fmt.Errorf("parallel child trial index must be >= 0, got %d", trialIdx)
	}
	_, selected, err := selectScenarioForChild(inputs, scenarios, InputsPath(), childIdx)
	if err != nil {
		result := ScenarioResult{
			Name: fallbackInputName(inputs, childIdx),
			Err:  err,
		}
		dumpPath, dumpErr := writeChildFailureDump(
			opts,
			result.Name,
			ScenarioContext{
				Workflow: "parallel-process-child",
				InputRef: inputRefForIndex(InputsPath(), childIdx),
			},
			jobIdxOrInput(jobIdx, childIdx),
			"select_scenario",
			err,
		)
		if dumpErr != nil {
			result.Err = fmt.Errorf("%w (and failed to write failure dump: %v)", result.Err, dumpErr)
		}
		result.DumpPath = dumpPath
		return []ScenarioResult{result}, result.Err
	}

	selected = applyMonteCarloTrialConfig(selected, childIdx, trialIdx)

	fmt.Printf("parallel-processes child: running input index %d trial %d as scenario %q\n", childIdx, trialIdx, selected.Name)
	result := r.runScenario(ctx, selected, opts, jobIdxOrInput(jobIdx, childIdx))
	if result.Err != nil {
		if result.DumpPath == "" {
			dumpPath, dumpErr := writeChildFailureDump(opts, selected.Name, selected.Context, jobIdxOrInput(jobIdx, childIdx), "run_scenario", result.Err)
			if dumpErr != nil {
				result.Err = fmt.Errorf("%w (and failed to write failure dump: %v)", result.Err, dumpErr)
			}
			result.DumpPath = dumpPath
		}
		return []ScenarioResult{result}, fmt.Errorf("parallel child index %d trial %d failed: %w", childIdx, trialIdx, result.Err)
	}
	return []ScenarioResult{result}, nil
}

// runInProcess executes all scenarios concurrently within the same process using goroutines.
func (r *ParallelRunner) runInProcess(ctx context.Context, scenarios []Scenario, opts ParallelOptions) ([]ScenarioResult, error) {
	if err := ensureParallelOutputDirs(opts); err != nil {
		return nil, err
	}

	results := make([]ScenarioResult, len(scenarios))
	if len(scenarios) == 0 {
		return results, nil
	}

	maxParallel := opts.MaxParallel
	if maxParallel <= 0 {
		maxParallel = runtime.GOMAXPROCS(0)
	}

	jobs := make(chan scenarioJob)
	resCh := make(chan scenarioResult, len(scenarios))

	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for job := range jobs {
			res := r.runScenario(ctx, job.scenario, opts, job.idx)
			resCh <- scenarioResult{idx: job.idx, result: res}
		}
	}

	for i := 0; i < maxParallel; i++ {
		wg.Add(1)
		go worker()
	}

	go func() {
		wg.Wait()
		close(resCh)
	}()

	for idx, scenario := range scenarios {
		jobs <- scenarioJob{idx: idx, scenario: scenario}
	}
	close(jobs)

	for item := range resCh {
		results[item.idx] = item.result
	}

	return results, nil
}

func (r *ParallelRunner) runScenario(ctx context.Context, scenario Scenario, opts ParallelOptions, idx int) ScenarioResult {
	result := ScenarioResult{Name: scenario.Name}
	if ctx.Err() != nil {
		result.Err = ctx.Err()
		return result
	}

	seed, err := r.builder.BuildRestartSeed(scenario.EnvironmentState)
	if err != nil {
		result.Err = fmt.Errorf("build restart seed: %w", err)
		return result
	}

	if scenario.ClosedLoop == nil || !PerturbEnabled() {
		phase := r.runScenarioPhase(ctx, scenario, opts, idx, "", scenario.Config, seed, nil, nil, scenario.Context)
		result.Phases = []ScenarioPhaseResult{phase}
		applyPhaseSummary(&result, phase)
		return result
	}

	reference := r.runScenarioPhase(ctx, scenario, opts, idx, "reference", scenario.Config, seed, nil, nil, scenario.Context)
	result.Phases = append(result.Phases, reference)
	applyPhaseSummary(&result, reference)
	if reference.Err != nil {
		return result
	}

	plans := []ScenarioPhasePlan{}
	if scenario.ClosedLoop.Plan != nil {
		planned, planErr := scenario.ClosedLoop.Plan(reference)
		if planErr != nil {
			result.Err = fmt.Errorf("closed-loop plan for scenario %s: %w", scenario.Name, planErr)
			return result
		}
		plans = planned
	}

	for i, plan := range plans {
		phaseName := strings.TrimSpace(plan.Name)
		if phaseName == "" {
			phaseName = fmt.Sprintf("phase_%d", i+1)
		}
		phaseCtx := scenario.Context
		if plan.Context != nil {
			phaseCtx = *plan.Context
		}
		runSeed := seed
		if plan.Seed != nil {
			runSeed = *plan.Seed
		}
		phase := r.runScenarioPhase(ctx, scenario, opts, idx, phaseName, plan.Config, runSeed, plan.Prefix, reference.VersionManager, phaseCtx)
		result.Phases = append(result.Phases, phase)
		applyPhaseSummary(&result, phase)
		if phase.Err != nil {
			return result
		}
	}

	return result
}

func applyPhaseSummary(result *ScenarioResult, phase ScenarioPhaseResult) {
	if result == nil {
		return
	}
	result.Result = phase.Result
	result.VersionManager = phase.VersionManager
	result.Stats = phase.Stats
	result.DumpPath = phase.DumpPath
	result.InvariantError = phase.InvariantError
	result.Err = phase.Err
}

func (r *ParallelRunner) runScenarioPhase(
	ctx context.Context,
	scenario Scenario,
	opts ParallelOptions,
	idx int,
	phaseName string,
	cfg tracecheck.ExploreConfig,
	seed tracecheck.RestartSeed,
	prefix tracecheck.ExecutionHistory,
	prefixResolver tracecheck.VersionManager,
	phaseCtx ScenarioContext,
) ScenarioPhaseResult {
	phaseLabel := strings.TrimSpace(phaseName)
	if phaseLabel == "" {
		phaseLabel = "run"
	}
	phase := ScenarioPhaseResult{Name: phaseLabel}

	fork := r.builder.Fork()
	if fork == nil {
		phase.Err = fmt.Errorf("fork builder: nil")
		return phase
	}
	fork.WithUserActions(cloneUserActions(scenario.UserInputs))
	fork.SetConfig(cfg)
	if len(prefix) > 0 && prefixResolver != nil {
		if err := fork.PrimeVersionStoreFromHistory(prefix, prefixResolver); err != nil {
			phase.Err = fmt.Errorf("prime prefix history store: %w", err)
			return phase
		}
	}

	startState, err := tracecheck.SeedToStateNode(seed, fork)
	if err != nil {
		phase.Err = fmt.Errorf("seed to state: %w", err)
		return phase
	}
	if len(prefix) > 0 {
		startState.ExecutionHistory = slices.Clone(prefix)
	}

	explorer, err := fork.Build("standalone")
	if err != nil {
		phase.Err = fmt.Errorf("build explorer: %w", err)
		return phase
	}

	runCtx := ctx
	if explorer.Config.Timeout > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, explorer.Config.Timeout)
		defer cancel()
	}

	res := explorer.Explore(runCtx, startState)
	phase.Result = res
	phase.VersionManager = explorer.VersionManager()
	phase.Stats = explorer.Stats()

	if scenario.Invariant != nil && res != nil {
		for _, state := range res.ConvergedStates {
			if err := scenario.Invariant(state.State); err != nil {
				phase.InvariantError = err
				break
			}
		}
	}

	artifactName := scenario.Name
	if strings.TrimSpace(phaseName) != "" {
		artifactName = fmt.Sprintf("%s/%s", scenario.Name, phaseLabel)
	}

	if opts.DumpDir != "" && res != nil {
		states := append([]tracecheck.ResultState{}, res.ConvergedStates...)
		states = append(states, res.AbortedStates...)
		if len(states) > 0 {
			path := scenarioDumpPath(opts.DumpDir, artifactName, idx)
			runIdx := idx
			attrs := copyAttributes(phaseCtx.Attributes)
			if strings.TrimSpace(phaseName) != "" {
				attrs["phase"] = phaseLabel
			}
			addMonteCarloDumpAttributes(attrs, cfg)
			if len(attrs) == 0 {
				attrs = nil
			}
			var dumpStats *tracecheck.ExploreStats
			if cfg.RecordPerfStats {
				dumpStats = phase.Stats
			}
			dumpContext := &interactive.InspectorDumpContext{
				ScenarioName:     scenario.Name,
				ScenarioRunIndex: &runIdx,
				Workflow:         phaseCtx.Workflow,
				InputRef:         phaseCtx.InputRef,
				Attributes:       attrs,
			}
			if err := interactive.SaveInspectorDumpWithContextAndStats(states, phase.VersionManager, path, dumpContext, dumpStats); err != nil {
				phase.Err = fmt.Errorf("dump scenario %s (%s): %w", scenario.Name, phaseLabel, err)
				return phase
			}
			phase.DumpPath = path
		}
	}

	return phase
}

func ensureParallelOutputDirs(opts ParallelOptions) error {
	if opts.DumpDir != "" {
		if err := os.MkdirAll(opts.DumpDir, 0o755); err != nil {
			return fmt.Errorf("create dump dir: %w", err)
		}
	}
	return nil
}

func childParallelOptions(opts ParallelOptions) ParallelOptions {
	opts.MaxParallel = 1
	return opts
}

func buildProcessJobs(inputs []coverage.Input, scenarios []Scenario, inputsPath string) ([]processJob, error) {
	jobs := make([]processJob, 0, len(inputs))
	for inputIdx := range inputs {
		_, scenario, err := selectScenarioForChild(inputs, scenarios, inputsPath, inputIdx)
		if err != nil {
			return nil, err
		}
		trials := scenarioTrialCount(scenario.Config)
		for trialIdx := 0; trialIdx < trials; trialIdx++ {
			jobs = append(jobs, processJob{
				JobIndex:     len(jobs),
				InputIndex:   inputIdx,
				TrialIndex:   trialIdx,
				ScenarioName: scenario.Name,
			})
		}
	}
	return jobs, nil
}

func scenarioTrialCount(cfg tracecheck.ExploreConfig) int {
	if cfg.SearchMode != tracecheck.SearchModeMonteCarlo {
		return 1
	}
	if cfg.MonteCarlo.Trials <= 0 {
		return 1
	}
	return cfg.MonteCarlo.Trials
}

func applyMonteCarloTrialConfig(scenario Scenario, inputIdx int, trialIdx int) Scenario {
	cfg := scenario.Config
	if cfg.SearchMode != tracecheck.SearchModeMonteCarlo {
		return scenario
	}

	cfg.MonteCarlo.TrialIndex = trialIdx
	if cfg.MonteCarlo.Trials <= 0 {
		cfg.MonteCarlo.Trials = 1
	}
	if strings.TrimSpace(cfg.MonteCarlo.ScenarioGroup) == "" {
		cfg.MonteCarlo.ScenarioGroup = monteCarloScenarioGroup(scenario, inputIdx)
	}
	scenario.Config = cfg
	return scenario
}

func monteCarloScenarioGroup(scenario Scenario, inputIdx int) string {
	if fromInputRef := strings.TrimSpace(scenario.Context.InputRef); fromInputRef != "" {
		return fromInputRef
	}
	return fmt.Sprintf("%s#%d", strings.TrimSpace(scenario.Name), inputIdx)
}

func jobIdxOrInput(jobIdx int, inputIdx int) int {
	if jobIdx >= 0 {
		return jobIdx
	}
	return inputIdx
}

func addMonteCarloDumpAttributes(attrs map[string]string, cfg tracecheck.ExploreConfig) {
	if cfg.SearchMode != tracecheck.SearchModeMonteCarlo {
		return
	}
	if attrs == nil {
		return
	}
	attrs["search_mode"] = string(tracecheck.SearchModeMonteCarlo)
	if group := strings.TrimSpace(cfg.MonteCarlo.ScenarioGroup); group != "" {
		attrs["mc_group_id"] = group
	}
	attrs["mc_trial_index"] = strconv.Itoa(cfg.MonteCarlo.TrialIndex)
	if cfg.MonteCarlo.Trials > 0 {
		attrs["mc_trial_count"] = strconv.Itoa(cfg.MonteCarlo.Trials)
	}
	attrs["mc_seed"] = strconv.FormatInt(cfg.MonteCarlo.DerivedSeed(), 10)
	if strings.TrimSpace(attrs["mc_role"]) == "" {
		attrs["mc_role"] = "trial"
	}
}

func selectScenarioForChild(inputs []coverage.Input, scenarios []Scenario, inputsPath string, childIdx int) (int, Scenario, error) {
	if childIdx < 0 {
		return -1, Scenario{}, fmt.Errorf("parallel child index must be >= 0, got %d", childIdx)
	}
	if childIdx >= len(inputs) {
		return -1, Scenario{}, fmt.Errorf("parallel child index %d out of range (inputs=%d)", childIdx, len(inputs))
	}

	inputName := strings.TrimSpace(inputs[childIdx].Name)
	if inputName == "" {
		return -1, Scenario{}, fmt.Errorf("input[%d] has empty name", childIdx)
	}

	matches := make([]int, 0)
	for idx, scenario := range scenarios {
		if scenarioBelongsToInput(scenario, inputsPath, inputName, childIdx) {
			matches = append(matches, idx)
		}
	}

	if len(matches) != 1 {
		return -1, Scenario{}, fmt.Errorf(
			"parallel child index %d expected exactly one scenario for input %q; found %d",
			childIdx,
			inputName,
			len(matches),
		)
	}
	return matches[0], scenarios[matches[0]], nil
}

func scenarioBelongsToInput(scenario Scenario, inputsPath string, inputName string, inputIdx int) bool {
	if idx, ok := scenarioInputIndexFromAttributes(scenario); ok {
		return idx == inputIdx
	}
	if matched, decided := scenarioInputRefMatch(scenario.Context.InputRef, inputsPath, inputName, inputIdx); decided {
		return matched
	}

	name := strings.TrimSpace(scenario.Name)
	if name == inputName {
		return true
	}
	return strings.HasPrefix(name, inputName+"/")
}

func scenarioInputIndexFromAttributes(scenario Scenario) (int, bool) {
	for _, key := range []string{"input_index", "inputIndex"} {
		value := strings.TrimSpace(scenario.Context.Attributes[key])
		if value == "" {
			continue
		}
		idx, err := strconv.Atoi(value)
		if err != nil {
			return 0, false
		}
		return idx, true
	}
	return 0, false
}

func scenarioInputRefMatch(ref string, inputsPath string, inputName string, inputIdx int) (bool, bool) {
	trimmed := strings.TrimSpace(ref)
	if trimmed == "" {
		return false, false
	}

	base := trimmed
	token := ""
	if before, after, ok := strings.Cut(trimmed, "#"); ok {
		base = strings.TrimSpace(before)
		token = strings.TrimSpace(after)
	}

	if inputsPath != "" && base != "" && base != inputsPath {
		return false, true
	}

	if token == "" {
		return trimmed == inputName, true
	}
	if idx, err := strconv.Atoi(token); err == nil {
		return idx == inputIdx, true
	}
	return token == inputName, true
}

func writeChildFailureDump(
	opts ParallelOptions,
	scenarioName string,
	scenarioCtx ScenarioContext,
	childIdx int,
	phase string,
	runErr error,
) (string, error) {
	if strings.TrimSpace(opts.DumpDir) == "" {
		return "", nil
	}

	name := strings.TrimSpace(scenarioName)
	if name == "" {
		name = fmt.Sprintf("input_%d", childIdx)
	}
	path := scenarioDumpPath(opts.DumpDir, name, childIdx)
	runIdx := childIdx

	attrs := copyAttributes(scenarioCtx.Attributes)
	attrs["status"] = "error"
	attrs["error_phase"] = strings.TrimSpace(phase)
	attrs["error_message"] = trimErrorMessage(runErr, 512)

	ctx := &interactive.InspectorDumpContext{
		ScenarioName:     name,
		ScenarioRunIndex: &runIdx,
		Workflow:         scenarioCtx.Workflow,
		InputRef:         scenarioCtx.InputRef,
		Attributes:       attrs,
	}

	if err := interactive.SaveInspectorDumpWithContext(nil, nil, path, ctx); err != nil {
		return "", fmt.Errorf("dump failure context for scenario %s: %w", name, err)
	}
	return path, nil
}

func fallbackInputName(inputs []coverage.Input, childIdx int) string {
	if childIdx >= 0 && childIdx < len(inputs) {
		if name := strings.TrimSpace(inputs[childIdx].Name); name != "" {
			return name
		}
	}
	return fmt.Sprintf("input_%d", childIdx)
}

func inputRefForIndex(inputsPath string, childIdx int) string {
	if strings.TrimSpace(inputsPath) == "" {
		return ""
	}
	return fmt.Sprintf("%s#%d", inputsPath, childIdx)
}

func copyAttributes(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func trimErrorMessage(err error, maxLen int) string {
	if err == nil {
		return ""
	}
	msg := strings.TrimSpace(err.Error())
	if maxLen <= 0 || len(msg) <= maxLen {
		return msg
	}
	return strings.TrimSpace(msg[:maxLen])
}

func ensureGoRunModuleDir(cwd string) error {
	modPath := filepath.Join(cwd, "go.mod")
	info, err := os.Stat(modPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("--parallel-processes requires running from a harness module directory with go.mod (missing %s)", modPath)
		}
		return fmt.Errorf("check module directory: %w", err)
	}
	if info.IsDir() {
		return fmt.Errorf("--parallel-processes requires go.mod file, found directory at %s", modPath)
	}
	return nil
}

func buildSupervisorChildArgs(parentArgs []string) ([]string, error) {
	if len(parentArgs) == 0 {
		return nil, fmt.Errorf("cannot launch process supervisor child: parent args unavailable")
	}

	args := append([]string{}, parentArgs[1:]...)
	args = stripChildIndexArgs(args)
	if !hasParallelProcessesFlag(args) {
		args = append(args, "--parallel-processes")
	}
	return args, nil
}

func stripChildIndexArgs(args []string) []string {
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if strings.HasPrefix(arg, "--parallel-child-index=") {
			continue
		}
		if arg == "--parallel-child-index" {
			if i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				i++
			}
			continue
		}
		if strings.HasPrefix(arg, "--parallel-child-trial-index=") {
			continue
		}
		if arg == "--parallel-child-trial-index" {
			if i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				i++
			}
			continue
		}
		if strings.HasPrefix(arg, "--parallel-child-job-index=") {
			continue
		}
		if arg == "--parallel-child-job-index" {
			if i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				i++
			}
			continue
		}
		out = append(out, arg)
	}
	return out
}

func hasParallelProcessesFlag(args []string) bool {
	for _, arg := range args {
		if arg == "--parallel-processes" || strings.HasPrefix(arg, "--parallel-processes=") {
			return true
		}
	}
	return false
}

func spawnGoRunChild(ctx context.Context, req childProcessRequest) childProcessResult {
	goArgs := append([]string{"run", "."}, req.Args...)
	cmd := exec.CommandContext(ctx, "go", goArgs...)
	cmd.Dir = req.CWD

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	return childProcessResult{
		JobIndex:   req.JobIndex,
		InputIndex: req.InputIndex,
		TrialIndex: req.TrialIndex,
		Stdout:     stdout.String(),
		Stderr:     stderr.String(),
		Err:        err,
	}
}

func summarizeChildFailure(res childProcessResult) string {
	output := strings.TrimSpace(res.Stderr)
	if output == "" {
		output = strings.TrimSpace(res.Stdout)
	}
	if output != "" {
		lines := strings.Split(output, "\n")
		output = strings.TrimSpace(lines[len(lines)-1])
	}
	if output == "" && res.Err != nil {
		output = strings.TrimSpace(res.Err.Error())
	}
	if output == "" {
		return "unknown failure"
	}
	if res.Err == nil {
		return output
	}
	if strings.Contains(output, res.Err.Error()) {
		return output
	}
	return fmt.Sprintf("%s: %s", strings.TrimSpace(res.Err.Error()), output)
}

func joinIntList(values []int) string {
	if len(values) == 0 {
		return ""
	}
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = strconv.Itoa(v)
	}
	return strings.Join(parts, ",")
}

func scenarioDumpPath(dir, name string, idx int) string {
	base := scenarioFileBase(name, idx)
	return filepath.Join(dir, base+".jsonl")
}

func scenarioFileBase(name string, idx int) string {
	base := sanitizeScenarioName(name)
	if idx >= 0 {
		return fmt.Sprintf("%s_%d", base, idx)
	}
	return base
}

func sanitizeScenarioName(name string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return "scenario"
	}
	lower := strings.ToLower(trimmed)
	var b strings.Builder
	b.Grow(len(lower))
	underscore := false
	for _, r := range lower {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			underscore = false
			continue
		}
		if !underscore {
			b.WriteByte('_')
			underscore = true
		}
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		out = "scenario"
	}
	const maxLen = 64
	if len(out) > maxLen {
		out = strings.TrimRight(out[:maxLen], "_")
		out = strings.TrimRight(out, "_")
		if out == "" {
			out = "scenario"
		}
	}
	return out
}
