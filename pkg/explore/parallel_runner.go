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
	"time"

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
	JobIndex     int
	InputIndex   int
	TrialIndex   int
	InvocationID string
	CWD          string
	Args         []string
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
	invocationID := resolveInvocationID()
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
			return r.runChildMode(ctx, scenarios, opts, invocationID)
		}
		return r.runSupervisorMode(ctx, scenarios, opts, invocationID)
	}
	return r.runInProcess(ctx, scenarios, opts, invocationID)
}

// runSupervisorMode launches child processes for each scenario and aggregates results.
// it expects the child processes to write results to disk and does not enforce any ordering guarantees on completion.
func (r *ParallelRunner) runSupervisorMode(ctx context.Context, scenarios []Scenario, opts ParallelOptions, invocationID string) ([]ScenarioResult, error) {
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
				JobIndex:     job.JobIndex,
				InputIndex:   job.InputIndex,
				TrialIndex:   job.TrialIndex,
				InvocationID: invocationID,
				CWD:          cwd,
				Args:         args,
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
			"parallel-processes supervisor: %d/%d child runs failed; failed job indices: %s",
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
func (r *ParallelRunner) runChildMode(ctx context.Context, scenarios []Scenario, opts ParallelOptions, invocationID string) ([]ScenarioResult, error) {
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
			invocationID,
		)
		if dumpErr != nil {
			result.Err = fmt.Errorf("%w (and failed to write failure dump: %v)", result.Err, dumpErr)
		}
		result.DumpPath = dumpPath
		return []ScenarioResult{result}, result.Err
	}

	selected = applyMonteCarloTrialConfig(selected, childIdx, trialIdx)

	fmt.Printf("parallel-processes child: running input index %d trial %d as scenario %q\n", childIdx, trialIdx, selected.Name)
	result := r.runScenario(ctx, selected, opts, jobIdxOrInput(jobIdx, childIdx), invocationID)
	if result.Err != nil {
		if result.DumpPath == "" {
			dumpPath, dumpErr := writeChildFailureDump(opts, selected.Name, selected.Context, jobIdxOrInput(jobIdx, childIdx), "run_scenario", result.Err, invocationID)
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
func (r *ParallelRunner) runInProcess(ctx context.Context, scenarios []Scenario, opts ParallelOptions, invocationID string) ([]ScenarioResult, error) {
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
			res := r.runScenario(ctx, job.scenario, opts, job.idx, invocationID)
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

func (r *ParallelRunner) runScenario(ctx context.Context, scenario Scenario, opts ParallelOptions, idx int, invocationID string) ScenarioResult {
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

	if !PerturbEnabled() {
		phase := r.runScenarioPhase(ctx, scenario, opts, idx, "", scenario.Config, seed, nil, nil, scenario.Context, invocationID)
		result.Phases = []ScenarioPhaseResult{phase}
		applyPhaseSummary(&result, phase)
		return result
	}

	referenceConfig := scenario.Config
	planFn := func(reference ScenarioPhaseResult) ([]ScenarioPhasePlan, error) {
		return nil, nil
	}
	if scenario.ClosedLoop == nil {
		// Naive v1 auto-closed-loop policy:
		// 1) run an unperturbed reference phase
		// 2) run one broad rerun phase derived from the reference trace
		//
		// This intentionally favors "get end-to-end scaffolding running" over
		// precision/attribution. Harnesses can still override with ClosedLoop.Plan.
		referenceConfig = disablePerturbations(scenario.Config)
		planFn = func(reference ScenarioPhaseResult) ([]ScenarioPhasePlan, error) {
			if len(scenario.UserInputs) >= 2 {
				return buildUserActionInterleavingPlans(reference, scenario.Config, scenario.Context, scenario.UserInputs), nil
			}
			return buildDefaultScenarioRerunPlans(reference, scenario.Config, scenario.Context), nil
		}
	} else if scenario.ClosedLoop.Plan != nil {
		planFn = scenario.ClosedLoop.Plan
	}

	reference := r.runScenarioPhase(ctx, scenario, opts, idx, "reference", referenceConfig, seed, nil, nil, scenario.Context, invocationID)
	result.Phases = append(result.Phases, reference)
	applyPhaseSummary(&result, reference)
	if reference.Err != nil {
		return result
	}

	plans, planErr := planFn(reference)
	if planErr != nil {
		result.Err = fmt.Errorf("closed-loop plan for scenario %s: %w", scenario.Name, planErr)
		return result
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
		phase := r.runScenarioPhase(ctx, scenario, opts, idx, phaseName, plan.Config, runSeed, plan.Prefix, reference.VersionManager, phaseCtx, invocationID)
		result.Phases = append(result.Phases, phase)
		applyPhaseSummary(&result, phase)
		if phase.Err != nil {
			return result
		}
	}

	return result
}

func disablePerturbations(cfg tracecheck.ExploreConfig) tracecheck.ExploreConfig {
	// Naive baseline utility used by v1 auto closed-loop:
	// treat reference runs as "control" by clearing all perturbation knobs.
	out := cfg.Clone()
	if out.Perturbations.PermuteOrder == nil {
		out.Perturbations.PermuteOrder = make(map[tracecheck.ReconcilerID]bool)
	}
	for id := range out.Perturbations.PermuteOrder {
		out.Perturbations.PermuteOrder[id] = false
	}
	out.Perturbations.Staleness = make(map[tracecheck.ReconcilerID]tracecheck.StalenessConfig)
	out.Perturbations.UserActionTargetDepth = make(map[int]int)
	return out
}

func buildDefaultScenarioRerunPlans(
	reference ScenarioPhaseResult,
	base tracecheck.ExploreConfig,
	scenarioCtx ScenarioContext,
) []ScenarioPhasePlan {
	// Naive v1 rerun strategy:
	// - exactly one rerun phase
	// - enable ordering permutation for all controllers observed in reference
	// This is intentionally generic and high-recall; it is not a precise
	// root-cause perturbation plan yet.
	rerunCfg := disablePerturbations(base)
	seenControllers := observedControllersInReference(reference)
	for _, controllerID := range seenControllers {
		if controllerID == "" || controllerID == tracecheck.UserControllerID {
			continue
		}
		rerunCfg.Perturbations.PermuteOrder[controllerID] = true
	}

	rerunContext := scenarioCtx
	return []ScenarioPhasePlan{
		{
			Name:    "rerun",
			Config:  rerunCfg,
			Context: &rerunContext,
		},
	}
}

func buildUserActionInterleavingPlans(
	reference ScenarioPhaseResult,
	base tracecheck.ExploreConfig,
	scenarioCtx ScenarioContext,
	userInputs []tracecheck.UserAction,
) []ScenarioPhasePlan {
	if len(userInputs) < 2 || reference.Result == nil || len(reference.Result.ConvergedStates) == 0 {
		return nil
	}
	firstState := reference.Result.ConvergedStates[0]
	if len(firstState.Paths) == 0 {
		return nil
	}
	path := firstState.Paths[0]
	if len(path) == 0 {
		return nil
	}

	actionDepth := make(map[int]int, len(userInputs))
	for depth, step := range path {
		if step == nil || step.ControllerID != tracecheck.UserControllerID || step.StepMetadata == nil {
			continue
		}
		rawIdx, ok := step.StepMetadata[tracecheck.UserActionIndexMetadataKey]
		if !ok {
			continue
		}
		actionIdx, err := strconv.Atoi(strings.TrimSpace(rawIdx))
		if err != nil {
			continue
		}
		if actionIdx < 0 || actionIdx >= len(userInputs) {
			continue
		}
		if _, alreadySet := actionDepth[actionIdx]; !alreadySet {
			actionDepth[actionIdx] = depth
		}
	}

	plans := make([]ScenarioPhasePlan, 0)
	for actionIdx := 1; actionIdx < len(userInputs); actionIdx++ {
		prevDepth, prevOk := actionDepth[actionIdx-1]
		currDepth, currOk := actionDepth[actionIdx]
		// If any required depth is missing in the reference path, skip interleavings.
		if !prevOk || !currOk {
			return nil
		}

		targetDepths := make([]int, 0)
		if currDepth > prevDepth+1 {
			for depth := prevDepth + 1; depth <= currDepth-1; depth++ {
				targetDepths = append(targetDepths, depth)
			}
		} else {
			// Empty strict-between window fallback.
			targetDepths = append(targetDepths, currDepth)
		}

		for _, targetDepth := range targetDepths {
			cfg := disablePerturbations(base)
			cfg.Perturbations.UserActionTargetDepth[actionIdx] = targetDepth

			attrs := copyAttributes(scenarioCtx.Attributes)
			attrs["perturbation.strategy"] = "user_action_interleaving"
			attrs["perturbation.action_index"] = strconv.Itoa(actionIdx)
			attrs["perturbation.target_depth"] = strconv.Itoa(targetDepth)
			attrs["perturbation.reference_prev_depth"] = strconv.Itoa(prevDepth)
			attrs["perturbation.reference_action_depth"] = strconv.Itoa(currDepth)
			ctxCopy := scenarioCtx
			ctxCopy.Attributes = attrs

			plans = append(plans, ScenarioPhasePlan{
				Name:    fmt.Sprintf("interleave_action_%d_depth_%d", actionIdx, targetDepth),
				Config:  cfg,
				Context: &ctxCopy,
			})
		}
	}
	return plans
}

func observedControllersInReference(reference ScenarioPhaseResult) []tracecheck.ReconcilerID {
	// Naive signal extraction for v1 planner:
	// derive candidate controllers from observed execution history only.
	if reference.Result == nil {
		return nil
	}
	seen := make(map[tracecheck.ReconcilerID]struct{})
	collect := func(paths []tracecheck.ResultState) {
		for _, state := range paths {
			for _, path := range state.Paths {
				for _, step := range path {
					if step == nil {
						continue
					}
					seen[step.ControllerID] = struct{}{}
				}
			}
		}
	}
	collect(reference.Result.ConvergedStates)
	collect(reference.Result.AbortedStates)

	out := make([]tracecheck.ReconcilerID, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i] < out[j]
	})
	return out
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
	invocationID string,
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

	start := time.Now()
	res := explorer.Explore(runCtx, startState)
	duration := time.Since(start)
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
			if strings.TrimSpace(invocationID) != "" {
				attrs[invocationIDAttributeKey] = invocationID
			}
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
			metrics := campaignMetricsForDump(phase.Stats, duration)
			if err := interactive.SaveInspectorDumpWithContextAndStatsAndCampaignMetrics(states, phase.VersionManager, path, dumpContext, dumpStats, metrics); err != nil {
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
	invocationID string,
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
	if strings.TrimSpace(invocationID) != "" {
		attrs[invocationIDAttributeKey] = invocationID
	}
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
	cmd.Env = os.Environ()
	if strings.TrimSpace(req.InvocationID) != "" {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", invocationIDEnvVar, strings.TrimSpace(req.InvocationID)))
	}

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
