package explore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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
	StatsDir    string
}

type childProcessRequest struct {
	ChildIndex int
	CWD        string
	Args       []string
}

type childProcessResult struct {
	ChildIndex int
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

	total := len(inputs)
	results := make([]ScenarioResult, total)

	maxParallel := opts.MaxParallel
	if maxParallel <= 0 {
		maxParallel = runtime.GOMAXPROCS(0)
	}
	if maxParallel > total {
		maxParallel = total
	}

	fmt.Printf("parallel-processes supervisor: starting %d child process(es) (max_parallel=%d)\n", total, maxParallel)

	jobs := make(chan int)
	resCh := make(chan childProcessResult, total)

	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for idx := range jobs {
			args := append([]string{}, childArgsBase...)
			args = append(args, fmt.Sprintf("--parallel-child-index=%d", idx))
			resCh <- r.spawnChildFn(ctx, childProcessRequest{
				ChildIndex: idx,
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
		for idx := range inputs {
			started++
			fmt.Printf("parallel-processes supervisor: start child input=%d (%d/%d)\n", idx, started, total)
			jobs <- idx
		}
		close(jobs)
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
		result := ScenarioResult{
			Name: strings.TrimSpace(inputs[item.ChildIndex].Name),
			Err:  item.Err,
		}
		if result.Name == "" {
			result.Name = fmt.Sprintf("input_%d", item.ChildIndex)
		}
		results[item.ChildIndex] = result

		if item.Err != nil {
			failed++
			failedIdx = append(failedIdx, item.ChildIndex)
			fmt.Printf("parallel-processes supervisor: child input=%d failed (%d/%d): %s\n",
				item.ChildIndex, completed, total, summarizeChildFailure(item))
			continue
		}

		fmt.Printf("parallel-processes supervisor: child input=%d completed (%d/%d)\n",
			item.ChildIndex, completed, total)
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
			childIdx,
			"select_scenario",
			err,
		)
		if dumpErr != nil {
			result.Err = fmt.Errorf("%w (and failed to write failure dump: %v)", result.Err, dumpErr)
		}
		result.DumpPath = dumpPath
		return []ScenarioResult{result}, result.Err
	}

	fmt.Printf("parallel-processes child: running input index %d as scenario %q\n", childIdx, selected.Name)
	result := r.runScenario(ctx, selected, opts, childIdx)
	if result.Err != nil {
		if result.DumpPath == "" {
			dumpPath, dumpErr := writeChildFailureDump(opts, selected.Name, selected.Context, childIdx, "run_scenario", result.Err)
			if dumpErr != nil {
				result.Err = fmt.Errorf("%w (and failed to write failure dump: %v)", result.Err, dumpErr)
			}
			result.DumpPath = dumpPath
		}
		return []ScenarioResult{result}, fmt.Errorf("parallel child index %d failed: %w", childIdx, result.Err)
	}
	return []ScenarioResult{result}, nil
}

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

	seed, err := r.builder.BuildRestartSeed(scenario.InitialState)
	if err != nil {
		result.Err = fmt.Errorf("build restart seed: %w", err)
		return result
	}

	fork := r.builder.Fork()
	if fork == nil {
		result.Err = fmt.Errorf("fork builder: nil")
		return result
	}
	fork.SetConfig(scenario.Config)

	startState, err := tracecheck.SeedToStateNode(seed, fork)
	if err != nil {
		result.Err = fmt.Errorf("seed to state: %w", err)
		return result
	}

	explorer, err := fork.Build("standalone")
	if err != nil {
		result.Err = fmt.Errorf("build explorer: %w", err)
		return result
	}

	runCtx := ctx
	if explorer.Config.Timeout > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, explorer.Config.Timeout)
		defer cancel()
	}

	res := explorer.Explore(runCtx, startState)
	result.Result = res
	result.VersionManager = explorer.VersionManager()
	result.Stats = explorer.Stats()

	if scenario.Invariant != nil && res != nil {
		for _, state := range res.ConvergedStates {
			if err := scenario.Invariant(state.State); err != nil {
				result.InvariantError = err
				break
			}
		}
	}

	if opts.StatsDir != "" {
		if err := writeScenarioStats(result.Stats, opts.StatsDir, scenario.Name, idx); err != nil {
			result.Err = err
		}
	}

	if opts.DumpDir != "" && res != nil {
		states := append([]tracecheck.ResultState{}, res.ConvergedStates...)
		states = append(states, res.AbortedStates...)
		if len(states) > 0 {
			path := scenarioDumpPath(opts.DumpDir, scenario.Name, idx)
			runIdx := idx
			dumpContext := &interactive.InspectorDumpContext{
				ScenarioName:     scenario.Name,
				ScenarioRunIndex: &runIdx,
				Workflow:         scenario.Context.Workflow,
				InputRef:         scenario.Context.InputRef,
				Attributes:       scenario.Context.Attributes,
			}
			if err := interactive.SaveInspectorDumpWithContext(states, result.VersionManager, path, dumpContext); err != nil {
				result.Err = fmt.Errorf("dump scenario %s: %w", scenario.Name, err)
			} else {
				result.DumpPath = path
			}
		}
	}

	return result
}

func ensureParallelOutputDirs(opts ParallelOptions) error {
	if opts.DumpDir != "" {
		if err := os.MkdirAll(opts.DumpDir, 0o755); err != nil {
			return fmt.Errorf("create dump dir: %w", err)
		}
	}
	if opts.StatsDir != "" {
		if err := os.MkdirAll(opts.StatsDir, 0o755); err != nil {
			return fmt.Errorf("create stats dir: %w", err)
		}
	}
	return nil
}

func childParallelOptions(opts ParallelOptions) ParallelOptions {
	opts.MaxParallel = 1
	return opts
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
		ChildIndex: req.ChildIndex,
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

func writeScenarioStats(stats *tracecheck.ExploreStats, dir string, name string, idx int) error {
	if stats == nil {
		return nil
	}
	stats.Finish()
	data, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal explore stats: %w", err)
	}
	path := scenarioStatsPath(dir, name, idx)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write stats to %s: %w", path, err)
	}
	return nil
}

func scenarioDumpPath(dir, name string, idx int) string {
	base := scenarioFileBase(name, idx)
	return filepath.Join(dir, base+".jsonl")
}

func scenarioStatsPath(dir, name string, idx int) string {
	base := scenarioFileBase(name, idx)
	return filepath.Join(dir, base+".json")
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
