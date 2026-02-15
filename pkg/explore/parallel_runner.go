package explore

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/tgoodwin/kamera/pkg/interactive"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
)

type ParallelOptions struct {
	MaxParallel int
	DumpDir     string
	StatsDir    string
}

type ParallelRunner struct {
	builder *tracecheck.ExplorerBuilder
}

// NewParallelRunner constructs a runner that can execute scenarios concurrently.
func NewParallelRunner(builder *tracecheck.ExplorerBuilder) (*ParallelRunner, error) {
	if builder == nil {
		return nil, fmt.Errorf("builder is nil")
	}
	return &ParallelRunner{builder: builder}, nil
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

	if opts.DumpDir != "" {
		if err := os.MkdirAll(opts.DumpDir, 0o755); err != nil {
			return nil, fmt.Errorf("create dump dir: %w", err)
		}
	}
	if opts.StatsDir != "" {
		if err := os.MkdirAll(opts.StatsDir, 0o755); err != nil {
			return nil, fmt.Errorf("create stats dir: %w", err)
		}
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
			if err := interactive.SaveInspectorDump(states, result.VersionManager, path); err != nil {
				result.Err = fmt.Errorf("dump scenario %s: %w", scenario.Name, err)
			} else {
				result.DumpPath = path
			}
		}
	}

	return result
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
