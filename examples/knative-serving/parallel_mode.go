package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"

	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/explore"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
)

type parallelMode string

const (
	parallelModeGoroutine parallelMode = "goroutine"
	parallelModeProcess   parallelMode = "process"
)

func parseParallelMode(raw string) (parallelMode, error) {
	mode := parallelMode(strings.ToLower(strings.TrimSpace(raw)))
	if mode == "" {
		return parallelModeGoroutine, nil
	}
	switch mode {
	case parallelModeGoroutine, parallelModeProcess:
		return mode, nil
	default:
		return "", fmt.Errorf("invalid --parallel-mode %q (expected goroutine or process)", raw)
	}
}

func scenariosForChildIndex(scenarios []explore.Scenario, idx int) ([]explore.Scenario, error) {
	if idx < 0 {
		return scenarios, nil
	}
	if idx >= len(scenarios) {
		return nil, fmt.Errorf("scenario-index %d out of range", idx)
	}
	return []explore.Scenario{scenarios[idx]}, nil
}

func buildParallelChildArgs(baseArgs []string, scenarioIdx int) []string {
	args := append([]string{}, baseArgs...)
	args = append(args,
		"--parallel-child=true",
		fmt.Sprintf("--scenario-index=%d", scenarioIdx),
		"--parallel-mode=goroutine",
		"--interactive=false",
	)
	return args
}

func runParallelChild(ctx context.Context, builder *tracecheck.ExplorerBuilder, inputs []coverage.Input, batchMode bool) error {
	if !batchMode {
		return fmt.Errorf("--parallel-child requires batch mode")
	}
	if *scenarioIndexFlag < 0 {
		return fmt.Errorf("--parallel-child requires --scenario-index")
	}

	scenarios, err := scenariosFromInputs(builder, inputs)
	if err != nil {
		return fmt.Errorf("convert inputs: %w", err)
	}
	selected, err := scenariosForChildIndex(scenarios, *scenarioIndexFlag)
	if err != nil {
		return err
	}
	_, err = runBatchScenariosGoroutine(ctx, builder, selected, 1)
	return err
}

func runBatchScenarios(ctx context.Context, builder *tracecheck.ExplorerBuilder, scenarios []explore.Scenario) ([]explore.ScenarioResult, error) {
	mode, err := parseParallelMode(*parallelModeFlag)
	if err != nil {
		return nil, err
	}
	if *parallelWorkersFlag < 0 {
		return nil, fmt.Errorf("--parallel-workers must be >= 0")
	}

	if mode == parallelModeProcess && !*parallelChildFlag {
		return runBatchScenariosProcess(ctx, scenarios)
	}

	workers := *parallelWorkersFlag
	if *parallelChildFlag && workers == 0 {
		workers = 1
	}
	return runBatchScenariosGoroutine(ctx, builder, scenarios, workers)
}

func runBatchScenariosGoroutine(ctx context.Context, builder *tracecheck.ExplorerBuilder, scenarios []explore.Scenario, workers int) ([]explore.ScenarioResult, error) {
	runner, err := explore.NewParallelRunner(builder)
	if err != nil {
		return nil, fmt.Errorf("runner setup error: %w", err)
	}
	opts := explore.ParallelOptions{
		MaxParallel: workers,
		DumpDir:     explore.DumpPath(),
		StatsDir:    explore.DumpStatsPath(),
	}
	return runner.RunAll(ctx, scenarios, opts)
}

func runBatchScenariosProcess(ctx context.Context, scenarios []explore.Scenario) ([]explore.ScenarioResult, error) {
	results := make([]explore.ScenarioResult, len(scenarios))
	if len(scenarios) == 0 {
		return results, nil
	}

	exePath, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("resolve executable: %w", err)
	}

	workers := *parallelWorkersFlag
	if workers == 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	if workers > len(scenarios) {
		workers = len(scenarios)
	}
	if workers <= 0 {
		workers = 1
	}

	type runResult struct {
		idx int
		err error
	}

	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	baseArgs := os.Args[1:]
	jobs := make(chan int)
	resCh := make(chan runResult, len(scenarios))

	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for idx := range jobs {
			args := buildParallelChildArgs(baseArgs, idx)
			cmd := exec.CommandContext(childCtx, exePath, args...)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			err := cmd.Run()
			resCh <- runResult{idx: idx, err: err}
			if err != nil {
				cancel()
			}
		}
	}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go worker()
	}

	for idx := range scenarios {
		jobs <- idx
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(resCh)
	}()

	var firstErr error
	for item := range resCh {
		results[item.idx] = explore.ScenarioResult{Name: scenarios[item.idx].Name, Err: item.err}
		if item.err != nil && firstErr == nil {
			firstErr = fmt.Errorf("scenario %q failed: %w", scenarios[item.idx].Name, item.err)
		}
	}

	return results, firstErr
}
