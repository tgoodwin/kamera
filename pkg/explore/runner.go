package explore

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/tgoodwin/kamera/pkg/interactive"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	"golang.org/x/exp/slices"
)

func dumpStatsIfRequested(stats *tracecheck.ExploreStats, runIdx int) error {
	if DumpStatsPath() == "" || stats == nil {
		return nil
	}
	stats.Finish()

	data, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal explore stats: %w", err)
	}

	target := withRunSuffix(DumpStatsPath(), runIdx)
	if err := os.WriteFile(target, data, 0o644); err != nil {
		return fmt.Errorf("write stats to %s: %w", target, err)
	}
	fmt.Printf("wrote stats to %s\n", target)
	return nil
}

func withRunSuffix(base string, runIdx int) string {
	if runIdx == 0 {
		return base
	}
	ext := filepath.Ext(base)
	prefix := strings.TrimSuffix(base, ext)
	return fmt.Sprintf("%s.run%d%s", prefix, runIdx, ext)
}

// Runner coordinates exploration runs and the inspector UI, including restart requests.
// Construct via NewRunner with a fully configured ExplorerBuilder
type Runner struct {
	builder *tracecheck.ExplorerBuilder
}

// NewRunner constructs a Runner from a configured ExplorerBuilder.
func NewRunner(builder *tracecheck.ExplorerBuilder) (*Runner, error) {
	if builder == nil {
		return nil, fmt.Errorf("builder is nil")
	}
	return &Runner{
		builder: builder,
	}, nil
}

// Run executes the initial exploration and, if interactive is enabled, loops handling restart requests.
func (r *Runner) Run(ctx context.Context, initialState tracecheck.StateNode) error {
	if r == nil || r.builder == nil {
		return fmt.Errorf("explore runner: builder is required")
	}

	currentConfig := r.builder.Config()
	baseline := initialState.Clone()

	mergeStates := func(existing, additions []tracecheck.ResultState) []tracecheck.ResultState {
		out := make([]tracecheck.ResultState, 0, len(existing)+len(additions))
		index := make(map[tracecheck.NodeHash]int)
		for _, st := range existing {
			key := st.State.Hash()
			index[key] = len(out)
			out = append(out, st)
		}
		for _, st := range additions {
			key := st.State.Hash()
			if idx, ok := index[key]; ok {
				mergedPaths := append(out[idx].Paths, st.Paths...)
				out[idx].Paths = tracecheck.GetUniquePaths(mergedPaths)
			} else {
				index[key] = len(out)
				out = append(out, st)
			}
		}
		return out
	}

	runOnce := func(ctx context.Context, state tracecheck.StateNode) (*tracecheck.Result, tracecheck.VersionManager, *tracecheck.ExploreStats, error) {
		r.builder.SetConfig(currentConfig)
		// get a fresh explorer for each run
		explorer, err := r.builder.Build("standalone")
		if err != nil {
			return nil, nil, nil, fmt.Errorf("build explorer: %w", err)
		}

		runCtx := ctx
		if explorer.Config.Timeout > 0 {
			var cancel context.CancelFunc
			runCtx, cancel = context.WithTimeout(ctx, explorer.Config.Timeout)
			defer cancel()
		}

		return explorer.Explore(runCtx, state), explorer.VersionManager(), explorer.Stats(), nil
	}

	resolver := tracecheck.VersionManager(nil)
	res, resolver, stats, err := runOnce(ctx, baseline.Clone())
	if err != nil {
		return err
	}
	runIdx := 0

	if err := dumpStatsIfRequested(stats, 0); err != nil {
		return err
	}

	states := append([]tracecheck.ResultState{}, res.ConvergedStates...)
	states = append(states, res.AbortedStates...)
	if len(states) == 0 {
		fmt.Println("no states returned from exploration")
		return nil
	}

	if DumpPath() != "" {
		if err := interactive.SaveInspectorDump(states, resolver, DumpPath()); err != nil {
			return fmt.Errorf("failed to dump results to %s: %w", DumpPath(), err)
		}
		fmt.Printf("wrote results to %s\n", DumpPath())
	}

	if !InteractiveEnabled() {
		fmt.Printf("interactive inspector disabled; states available: %d (converged=%d, aborted=%d)\n",
			len(states), len(res.ConvergedStates), len(res.AbortedStates))
		return nil
	}

	// enter a loop to handle optional search restarts via the interactive inspector
	for {
		// seed is an intermediate state node that can be used to restart the exploration
		// from that point. If the user decides not to restart, seed will be nil.
		restart, err := interactive.RunStateInspectorTUIView(states, resolver, true, currentConfig)
		if err != nil {
			return fmt.Errorf("inspector error: %w", err)
		}
		if restart == nil {
			break
		}

		currentConfig = restart.Config

		nextState, err := tracecheck.SeedToStateNode(restart.Seed, r.builder)
		if err != nil {
			return fmt.Errorf("seed to state: %w", err)
		}
		if restart.Seed.Depth > 0 {
			nextState = nextState.WithDepth(restart.Seed.Depth)
		}
		// apply the prefix to the next state so that new paths include the prefix leading to the selected subtree
		if len(restart.Prefix) > 0 {
			nextState.ExecutionHistory = slices.Clone(restart.Prefix)
		}

		nextRes, nextResolver, nextStats, err := runOnce(ctx, nextState)
		if err != nil {
			return fmt.Errorf("restart explore error: %w", err)
		}
		runIdx++
		if nextResolver != nil {
			resolver = nextResolver
		}
		if err := dumpStatsIfRequested(nextStats, runIdx); err != nil {
			return err
		}
		newStates := append([]tracecheck.ResultState{}, nextRes.ConvergedStates...)
		newStates = append(newStates, nextRes.AbortedStates...)
		if restart.PreserveHistory {
			states = mergeStates(states, newStates)
		} else {
			states = newStates
		}
		if DumpPath() != "" {
			if err := interactive.SaveInspectorDump(states, resolver, DumpPath()); err != nil {
				return fmt.Errorf("failed to dump results to %s: %w", DumpPath(), err)
			}
			fmt.Printf("wrote results to %s\n", DumpPath())
		}
	}

	// Explicitly flush output to avoid interleaving when invoked from other tools.
	_ = os.Stdout.Sync()
	_ = os.Stderr.Sync()
	return nil
}
