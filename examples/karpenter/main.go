package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/explore"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
)

var fuzzCasesFlag = flag.Int("fuzz-cases", 12, "number of sampled parameterized scenarios to generate per input")
var fuzzSeedFlag = flag.Int64("fuzz-seed", 1337, "seed for deterministic sampled parameterized scenario generation")

func main() {
	setDefaultKarpenterInputsFlag()
	flag.Parse()

	err := explore.RunHarnessCLI(context.Background(), explore.HarnessCLIOptions{
		NewBuilder: func([]coverage.Input) (*tracecheck.ExplorerBuilder, error) {
			return newKarpenterExplorerBuilder(), nil
		},
		Compile:                       scenariosFromInputs,
		RequireInputs:                 true,
		RequireSingleInteractiveInput: true,
		ParallelOptions: func() explore.ParallelOptions {
			return batchParallelOptions(explore.ParallelProcessesEnabled(), explore.DumpPath())
		},
		InteractiveInput: func(builder *tracecheck.ExplorerBuilder, inputs []coverage.Input) (explore.RunInput, error) {
			config, err := applyInputTuning(builder.Config(), inputs[0].Tuning)
			if err != nil {
				return explore.RunInput{}, fmt.Errorf("apply input tuning: %w", err)
			}
			builder.SetConfig(config)
			state, seededObjects, err := buildStateFromCoverageInput(builder, inputs[0])
			if err != nil {
				return explore.RunInput{}, fmt.Errorf("build initial state: %w", err)
			}
			actions, err := buildUserActionsFromCoverageInput(inputs[0], seededObjects)
			if err != nil {
				return explore.RunInput{}, fmt.Errorf("build user actions: %w", err)
			}
			return explore.RunInput{EnvironmentState: state, UserActions: actions}, nil
		},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func setDefaultKarpenterInputsFlag() {
	inputsPath, err := defaultKarpenterInputsPath()
	if err != nil {
		return
	}

	_ = flag.CommandLine.Set("inputs", inputsPath)
}

func batchParallelOptions(parallelProcessesEnabled bool, dumpDir string) explore.ParallelOptions {
	opts := explore.ParallelOptions{DumpDir: dumpDir}
	// Karpenter harness constructors currently capture singleton state
	// (cluster/provisioner/switcher), which is not scenario-safe under
	// in-process parallel execution.
	if !parallelProcessesEnabled {
		opts.MaxParallel = 1
	}
	return opts
}

func defaultKarpenterInputsPath() (string, error) {
	for _, path := range defaultKarpenterInputsSearchPaths {
		_, err := os.Stat(path)
		if err == nil {
			return path, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("check default inputs path %s: %w", path, err)
		}
	}
	return "", fmt.Errorf("default inputs file not found in search paths %v: %w", defaultKarpenterInputsSearchPaths, os.ErrNotExist)
}

var defaultKarpenterInputsSearchPaths = []string{
	"input-example.json",
	filepath.Join("examples", "karpenter", "input-example.json"),
}
