package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/explore"
)

var fuzzCasesFlag = flag.Int("fuzz-cases", 12, "number of sampled parameterized scenarios to generate per input")
var fuzzSeedFlag = flag.Int64("fuzz-seed", 1337, "seed for deterministic sampled parameterized scenario generation")

func main() {
	setDefaultKarpenterInputsFlag()
	flag.Parse()

	ctx := context.Background()
	builder := newKarpenterExplorerBuilder()
	if cfgPath := explore.ConfigPath(); cfgPath != "" {
		loadedCfg, err := explore.LoadExploreConfigFromFile(cfgPath, builder.Config())
		if err != nil {
			fmt.Fprintf(os.Stderr, "load explore config: %v\n", err)
			os.Exit(1)
		}
		builder.SetConfig(loadedCfg)
	}

	inputsPath := strings.TrimSpace(explore.InputsPath())
	inputs, err := loadInputsForRun(inputsPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load inputs: %v\n", err)
		os.Exit(1)
	}

	if !explore.InteractiveEnabled() {
		if len(inputs) == 0 {
			if inputsPath != "" {
				fmt.Fprintf(os.Stderr, "batch mode requires valid --inputs input file, got %q\n", inputsPath)
			} else {
				fmt.Fprintf(os.Stderr, "batch mode requires --inputs\n")
			}
			os.Exit(1)
		}

		scenarios, err := scenariosFromInputs(builder, inputs)
		if err != nil {
			fmt.Fprintf(os.Stderr, "convert inputs: %v\n", err)
			os.Exit(1)
		}
		runner, err := explore.NewParallelRunner(builder)
		if err != nil {
			fmt.Fprintf(os.Stderr, "runner setup error: %v\n", err)
			os.Exit(1)
		}
		opts := explore.ParallelOptions{DumpDir: explore.DumpPath(), StatsDir: explore.DumpStatsPath()}
		if _, err := runner.RunAll(ctx, scenarios, opts); err != nil {
			fmt.Fprintf(os.Stderr, "batch run error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if len(inputs) == 0 {
		fmt.Fprintf(os.Stderr, "interactive mode requires one input file: either --inputs or default %q\n", defaultKarpenterInputsSearchPaths[0])
		os.Exit(1)
	}
	if len(inputs) != 1 {
		fmt.Fprintf(os.Stderr, "expected a single input for interactive mode, got %d\n", len(inputs))
		os.Exit(1)
	}

	builder.SetConfig(applyInputTuning(builder.Config(), inputs[0].Tuning))

	initialState, seededObjects, err := buildStateFromCoverageInput(builder, inputs[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "build initial state from input: %v\n", err)
		os.Exit(1)
	}
	userActions, err := buildUserActionsFromCoverageInput(inputs[0], seededObjects)
	if err != nil {
		fmt.Fprintf(os.Stderr, "build user actions from input: %v\n", err)
		os.Exit(1)
	}

	runner, err := explore.NewRunner(builder)
	if err != nil {
		fmt.Fprintf(os.Stderr, "runner setup error: %v\n", err)
		os.Exit(1)
	}

	if err := runner.Run(ctx, explore.RunInput{EnvironmentState: initialState, UserActions: userActions}); err != nil {
		fmt.Fprintf(os.Stderr, "session error: %v\n", err)
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

func loadInputsForRun(inputsPath string) ([]coverage.Input, error) {
	if inputsPath == "" {
		return nil, nil
	}

	inputs, err := coverage.LoadInputs(inputsPath)
	if err != nil {
		return nil, fmt.Errorf("load inputs from %s: %w", inputsPath, err)
	}
	return inputs, nil
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
