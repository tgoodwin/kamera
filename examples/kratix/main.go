package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/explore"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
)

type flowBuilder func() (*tracecheck.ExplorerBuilder, tracecheck.StateNode, error)

var flows = map[string]flowBuilder{
	"works":    buildWorksFlow,
	"promises": buildPromisesFlow,
}

func main() {
	flowName := flag.String("flow", "works", "flow to run for non-input mode (works|promises)")
	flag.Parse()

	ctx := context.Background()
	if inputsPath := explore.InputsPath(); inputsPath != "" {
		if *flowName != "works" {
			fmt.Fprintln(os.Stderr, "flow flag ignored in input mode; behavior is derived from inputs")
		}
		inputs, err := coverage.LoadInputs(inputsPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "load inputs: %v\n", err)
			os.Exit(1)
		}
		builder, err := buildInputDrivenBuilder(inputs)
		if err != nil {
			fmt.Fprintf(os.Stderr, "build input-driven builder: %v\n", err)
			os.Exit(1)
		}
		if cfgPath := explore.ConfigPath(); cfgPath != "" {
			loadedCfg, err := explore.LoadExploreConfigFromFile(cfgPath, builder.Config())
			if err != nil {
				fmt.Fprintf(os.Stderr, "load explore config: %v\n", err)
				os.Exit(1)
			}
			builder.SetConfig(loadedCfg)
		}
		if explore.InteractiveEnabled() {
			fmt.Fprintln(os.Stderr, "interactive ignored in batch mode")
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
		opts := explore.ParallelOptions{DumpDir: explore.DumpPath()}
		if _, err := runner.RunAll(ctx, scenarios, opts); err != nil {
			fmt.Fprintf(os.Stderr, "batch run error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	builderFn, ok := flows[*flowName]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown flow %q (valid: works, promises)\n", *flowName)
		os.Exit(2)
	}
	builder, initialState, err := builderFn()
	if err != nil {
		fmt.Fprintf(os.Stderr, "build flow %s: %v\n", *flowName, err)
		os.Exit(1)
	}
	if cfgPath := explore.ConfigPath(); cfgPath != "" {
		loadedCfg, err := explore.LoadExploreConfigFromFile(cfgPath, builder.Config())
		if err != nil {
			fmt.Fprintf(os.Stderr, "load explore config: %v\n", err)
			os.Exit(1)
		}
		builder.SetConfig(loadedCfg)
	}

	runner, err := explore.NewRunner(builder)
	if err != nil {
		fmt.Fprintf(os.Stderr, "runner setup error: %v\n", err)
		os.Exit(1)
	}

	if err := runner.Run(ctx, explore.RunInput{EnvironmentState: initialState}); err != nil {
		fmt.Fprintf(os.Stderr, "session error: %v\n", err)
		os.Exit(1)
	}
}
