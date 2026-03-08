package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/explore"
)

func main() {
	flag.Parse()

	ctx := context.Background()
	builder := newKROExplorerBuilder()
	if cfgPath := explore.ConfigPath(); cfgPath != "" {
		loadedCfg, err := explore.LoadExploreConfigFromFile(cfgPath, builder.Config())
		if err != nil {
			fmt.Fprintf(os.Stderr, "load explore config: %v\n", err)
			os.Exit(1)
		}
		builder.SetConfig(loadedCfg)
	}

	if inputsPath := explore.InputsPath(); inputsPath != "" {
		if explore.InteractiveEnabled() {
			fmt.Fprintln(os.Stderr, "interactive ignored in batch mode")
		}
		inputs, err := coverage.LoadInputs(inputsPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "load inputs: %v\n", err)
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
		opts := explore.ParallelOptions{DumpDir: explore.DumpPath()}
		if _, err := runner.RunAll(ctx, scenarios, opts); err != nil {
			fmt.Fprintf(os.Stderr, "batch run error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	initialState := buildInitialKROState(builder)
	runner, err := explore.NewRunner(builder)
	if err != nil {
		fmt.Fprintf(os.Stderr, "runner setup error: %v\n", err)
		os.Exit(1)
	}
	if err := runner.Run(ctx, explore.RunInput{
		EnvironmentState: initialState,
		UserActions:      defaultInteractiveUserActions(),
	}); err != nil {
		fmt.Fprintf(os.Stderr, "session error: %v\n", err)
		os.Exit(1)
	}
}
