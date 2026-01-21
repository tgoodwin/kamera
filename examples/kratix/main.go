package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/tgoodwin/kamera/pkg/explore"
	"github.com/tgoodwin/kamera/pkg/interactive"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
)

type flowBuilder func() (*tracecheck.ExplorerBuilder, tracecheck.StateNode, error)

var flows = map[string]flowBuilder{
	"works":    buildWorksFlow,
	"promises": buildPromisesFlow,
}

func main() {
	flowName := flag.String("flow", "works", "flow to run (works|promises)")
	flag.Parse()

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

	if explore.DumpPath() != "" {
		explorer, err := builder.Build("standalone")
		if err != nil {
			fmt.Fprintf(os.Stderr, "build explorer: %v\n", err)
			os.Exit(1)
		}
		result := explorer.Explore(context.Background(), initialState)
		states := append([]tracecheck.ResultState{}, result.ConvergedStates...)
		states = append(states, result.AbortedStates...)
		if err := interactive.SaveInspectorDump(states, explorer.VersionManager(), explore.DumpPath()); err != nil {
			fmt.Fprintf(os.Stderr, "dump results: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("wrote results to %s\n", explore.DumpPath())
		return
	}

	runner, err := explore.NewRunner(builder)
	if err != nil {
		fmt.Fprintf(os.Stderr, "runner setup error: %v\n", err)
		os.Exit(1)
	}

	if err := runner.Run(context.Background(), initialState); err != nil {
		fmt.Fprintf(os.Stderr, "session error: %v\n", err)
		os.Exit(1)
	}
}
