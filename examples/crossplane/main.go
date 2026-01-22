package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/tgoodwin/kamera/pkg/explore"
)

func main() {
	flag.Parse()

	builder := newCrossplaneExplorerBuilder()
	if cfgPath := explore.ConfigPath(); cfgPath != "" {
		loadedCfg, err := explore.LoadExploreConfigFromFile(cfgPath, builder.Config())
		if err != nil {
			fmt.Fprintf(os.Stderr, "load explore config: %v\n", err)
			os.Exit(1)
		}
		builder.SetConfig(loadedCfg)
	}

	initialState := buildInitialCrossplaneState(builder)
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
