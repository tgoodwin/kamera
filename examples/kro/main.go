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

func main() {
	flag.Parse()

	err := explore.RunHarnessCLI(context.Background(), explore.HarnessCLIOptions{
		NewBuilder: func([]coverage.Input) (*tracecheck.ExplorerBuilder, error) {
			return newKROExplorerBuilder(), nil
		},
		Compile:           scenariosFromInputs,
		InputsAlwaysBatch: true,
		InteractiveInput: func(builder *tracecheck.ExplorerBuilder, _ []coverage.Input) (explore.RunInput, error) {
			return explore.RunInput{
				EnvironmentState: buildInitialKROState(builder),
				UserActions:      defaultInteractiveUserActions(),
			}, nil
		},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
