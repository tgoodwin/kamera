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

	var interactiveState tracecheck.StateNode
	err := explore.RunHarnessCLI(context.Background(), explore.HarnessCLIOptions{
		NewBuilder: func(inputs []coverage.Input) (*tracecheck.ExplorerBuilder, error) {
			if len(inputs) > 0 {
				if *flowName != "works" {
					fmt.Fprintln(os.Stderr, "flow flag ignored in input mode; behavior is derived from inputs")
				}
				return buildInputDrivenBuilder(inputs)
			}
			builderFn, ok := flows[*flowName]
			if !ok {
				return nil, fmt.Errorf("unknown flow %q (valid: works, promises)", *flowName)
			}
			builder, state, err := builderFn()
			interactiveState = state
			return builder, err
		},
		Compile:           scenariosFromInputs,
		InputsAlwaysBatch: true,
		InteractiveInput: func(_ *tracecheck.ExplorerBuilder, _ []coverage.Input) (explore.RunInput, error) {
			return explore.RunInput{EnvironmentState: interactiveState}, nil
		},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
