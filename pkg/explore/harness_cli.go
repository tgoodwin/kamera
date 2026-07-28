package explore

import (
	"context"
	"fmt"
	"os"

	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
)

// HarnessCLIOptions describes the project-specific seams needed by the common
// example-harness command-line lifecycle.
type HarnessCLIOptions struct {
	NewBuilder func([]coverage.Input) (*tracecheck.ExplorerBuilder, error)
	Compile    func(*tracecheck.ExplorerBuilder, []coverage.Input) ([]Scenario, error)

	InteractiveInput func(*tracecheck.ExplorerBuilder, []coverage.Input) (RunInput, error)

	InputsAlwaysBatch             bool
	RequireInputs                 bool
	RequireSingleInteractiveInput bool
	DirectSingleScenario          bool
	ParallelOptions               func() ParallelOptions
}

// RunHarnessCLI loads declarative inputs and exploration config, constructs the
// target harness, and dispatches either batch scenarios or an interactive run.
func RunHarnessCLI(ctx context.Context, options HarnessCLIOptions) error {
	if options.NewBuilder == nil {
		return fmt.Errorf("builder factory is nil")
	}

	inputsPath := InputsPath()
	var inputs []coverage.Input
	if inputsPath != "" {
		loaded, err := coverage.LoadInputs(inputsPath)
		if err != nil {
			return fmt.Errorf("load inputs: %w", err)
		}
		inputs = loaded
	}
	if options.RequireInputs && len(inputs) == 0 {
		return fmt.Errorf("harness requires --inputs")
	}

	builder, err := options.NewBuilder(inputs)
	if err != nil {
		return fmt.Errorf("build harness: %w", err)
	}
	if builder == nil {
		return fmt.Errorf("builder factory returned nil")
	}
	if configPath := ConfigPath(); configPath != "" {
		config, err := LoadExploreConfigFromFile(configPath, builder.Config())
		if err != nil {
			return fmt.Errorf("load explore config: %w", err)
		}
		builder.SetConfig(config)
	}

	batchMode := inputsPath != "" && (options.InputsAlwaysBatch || !InteractiveEnabled())
	if batchMode {
		if options.InputsAlwaysBatch && InteractiveEnabled() {
			fmt.Fprintln(os.Stderr, "interactive ignored in batch mode")
		}
		return runHarnessBatch(ctx, builder, inputs, options)
	}

	if options.RequireSingleInteractiveInput && len(inputs) != 1 {
		return fmt.Errorf("interactive mode requires exactly one input, got %d", len(inputs))
	}
	if options.InteractiveInput == nil {
		return fmt.Errorf("interactive input builder is nil")
	}
	input, err := options.InteractiveInput(builder, inputs)
	if err != nil {
		return fmt.Errorf("build interactive input: %w", err)
	}
	runner, err := NewRunner(builder)
	if err != nil {
		return fmt.Errorf("runner setup error: %w", err)
	}
	if err := runner.Run(ctx, input); err != nil {
		return fmt.Errorf("session error: %w", err)
	}
	return nil
}

func runHarnessBatch(
	ctx context.Context,
	builder *tracecheck.ExplorerBuilder,
	inputs []coverage.Input,
	options HarnessCLIOptions,
) error {
	if options.Compile == nil {
		return fmt.Errorf("scenario compiler is nil")
	}
	scenarios, err := options.Compile(builder, inputs)
	if err != nil {
		return fmt.Errorf("convert inputs: %w", err)
	}

	if options.DirectSingleScenario && len(scenarios) == 1 {
		scenario := scenarios[0]
		builder.SetConfig(scenario.Config)
		builder.WithUserActions(scenario.ExternalInputs)
		runner, err := NewRunner(builder)
		if err != nil {
			return fmt.Errorf("runner setup error: %w", err)
		}
		if err := runner.Run(ctx, RunInput{
			EnvironmentState: scenario.EnvironmentState,
			UserActions:      scenario.ExternalInputs,
		}); err != nil {
			return fmt.Errorf("batch run error: %w", err)
		}
		return nil
	}

	runner, err := NewParallelRunner(builder)
	if err != nil {
		return fmt.Errorf("runner setup error: %w", err)
	}
	parallelOptions := ParallelOptions{DumpDir: DumpPath()}
	if options.ParallelOptions != nil {
		parallelOptions = options.ParallelOptions()
	}
	if _, err := runner.RunAll(ctx, scenarios, parallelOptions); err != nil {
		return fmt.Errorf("batch run error: %w", err)
	}
	return nil
}
