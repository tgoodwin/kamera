package main

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
)

func TestTwoStepWorkflowReferenceRunConverges(t *testing.T) {
	const (
		trials = 3
		// the # TODO fix: steps is still nondeterministic
		minSteps = 175
		maxSteps = 182
	)

	for trial := 0; trial < trials; trial++ {
		scenario := loadTwoStepWorkflowScenario(t)
		result := runReferenceScenario(t, scenario)

		if len(result.ConvergedStates) != 1 {
			t.Fatalf("trial %d: expected exactly 1 converged state, got %d", trial, len(result.ConvergedStates))
		}
		if len(result.AbortedStates) != 0 {
			t.Fatalf("trial %d: expected no aborted states, got %d", trial, len(result.AbortedStates))
		}

		path := result.ConvergedStates[0].Paths[0]
		if got := len(path); got < minSteps || got > maxSteps {
			t.Fatalf("trial %d: expected reference steps in [%d, %d], got %d", trial, minSteps, maxSteps, got)
		}

		userSteps := make([]int, 0, 2)
		for idx, step := range path {
			if step != nil && step.ControllerID == tracecheck.UserControllerID {
				userSteps = append(userSteps, idx)
			}
		}
		if !reflect.DeepEqual(userSteps, []int{0, 73}) {
			t.Fatalf("trial %d: expected user action steps [0 73], got %v", trial, userSteps)
		}
	}
}

func loadTwoStepWorkflowScenario(t *testing.T) exploreScenario {
	t.Helper()

	builder := newKnativeExplorerBuilder()
	inputs, err := coverage.LoadInputs(filepath.Join(".", "two-step-workflow.json"))
	if err != nil {
		t.Fatalf("load inputs: %v", err)
	}
	scenarios, err := scenariosFromInputs(builder, inputs)
	if err != nil {
		t.Fatalf("build scenarios: %v", err)
	}
	if len(scenarios) != 1 {
		t.Fatalf("expected exactly 1 scenario, got %d", len(scenarios))
	}

	return exploreScenario{
		builder:  builder,
		state:    scenarios[0].EnvironmentState,
		actions:  append([]tracecheck.UserAction(nil), scenarios[0].UserInputs...),
		config:   disableExamplePerturbations(scenarios[0].Config),
		scenario: scenarios[0].Name,
	}
}

type exploreScenario struct {
	builder  *tracecheck.ExplorerBuilder
	state    tracecheck.StateNode
	actions  []tracecheck.UserAction
	config   tracecheck.ExploreConfig
	scenario string
}

func runReferenceScenario(t *testing.T, scenario exploreScenario) *tracecheck.Result {
	t.Helper()

	fork := scenario.builder.Fork()
	fork.WithUserActions(append([]tracecheck.UserAction(nil), scenario.actions...))
	fork.SetConfig(scenario.config)

	seed, err := scenario.builder.BuildRestartSeed(scenario.state)
	if err != nil {
		t.Fatalf("build restart seed: %v", err)
	}
	startState, err := tracecheck.SeedToStateNode(seed, fork)
	if err != nil {
		t.Fatalf("seed to state: %v", err)
	}

	explorer, err := fork.Build("standalone")
	if err != nil {
		t.Fatalf("build explorer: %v", err)
	}

	result := explorer.Explore(context.Background(), startState)
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	return result
}

func disableExamplePerturbations(cfg tracecheck.ExploreConfig) tracecheck.ExploreConfig {
	out := cfg.Clone()
	out.MaxDepth = 400
	if out.Perturbations.PermuteOrder == nil {
		out.Perturbations.PermuteOrder = make(map[tracecheck.ReconcilerID]bool)
	}
	for id := range out.Perturbations.PermuteOrder {
		out.Perturbations.PermuteOrder[id] = false
	}
	out.Perturbations.Staleness = make(map[tracecheck.ReconcilerID]tracecheck.StalenessConfig)
	out.Perturbations.UserActionReadyDepths = make(map[int]int)
	return out
}
