package explore

import (
	"fmt"
	"strings"

	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// InputStateBuilder converts one declarative input into the initial simulator
// state and returns the objects seeded into that state.
type InputStateBuilder func(
	builder *tracecheck.ExplorerBuilder,
	input coverage.Input,
) (tracecheck.StateNode, []client.Object, error)

// InputExpander optionally turns one declarative input into multiple variants.
type InputExpander func(input coverage.Input) ([]coverage.Input, error)

// ScenarioCompileOptions supplies the project-specific seams needed to compile
// declarative inputs into executable scenarios.
type ScenarioCompileOptions struct {
	BuildState   InputStateBuilder
	ExpandInput  InputExpander
	BuildActions func(coverage.Input, []client.Object) ([]tracecheck.UserAction, error)
	ApplyTuning  func(tracecheck.ExploreConfig, coverage.InputTuning) (tracecheck.ExploreConfig, error)
}

// CompileInputScenarios performs the common input-to-scenario translation used
// by example harnesses while leaving state seeding and input expansion to each
// target control plane.
func CompileInputScenarios(
	builder *tracecheck.ExplorerBuilder,
	inputs []coverage.Input,
	options ScenarioCompileOptions,
) ([]Scenario, error) {
	if builder == nil {
		return nil, fmt.Errorf("builder is nil")
	}
	if len(inputs) == 0 {
		return nil, fmt.Errorf("no inputs supplied")
	}
	if options.BuildState == nil {
		return nil, fmt.Errorf("state builder is nil")
	}

	buildActions := options.BuildActions
	if buildActions == nil {
		buildActions = UserActionsFromInput
	}
	applyTuning := options.ApplyTuning
	if applyTuning == nil {
		applyTuning = ApplyInputTuning
	}

	baseConfig := builder.Config()
	scenarios := make([]Scenario, 0, len(inputs))
	for inputIndex, input := range inputs {
		variants := []coverage.Input{input}
		if options.ExpandInput != nil {
			expanded, err := options.ExpandInput(input)
			if err != nil {
				return nil, fmt.Errorf("expand input %d (%s): %w", inputIndex, input.Name, err)
			}
			variants = expanded
		}

		for _, variant := range variants {
			state, seededObjects, err := options.BuildState(builder, variant)
			if err != nil {
				return nil, fmt.Errorf("build start state for %s: %w", variant.Name, err)
			}
			actions, err := buildActions(variant, seededObjects)
			if err != nil {
				return nil, fmt.Errorf("build user actions for %s: %w", variant.Name, err)
			}
			config, err := applyTuning(baseConfig, variant.Tuning)
			if err != nil {
				return nil, fmt.Errorf("apply tuning for %s: %w", variant.Name, err)
			}

			scenarios = append(scenarios, Scenario{
				Name:             variant.Name,
				EnvironmentState: state,
				ExternalInputs:   actions,
				Config:           config,
			})
		}
	}

	if len(scenarios) == 0 {
		return nil, fmt.Errorf("no scenarios produced")
	}
	return scenarios, nil
}

// UserActionsFromInput converts declarative external inputs into simulator
// actions. A CREATE for an object already present in the initial state becomes
// an UPDATE, matching the behavior previously duplicated by each harness.
func UserActionsFromInput(
	input coverage.Input,
	seededObjects []client.Object,
) ([]tracecheck.UserAction, error) {
	actions := make([]tracecheck.UserAction, 0, len(input.ExternalInputs))
	for index, externalInput := range input.ExternalInputs {
		if externalInput.Object == nil {
			return nil, fmt.Errorf("input user input %d has nil object", index)
		}

		id := strings.TrimSpace(externalInput.ID)
		if id == "" {
			id = fmt.Sprintf("user-input-%d", index)
		}
		opType := externalInput.OpType
		if opType == event.CREATE && InputObjectSeeded(externalInput.Object, seededObjects) {
			opType = event.UPDATE
		}
		actions = append(actions, tracecheck.UserAction{
			ID:      id,
			OpType:  opType,
			Payload: externalInput.Object.DeepCopy(),
		})
	}
	return actions, nil
}

// InputObjectSeeded reports whether an object's resource identity is already
// present in the initial environment state.
func InputObjectSeeded(object client.Object, seededObjects []client.Object) bool {
	if object == nil {
		return false
	}
	for _, seeded := range seededObjects {
		if SameObjectIdentity(seeded, object) {
			return true
		}
	}
	return false
}

// SameObjectIdentity compares the Kubernetes group, kind, namespace, and name
// that identify two objects.
func SameObjectIdentity(a, b client.Object) bool {
	if a == nil || b == nil {
		return false
	}
	aGVK := a.GetObjectKind().GroupVersionKind()
	bGVK := b.GetObjectKind().GroupVersionKind()
	return aGVK.Group == bGVK.Group &&
		aGVK.Kind == bGVK.Kind &&
		a.GetNamespace() == b.GetNamespace() &&
		a.GetName() == b.GetName()
}
