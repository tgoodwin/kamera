package explore

import (
	"testing"

	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestCompileInputScenariosUsesProjectHooks(t *testing.T) {
	builder := tracecheck.NewExplorerBuilder(runtime.NewScheme())
	inputs := []coverage.Input{{Name: "base"}}
	state := tracecheck.StateNode{}

	scenarios, err := CompileInputScenarios(builder, inputs, ScenarioCompileOptions{
		ExpandInput: func(input coverage.Input) ([]coverage.Input, error) {
			input.Name += "/variant"
			return []coverage.Input{input}, nil
		},
		BuildState: func(
			_ *tracecheck.ExplorerBuilder,
			_ coverage.Input,
		) (tracecheck.StateNode, []client.Object, error) {
			return state, nil, nil
		},
	})
	if err != nil {
		t.Fatalf("compile scenarios: %v", err)
	}
	if len(scenarios) != 1 {
		t.Fatalf("expected one scenario, got %d", len(scenarios))
	}
	if scenarios[0].Name != "base/variant" {
		t.Fatalf("expected expanded name, got %q", scenarios[0].Name)
	}
}

func TestCompileInputScenariosValidatesRequiredInputs(t *testing.T) {
	builder := tracecheck.NewExplorerBuilder(runtime.NewScheme())
	buildState := func(
		_ *tracecheck.ExplorerBuilder,
		_ coverage.Input,
	) (tracecheck.StateNode, []client.Object, error) {
		return tracecheck.StateNode{}, nil, nil
	}

	if _, err := CompileInputScenarios(nil, []coverage.Input{{Name: "test"}}, ScenarioCompileOptions{BuildState: buildState}); err == nil {
		t.Fatal("expected nil builder error")
	}
	if _, err := CompileInputScenarios(builder, nil, ScenarioCompileOptions{BuildState: buildState}); err == nil {
		t.Fatal("expected empty inputs error")
	}
	if _, err := CompileInputScenarios(builder, []coverage.Input{{Name: "test"}}, ScenarioCompileOptions{}); err == nil {
		t.Fatal("expected nil state builder error")
	}
}

func TestUserActionsFromInputDefaultsIDsAndUpdatesSeededCreates(t *testing.T) {
	seeded := testInputObject("Widget", "default", "sample")
	created := seeded.DeepCopy()
	created.Object["spec"] = map[string]any{"value": "new"}

	actions, err := UserActionsFromInput(coverage.Input{
		ExternalInputs: []coverage.ExternalInput{{
			OpType: event.CREATE,
			Object: created,
		}},
	}, []client.Object{seeded})
	if err != nil {
		t.Fatalf("build user actions: %v", err)
	}
	if len(actions) != 1 {
		t.Fatalf("expected one action, got %d", len(actions))
	}
	if actions[0].ID != "user-input-0" {
		t.Fatalf("expected generated ID, got %q", actions[0].ID)
	}
	if actions[0].OpType != event.UPDATE {
		t.Fatalf("expected seeded CREATE to become UPDATE, got %q", actions[0].OpType)
	}
}

func TestSameObjectIdentityIncludesGroupAndKind(t *testing.T) {
	a := testInputObject("Widget", "default", "sample")
	b := a.DeepCopy()
	if !SameObjectIdentity(a, b) {
		t.Fatal("expected matching objects")
	}

	b.SetGroupVersionKind(schema.GroupVersionKind{Group: "other.example", Version: "v1", Kind: "Widget"})
	if SameObjectIdentity(a, b) {
		t.Fatal("expected groups to distinguish identities")
	}
}

func testInputObject(kind, namespace, name string) *unstructured.Unstructured {
	object := &unstructured.Unstructured{}
	object.SetGroupVersionKind(schema.GroupVersionKind{Group: "example.org", Version: "v1", Kind: kind})
	object.SetNamespace(namespace)
	object.SetName(name)
	return object
}
