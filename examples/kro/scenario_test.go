package main

import (
	"testing"

	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestScenariosFromInputsRequiresBuilder(t *testing.T) {
	_, err := scenariosFromInputs(nil, []coverage.Input{{Name: "x"}})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestScenariosFromInputsRequiresInputs(t *testing.T) {
	builder := newKROExplorerBuilder()
	_, err := scenariosFromInputs(builder, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestScenariosFromInputsTranslatesQuickstartApplication(t *testing.T) {
	builder := newKROExplorerBuilder()

	input := coverage.Input{
		Name: "kro-quickstart/application-create",
		EnvironmentState: coverage.EnvironmentState{
			Objects: []*unstructured.Unstructured{
				buildQuickstartApplicationRGD(),
			},
		},
		ExternalInputs: []coverage.ExternalInput{
			{
				ID:     "create-application-instance",
				OpType: event.CREATE,
				Object: buildQuickstartApplicationInstance(),
			},
		},
		Tuning: coverage.InputTuning{
			MaxDepth:           9,
			PermuteControllers: []string{"ApplicationController"},
			StaleReads: map[string][]string{
				"ApplicationController": {"apps/Deployment"},
			},
			StaleLookback: map[string]int{
				"apps/Deployment": 2,
			},
		},
	}

	scenarios, err := scenariosFromInputs(builder, []coverage.Input{input})
	if err != nil {
		t.Fatalf("scenariosFromInputs error = %v", err)
	}
	if len(scenarios) != 1 {
		t.Fatalf("expected 1 scenario, got %d", len(scenarios))
	}

	sc := scenarios[0]
	if sc.Name != input.Name {
		t.Fatalf("expected scenario name %q, got %q", input.Name, sc.Name)
	}
	if len(sc.EnvironmentState.Objects()) == 0 {
		t.Fatalf("expected seeded state objects, got empty state")
	}
	if len(sc.ExternalInputs) != 1 {
		t.Fatalf("expected 1 user action, got %d", len(sc.ExternalInputs))
	}
	if sc.ExternalInputs[0].ID != "create-application-instance" {
		t.Fatalf("expected user action id create-application-instance, got %q", sc.ExternalInputs[0].ID)
	}
	if sc.ExternalInputs[0].OpType != event.CREATE {
		t.Fatalf("expected CREATE user action, got %q", sc.ExternalInputs[0].OpType)
	}

	payload, ok := sc.ExternalInputs[0].Payload.(*unstructured.Unstructured)
	if !ok {
		t.Fatalf("expected unstructured payload, got %T", sc.ExternalInputs[0].Payload)
	}
	if payload.GetKind() != "Application" || payload.GetName() != "my-app-instance" {
		t.Fatalf("unexpected payload identity: kind=%q name=%q", payload.GetKind(), payload.GetName())
	}
	if sc.Config.MaxDepth != 9 {
		t.Fatalf("expected MaxDepth=9, got %d", sc.Config.MaxDepth)
	}
	if !sc.Config.Perturbations.PermuteOrder[tracecheck.ReconcilerID("ApplicationController")] {
		t.Fatalf("expected ApplicationController permutation to be enabled")
	}
	staleness, ok := sc.Config.Perturbations.Staleness[tracecheck.ReconcilerID("ApplicationController")]
	if !ok {
		t.Fatal("expected staleness tuning for ApplicationController")
	}
	if staleness.StaleReadBounds["apps/Deployment"] != 2 {
		t.Fatalf("expected lookback=2, got %d", staleness.StaleReadBounds["apps/Deployment"])
	}
}
