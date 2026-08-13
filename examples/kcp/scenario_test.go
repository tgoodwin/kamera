package main

import (
	"testing"

	"github.com/tgoodwin/kamera/pkg/coverage"
)

func TestScenariosFromInputsRequiresBuilder(t *testing.T) {
	_, err := scenariosFromInputs(nil, []coverage.Input{{Name: "x"}})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestScenariosFromInputsRequiresInputs(t *testing.T) {
	builder := newKCPExplorerBuilder()
	_, err := scenariosFromInputs(builder, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestDefaultInteractiveUserActionsAreEmpty(t *testing.T) {
	actions := defaultInteractiveUserActions()
	if len(actions) != 0 {
		t.Fatalf("expected no default user actions, got %d", len(actions))
	}
}
