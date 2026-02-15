package main

import (
	"strings"
	"testing"
)

func TestScenariosFromInputsUnimplemented(t *testing.T) {
	scenarios, err := scenariosFromInputs(nil, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "input to scenario conversion not implemented") {
		t.Fatalf("unexpected error: %v", err)
	}
	if scenarios != nil {
		t.Fatalf("expected nil scenarios, got %v", scenarios)
	}
}
