package main

import (
	"strings"
	"testing"

	"github.com/tgoodwin/kamera/pkg/explore"
)

func TestParseParallelModeDefaultsToGoroutine(t *testing.T) {
	mode, err := parseParallelMode("")
	if err != nil {
		t.Fatalf("parseParallelMode error = %v", err)
	}
	if mode != parallelModeGoroutine {
		t.Fatalf("expected %q, got %q", parallelModeGoroutine, mode)
	}
}

func TestParseParallelModeAcceptsProcess(t *testing.T) {
	mode, err := parseParallelMode("PROCESS")
	if err != nil {
		t.Fatalf("parseParallelMode error = %v", err)
	}
	if mode != parallelModeProcess {
		t.Fatalf("expected %q, got %q", parallelModeProcess, mode)
	}
}

func TestParseParallelModeRejectsUnknown(t *testing.T) {
	_, err := parseParallelMode("threads")
	if err == nil {
		t.Fatal("expected error for invalid mode")
	}
	if !strings.Contains(err.Error(), "invalid --parallel-mode") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestScenariosForChildIndexAllScenariosWhenUnset(t *testing.T) {
	scenarios := []explore.Scenario{{Name: "a"}, {Name: "b"}}
	got, err := scenariosForChildIndex(scenarios, -1)
	if err != nil {
		t.Fatalf("scenariosForChildIndex error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 scenarios, got %d", len(got))
	}
}

func TestScenariosForChildIndexSelectsSingleScenario(t *testing.T) {
	scenarios := []explore.Scenario{{Name: "a"}, {Name: "b"}}
	got, err := scenariosForChildIndex(scenarios, 1)
	if err != nil {
		t.Fatalf("scenariosForChildIndex error = %v", err)
	}
	if len(got) != 1 || got[0].Name != "b" {
		t.Fatalf("unexpected scenarios: %#v", got)
	}
}

func TestScenariosForChildIndexRejectsOutOfRange(t *testing.T) {
	scenarios := []explore.Scenario{{Name: "a"}}
	_, err := scenariosForChildIndex(scenarios, 1)
	if err == nil {
		t.Fatal("expected out-of-range error")
	}
	if !strings.Contains(err.Error(), "scenario-index 1 out of range") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildParallelChildArgsAppendsOverrides(t *testing.T) {
	args := buildParallelChildArgs([]string{"--parallel", "--fuzz-cases=10"}, 7)
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "--parallel-child=true") {
		t.Fatalf("expected child mode override in args: %q", joined)
	}
	if !strings.Contains(joined, "--scenario-index=7") {
		t.Fatalf("expected scenario index override in args: %q", joined)
	}
	if !strings.Contains(joined, "--parallel-mode=goroutine") {
		t.Fatalf("expected goroutine override in args: %q", joined)
	}
	if !strings.Contains(joined, "--interactive=false") {
		t.Fatalf("expected interactive override in args: %q", joined)
	}
}
