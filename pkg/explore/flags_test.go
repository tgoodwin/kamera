package explore

import (
	"flag"
	"testing"
)

func TestInputsPathDefault(t *testing.T) {
	if InputsPath() != "" {
		t.Fatalf("expected empty inputs path by default")
	}
}

func TestPerturbEnabledDefault(t *testing.T) {
	if !PerturbEnabled() {
		t.Fatalf("expected perturb enabled by default")
	}
}

func TestParallelProcessesDefault(t *testing.T) {
	if ParallelProcessesEnabled() {
		t.Fatalf("expected parallel-processes to be disabled by default")
	}
}

func TestParallelChildIndexDefault(t *testing.T) {
	if ParallelChildIndex() != -1 {
		t.Fatalf("expected parallel-child-index default -1, got %d", ParallelChildIndex())
	}
}

func TestDumpStatsFlagRemoved(t *testing.T) {
	if got := flag.CommandLine.Lookup("dump-stats"); got != nil {
		t.Fatalf("expected dump-stats flag to be removed")
	}
}
