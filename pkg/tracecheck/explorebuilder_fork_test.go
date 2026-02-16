package tracecheck

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
)

func TestExplorerBuilderForkIsolatesStoresAndConfig(t *testing.T) {
	scheme := runtime.NewScheme()
	b := NewExplorerBuilder(scheme)
	b.WithMaxDepth(5)

	fork := b.Fork()

	if b.snapStore == fork.snapStore {
		t.Fatalf("expected fork to have a fresh snapshot store")
	}
	if b.emitter == fork.emitter {
		t.Fatalf("expected fork to have a fresh emitter")
	}

	fork.config.MaxDepth = 99
	if b.config.MaxDepth == 99 {
		t.Fatalf("expected config to be cloned")
	}

	fork.config.Perturbations.PermuteOrder[ReconcilerID("X")] = true
	if b.config.Perturbations.PermuteOrder[ReconcilerID("X")] {
		t.Fatalf("expected permute map to be cloned")
	}
}
