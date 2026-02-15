package tracecheck

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
)

func TestNewExplorerBuilder_DefaultConfigIsOptIn(t *testing.T) {
	builder := NewExplorerBuilder(runtime.NewScheme())
	cfg := builder.Config()

	if cfg.OptimizationsEnabled() {
		t.Fatalf("expected optimizations disabled by default")
	}

	for id, enabled := range cfg.PermuteOrder {
		if enabled {
			t.Fatalf("expected permutation to be opt-in, but %s is enabled by default", id)
		}
	}

	if len(cfg.perturbationCfg) != 0 {
		t.Fatalf("expected no perturbation config by default, found %d entries", len(cfg.perturbationCfg))
	}
}

func TestOptimizationConfigAnyEnabled_OnlyPermuteTriggeredDoesNotEnableOptimizations(t *testing.T) {
	opt := OptimizationConfig{OnlyPermuteTriggered: true}
	if opt.AnyEnabled() {
		t.Fatalf("expected OnlyPermuteTriggered to be a scope modifier, not an optimization toggle")
	}
}

func TestExplorerBuilderSetConfigSetsPermuteOrder(t *testing.T) {
	builder := NewExplorerBuilder(runtime.NewScheme())
	cfg := builder.Config()
	cfg.PermuteOrder["ServiceController"] = true
	cfg.PermuteOrder["EndpointsController"] = false
	builder.SetConfig(cfg)

	got := builder.Config()
	if !got.PermuteOrder["ServiceController"] {
		t.Fatalf("expected ServiceController permutation enabled")
	}
	if got.PermuteOrder["EndpointsController"] {
		t.Fatalf("expected EndpointsController permutation disabled")
	}
}
