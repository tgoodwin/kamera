package tracecheck

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
)

func TestNewExplorerBuilder_DefaultConfigEnablesOptimizations(t *testing.T) {
	builder := NewExplorerBuilder(runtime.NewScheme())
	cfg := builder.Config()

	if !cfg.OptimizationsEnabled() {
		t.Fatalf("expected optimizations enabled by default")
	}
	if !cfg.Optimizations.EarlyConvergence {
		t.Fatalf("expected EarlyConvergence enabled by default")
	}
	if !cfg.Optimizations.CompletedPathDedup {
		t.Fatalf("expected CompletedPathDedup enabled by default")
	}
	if !cfg.Optimizations.OrderingPruning {
		t.Fatalf("expected OrderingPruning enabled by default")
	}
	if !cfg.Optimizations.CachePrediction {
		t.Fatalf("expected CachePrediction enabled by default")
	}
	if !cfg.Optimizations.SubtreeCompletion {
		t.Fatalf("expected SubtreeCompletion enabled by default")
	}
	if !cfg.Optimizations.OnlyPermuteTriggered {
		t.Fatalf("expected OnlyPermuteTriggered enabled by default")
	}

	for id, enabled := range cfg.Perturbations.PermuteOrder {
		if enabled {
			t.Fatalf("expected permutation to be opt-in, but %s is enabled by default", id)
		}
	}

	if len(cfg.Perturbations.Staleness) != 0 {
		t.Fatalf("expected no perturbation config by default, found %d entries", len(cfg.Perturbations.Staleness))
	}

	if cfg.SearchMode != SearchModeDFS {
		t.Fatalf("expected default search mode dfs, got %q", cfg.SearchMode)
	}
	if cfg.MonteCarlo != (MonteCarloConfig{}) {
		t.Fatalf("expected monte carlo config to remain zero-value in dfs mode, got %+v", cfg.MonteCarlo)
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
	cfg.Perturbations.PermuteOrder["ServiceController"] = true
	cfg.Perturbations.PermuteOrder["EndpointsController"] = false
	builder.SetConfig(cfg)

	got := builder.Config()
	if !got.Perturbations.PermuteOrder["ServiceController"] {
		t.Fatalf("expected ServiceController permutation enabled")
	}
	if got.Perturbations.PermuteOrder["EndpointsController"] {
		t.Fatalf("expected EndpointsController permutation disabled")
	}
}

func TestExplorerBuilderSetConfigSetsPerturbationConfig(t *testing.T) {
	builder := NewExplorerBuilder(runtime.NewScheme())
	cfg := builder.Config()
	cfg.Perturbations.Staleness["ServiceController"] = StalenessConfig{
		StaleReadBounds: LookbackLimits{
			"core/Service": 2,
		},
		MaxRestarts: 1,
	}
	builder.SetConfig(cfg)

	got := builder.Config()
	rc, ok := got.Perturbations.Staleness["ServiceController"]
	if !ok {
		t.Fatalf("expected ServiceController perturbation config to be set")
	}
	if rc.MaxRestarts != 1 {
		t.Fatalf("expected MaxRestarts=1, got %d", rc.MaxRestarts)
	}
	if rc.StaleReadBounds["core/Service"] != 2 {
		t.Fatalf("expected core/Service lookback=2, got %d", rc.StaleReadBounds["core/Service"])
	}
}
