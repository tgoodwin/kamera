package explore

import (
	"testing"

	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
)

func TestApplyInputTuningMaxDepth(t *testing.T) {
	base := tracecheck.ExploreConfig{MaxDepth: 10}
	got, err := ApplyInputTuning(base, coverage.InputTuning{MaxDepth: 50})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.MaxDepth != 50 {
		t.Fatalf("expected maxDepth=50, got %d", got.MaxDepth)
	}
}

func TestApplyInputTuningMaxDepthZeroIsIgnored(t *testing.T) {
	base := tracecheck.ExploreConfig{MaxDepth: 10}
	got, err := ApplyInputTuning(base, coverage.InputTuning{MaxDepth: 0})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.MaxDepth != 10 {
		t.Fatalf("expected base maxDepth=10 preserved, got %d", got.MaxDepth)
	}
}

func TestApplyInputTuningPermuteControllers(t *testing.T) {
	base := tracecheck.ExploreConfig{}
	got, err := ApplyInputTuning(base, coverage.InputTuning{
		PermuteControllers: []string{"ControllerA", "ControllerB"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got.Perturbations.PermuteOrder["ControllerA"] {
		t.Fatalf("expected ControllerA in PermuteOrder")
	}
	if !got.Perturbations.PermuteOrder["ControllerB"] {
		t.Fatalf("expected ControllerB in PermuteOrder")
	}
}

func TestApplyInputTuningStaleReads(t *testing.T) {
	base := tracecheck.ExploreConfig{}
	got, err := ApplyInputTuning(base, coverage.InputTuning{
		StaleReads: map[string][]string{
			"ControllerA": {"apps/Deployment", "core/ConfigMap"},
		},
		StaleLookback: map[string]int{
			"apps/Deployment": 3,
			"core/ConfigMap":  0, // should default to 1
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	st, ok := got.Perturbations.Staleness["ControllerA"]
	if !ok {
		t.Fatalf("expected Staleness entry for ControllerA")
	}
	if st.StaleReadBounds["apps/Deployment"] != 3 {
		t.Fatalf("expected lookback 3 for apps/Deployment, got %d", st.StaleReadBounds["apps/Deployment"])
	}
	if st.StaleReadBounds["core/ConfigMap"] != 1 {
		t.Fatalf("expected default lookback 1 for core/ConfigMap, got %d", st.StaleReadBounds["core/ConfigMap"])
	}
}

func TestApplyInputTuningUserActionReadyDepths(t *testing.T) {
	base := tracecheck.ExploreConfig{}
	got, err := ApplyInputTuning(base, coverage.InputTuning{
		UserActionReadyDepths: map[string]int{
			"0": 5,
			"1": 12,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Perturbations.UserActionReadyDepths[0] != 5 {
		t.Fatalf("expected depth 5 for action 0, got %d", got.Perturbations.UserActionReadyDepths[0])
	}
	if got.Perturbations.UserActionReadyDepths[1] != 12 {
		t.Fatalf("expected depth 12 for action 1, got %d", got.Perturbations.UserActionReadyDepths[1])
	}
}

func TestApplyInputTuningStalenessIntervals(t *testing.T) {
	base := tracecheck.ExploreConfig{}
	got, err := ApplyInputTuning(base, coverage.InputTuning{
		StalenessIntervals: []coverage.InputStalenessInterval{
			{Reconciler: "ControllerA", Kind: "apps/Deployment", StaleAt: 3, CatchUpAt: 8, Lag: -1},
			{Reconciler: "ControllerB", Kind: "core/ConfigMap", StaleAt: 1, CatchUpAt: 5, Lag: 2},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got.Perturbations.StalenessIntervals) != 2 {
		t.Fatalf("expected 2 staleness intervals, got %d", len(got.Perturbations.StalenessIntervals))
	}
	si0 := got.Perturbations.StalenessIntervals[0]
	if si0.ReconcilerID != "ControllerA" || si0.Kind != "apps/Deployment" || si0.StaleAt != 3 || si0.CatchUpAt != 8 || si0.Lag != -1 {
		t.Fatalf("unexpected interval 0: %+v", si0)
	}
}

func TestApplyInputTuningSearchMonteCarlo(t *testing.T) {
	seed := int64(42)
	trials := 10
	trialIdx := 3
	group := "my-scenario"
	base := tracecheck.ExploreConfig{}
	got, err := ApplyInputTuning(base, coverage.InputTuning{
		Search: coverage.InputSearchTuning{
			Mode: "monte_carlo",
			MonteCarlo: coverage.InputMonteCarloTuning{
				Seed:          &seed,
				Trials:        &trials,
				TrialIndex:    &trialIdx,
				ScenarioGroup: &group,
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.SearchMode != tracecheck.SearchModeMonteCarlo {
		t.Fatalf("expected monte_carlo search mode, got %q", got.SearchMode)
	}
	if got.MonteCarlo.Seed != 42 {
		t.Fatalf("expected seed 42, got %d", got.MonteCarlo.Seed)
	}
	if got.MonteCarlo.Trials != 10 {
		t.Fatalf("expected 10 trials, got %d", got.MonteCarlo.Trials)
	}
	if got.MonteCarlo.TrialIndex != 3 {
		t.Fatalf("expected trial index 3, got %d", got.MonteCarlo.TrialIndex)
	}
	if got.MonteCarlo.ScenarioGroup != "my-scenario" {
		t.Fatalf("expected scenarioGroup my-scenario, got %q", got.MonteCarlo.ScenarioGroup)
	}
}

func TestApplyInputTuningSearchInvalidModeReturnsError(t *testing.T) {
	base := tracecheck.ExploreConfig{}
	_, err := ApplyInputTuning(base, coverage.InputTuning{
		Search: coverage.InputSearchTuning{Mode: "bogus_mode"},
	})
	if err == nil {
		t.Fatalf("expected error for invalid search mode")
	}
}

func TestApplyInputTuningDoesNotMutateBase(t *testing.T) {
	base := tracecheck.ExploreConfig{MaxDepth: 5}
	_, err := ApplyInputTuning(base, coverage.InputTuning{
		MaxDepth:           99,
		PermuteControllers: []string{"X"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if base.MaxDepth != 5 {
		t.Fatalf("base was mutated: maxDepth=%d", base.MaxDepth)
	}
	if base.Perturbations.PermuteOrder != nil {
		t.Fatalf("base.Perturbations.PermuteOrder was mutated")
	}
}
