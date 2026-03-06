package tracecheck

import "testing"

func TestExploreConfigCloneCopiesSearchFields(t *testing.T) {
	cfg := ExploreConfig{
		SearchMode: SearchModeMonteCarlo,
		MonteCarlo: MonteCarloConfig{
			Seed:          4242,
			Trials:        100,
			TrialIndex:    5,
			ScenarioGroup: "scenario-a",
		},
	}

	clone := cfg.Clone()
	if clone.SearchMode != SearchModeMonteCarlo {
		t.Fatalf("expected cloned search mode monte_carlo, got %q", clone.SearchMode)
	}
	if clone.MonteCarlo.Seed != 4242 {
		t.Fatalf("expected cloned seed 4242, got %d", clone.MonteCarlo.Seed)
	}
	if clone.MonteCarlo.Trials != 100 {
		t.Fatalf("expected cloned trials 100, got %d", clone.MonteCarlo.Trials)
	}
	if clone.MonteCarlo.TrialIndex != 5 {
		t.Fatalf("expected cloned trial index 5, got %d", clone.MonteCarlo.TrialIndex)
	}
	if clone.MonteCarlo.ScenarioGroup != "scenario-a" {
		t.Fatalf("expected cloned scenario group scenario-a, got %q", clone.MonteCarlo.ScenarioGroup)
	}
}

func TestDeriveMonteCarloSeedDeterministic(t *testing.T) {
	a := DeriveMonteCarloSeed(1337, "scenario/input#2", 7)
	b := DeriveMonteCarloSeed(1337, "scenario/input#2", 7)
	if a != b {
		t.Fatalf("expected deterministic seed derivation for same inputs, got %d and %d", a, b)
	}
}

func TestDeriveMonteCarloSeedVariesByInput(t *testing.T) {
	base := DeriveMonteCarloSeed(1337, "scenario/input#2", 7)
	withDifferentBase := DeriveMonteCarloSeed(2024, "scenario/input#2", 7)
	withDifferentGroup := DeriveMonteCarloSeed(1337, "scenario/input#9", 7)
	withDifferentTrial := DeriveMonteCarloSeed(1337, "scenario/input#2", 8)

	if base == withDifferentBase {
		t.Fatalf("expected base seed to influence derived seed")
	}
	if base == withDifferentGroup {
		t.Fatalf("expected scenario group to influence derived seed")
	}
	if base == withDifferentTrial {
		t.Fatalf("expected trial index to influence derived seed")
	}
}
