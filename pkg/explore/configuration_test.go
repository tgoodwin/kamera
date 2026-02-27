package explore

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tgoodwin/kamera/pkg/tracecheck"
)

func TestLoadExploreConfigFromFileIgnoresPerturbationsBlock(t *testing.T) {
	base := tracecheck.ExploreConfig{
		MaxDepth: 5,
		Perturbations: tracecheck.PerturbationConfig{
			PermuteOrder: map[tracecheck.ReconcilerID]bool{
				"BaseController": false,
			},
			Staleness: map[tracecheck.ReconcilerID]tracecheck.StalenessConfig{
				"BaseController": {
					StaleReadBounds: tracecheck.LookbackLimits{
						"apps/Deployment": tracecheck.LookbackLimit(2),
					},
					MaxRestarts: 1,
				},
			},
		},
	}

	configPath := filepath.Join(t.TempDir(), "explore-config.json")
	data := []byte(`{
  "maxDepth": 11,
  "timeout": "30s",
  "perturbations": {
    "permuteOrder": {
      "ServiceReconciler": true
    },
    "staleness": {
      "ServiceReconciler": {
        "staleReadBounds": {
          "core/ConfigMap": 3,
          "core/Secret": 1
        },
        "maxRestarts": 4
      }
    }
  }
}`)
	if err := os.WriteFile(configPath, data, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	got, err := LoadExploreConfigFromFile(configPath, base)
	if err != nil {
		t.Fatalf("LoadExploreConfigFromFile() error = %v", err)
	}

	if got.MaxDepth != 11 {
		t.Fatalf("expected maxDepth=11, got %d", got.MaxDepth)
	}
	if got.Timeout != 30*time.Second {
		t.Fatalf("expected timeout=30s, got %v", got.Timeout)
	}
	if got.Perturbations.PermuteOrder["ServiceReconciler"] {
		t.Fatalf("expected perturbations block to be ignored")
	}

	if _, ok := got.Perturbations.Staleness["ServiceReconciler"]; ok {
		t.Fatalf("expected staleness perturbations block to be ignored")
	}
}

func TestLoadExploreConfigFromFileIgnoresNegativeLookbackInPerturbationsBlock(t *testing.T) {
	base := tracecheck.ExploreConfig{}

	configPath := filepath.Join(t.TempDir(), "explore-config.json")
	data := []byte(`{
  "perturbations": {
    "staleness": {
      "ServiceReconciler": {
        "staleReadBounds": {
          "core/ConfigMap": -1
        }
      }
    }
  }
}`)
	if err := os.WriteFile(configPath, data, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if _, err := LoadExploreConfigFromFile(configPath, base); err != nil {
		t.Fatalf("expected perturbations block to be ignored, got error: %v", err)
	}
}

func TestLoadExploreConfigFromFileAppliesSearchBlock(t *testing.T) {
	base := tracecheck.ExploreConfig{
		SearchMode: tracecheck.SearchModeDFS,
		MonteCarlo: tracecheck.MonteCarloConfig{
			Seed:          1,
			Trials:        1,
			TrialIndex:    0,
			ScenarioGroup: "base",
		},
	}

	configPath := filepath.Join(t.TempDir(), "explore-config.json")
	data := []byte(`{
  "search": {
    "mode": "monte_carlo",
    "monteCarlo": {
      "seed": 1337,
      "trials": 1000,
      "trialIndex": 7,
      "scenarioGroup": "scenario/input#7"
    }
  }
}`)
	if err := os.WriteFile(configPath, data, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	got, err := LoadExploreConfigFromFile(configPath, base)
	if err != nil {
		t.Fatalf("LoadExploreConfigFromFile() error = %v", err)
	}

	if got.SearchMode != tracecheck.SearchModeMonteCarlo {
		t.Fatalf("expected search mode monte_carlo, got %q", got.SearchMode)
	}
	if got.MonteCarlo.Seed != 1337 {
		t.Fatalf("expected monte carlo seed 1337, got %d", got.MonteCarlo.Seed)
	}
	if got.MonteCarlo.Trials != 1000 {
		t.Fatalf("expected monte carlo trials 1000, got %d", got.MonteCarlo.Trials)
	}
	if got.MonteCarlo.TrialIndex != 7 {
		t.Fatalf("expected monte carlo trial index 7, got %d", got.MonteCarlo.TrialIndex)
	}
	if got.MonteCarlo.ScenarioGroup != "scenario/input#7" {
		t.Fatalf("expected scenarioGroup to be set from config")
	}
}

func TestLoadExploreConfigFromFileRejectsUnknownSearchMode(t *testing.T) {
	base := tracecheck.ExploreConfig{}

	configPath := filepath.Join(t.TempDir(), "explore-config.json")
	data := []byte(`{
  "search": {
    "mode": "totally_new_mode"
  }
}`)
	if err := os.WriteFile(configPath, data, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if _, err := LoadExploreConfigFromFile(configPath, base); err == nil {
		t.Fatalf("expected unknown search mode to return an error")
	}
}
