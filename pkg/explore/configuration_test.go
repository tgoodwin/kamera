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
