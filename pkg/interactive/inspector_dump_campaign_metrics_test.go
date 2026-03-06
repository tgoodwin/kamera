package interactive

import (
	"path/filepath"
	"testing"

	"github.com/tgoodwin/kamera/pkg/analysis"
)

func TestSaveInspectorDumpWithCampaignMetrics(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.jsonl")
	metrics := &analysis.CampaignMetrics{
		UniqueNodeVisits:    5,
		TotalNodeVisits:     8,
		UniqueResourceStates: 4,
		DurationNS:          98765,
	}

	err := SaveInspectorDumpWithContextAndStatsAndCampaignMetrics(nil, nil, path, nil, nil, metrics)
	if err != nil {
		t.Fatalf("save dump: %v", err)
	}

	dump, err := analysis.LoadDump(path)
	if err != nil {
		t.Fatalf("load dump: %v", err)
	}
	if dump.CampaignMetrics == nil {
		t.Fatalf("expected campaign metrics in dump")
	}
	if dump.CampaignMetrics.UniqueNodeVisits != 5 {
		t.Fatalf("expected unique node visits 5, got %d", dump.CampaignMetrics.UniqueNodeVisits)
	}
	if dump.CampaignMetrics.TotalNodeVisits != 8 {
		t.Fatalf("expected total node visits 8, got %d", dump.CampaignMetrics.TotalNodeVisits)
	}
	if dump.CampaignMetrics.UniqueResourceStates != 4 {
		t.Fatalf("expected unique resource states 4, got %d", dump.CampaignMetrics.UniqueResourceStates)
	}
	if dump.CampaignMetrics.DurationNS != 98765 {
		t.Fatalf("expected durationNs 98765, got %d", dump.CampaignMetrics.DurationNS)
	}
}

