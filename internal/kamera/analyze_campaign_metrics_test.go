package kamera

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tgoodwin/kamera/pkg/analysis"
)

func TestRunAnalyzeCampaignMetricsGroupsByInvocation(t *testing.T) {
	dir := t.TempDir()

	require.NoError(t, writeCampaignMetricsDump(t, filepath.Join(dir, "a1.jsonl"), "inv-a", &analysis.CampaignMetrics{
		UniqueNodeVisits:     1,
		TotalNodeVisits:      2,
		UniqueResourceStates: 3,
		DurationNS:           10,
	}))
	require.NoError(t, writeCampaignMetricsDump(t, filepath.Join(dir, "a2.jsonl"), "inv-a", &analysis.CampaignMetrics{
		UniqueNodeVisits:     4,
		TotalNodeVisits:      5,
		UniqueResourceStates: 6,
		DurationNS:           20,
	}))
	require.NoError(t, writeCampaignMetricsDump(t, filepath.Join(dir, "b1.jsonl"), "inv-b", &analysis.CampaignMetrics{
		UniqueNodeVisits:     7,
		TotalNodeVisits:      8,
		UniqueResourceStates: 9,
		DurationNS:           30,
	}))
	require.NoError(t, writeCampaignMetricsDump(t, filepath.Join(dir, "missing-invocation.jsonl"), "", &analysis.CampaignMetrics{
		UniqueNodeVisits:     99,
		TotalNodeVisits:      99,
		UniqueResourceStates: 99,
		DurationNS:           99,
	}))

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := RunAnalyze([]string{"campaign-metrics", dir}, &stdout, &stderr)
	require.Equal(t, 0, code)

	out := stdout.String()
	require.Contains(t, out, "invocation_id")
	require.Contains(t, out, "inv-a")
	require.Contains(t, out, "inv-b")

	parsed := parseCampaignMetricsOutput(t, out)
	require.Equal(t, []string{"2", "5", "7", "9", "30"}, parsed["inv-a"])
	require.Equal(t, []string{"1", "7", "8", "9", "30"}, parsed["inv-b"])
	require.Contains(t, stderr.String(), "skipped 1 dump(s) without invocation_id")
}

func writeCampaignMetricsDump(t *testing.T, path, invocationID string, metrics *analysis.CampaignMetrics) error {
	t.Helper()

	attributes := map[string]string{}
	if invocationID != "" {
		attributes["invocation_id"] = invocationID
	}
	dump := analysis.Dump{
		Context: &analysis.DumpContext{
			Scenario: &analysis.DumpScenarioContext{
				Name:       "test",
				RunIndex:   intPtr(0),
				Workflow:   "workflow",
				InputRef:   "inputs.json#0",
				Attributes: attributes,
			},
		},
		CampaignMetrics: metrics,
		Objects:         []analysis.DumpObject{},
		States:          []analysis.DumpResultState{},
	}
	raw, err := json.Marshal(dump)
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0644)
}

func intPtr(v int) *int {
	return &v
}

func parseCampaignMetricsOutput(t *testing.T, output string) map[string][]string {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	out := map[string][]string{}
	for idx, line := range lines {
		if idx == 0 {
			continue // header
		}
		fields := strings.Fields(line)
		if len(fields) != 6 {
			continue
		}
		out[fields[0]] = fields[1:]
	}
	return out
}
