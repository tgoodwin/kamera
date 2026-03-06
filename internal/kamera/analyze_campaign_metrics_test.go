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
	require.Contains(t, out, "Campaign metrics by invocation")
	require.Contains(t, out, "invocation: inv-a")
	require.Contains(t, out, "invocation: inv-b")

	parsed := parseCampaignMetricsBlockOutput(t, out)
	require.Equal(t, "5", parsed["inv-a"]["unique node visits"])
	require.Equal(t, "7", parsed["inv-a"]["total node visits"])
	require.Equal(t, "9", parsed["inv-a"]["unique resource states"])
	require.Equal(t, "0s", parsed["inv-a"]["duration"])
	require.Equal(t, "7", parsed["inv-b"]["unique node visits"])
	require.Equal(t, "8", parsed["inv-b"]["total node visits"])
	require.Equal(t, "9", parsed["inv-b"]["unique resource states"])
	require.Equal(t, "0s", parsed["inv-b"]["duration"])
	require.NotContains(t, out, "\n  dumps:")
	require.NotContains(t, out, "error dumps:")
	require.Contains(t, stderr.String(), "skipped 1 dump(s) without invocation_id")
}

func TestRunAnalyzeCampaignMetricsReportsErrorAndAbortContext(t *testing.T) {
	dir := t.TempDir()

	require.NoError(t, writeCampaignMetricsDumpWithStateErrors(
		t,
		filepath.Join(dir, "ok-with-aborts.jsonl"),
		"inv-a",
		&analysis.CampaignMetrics{
			UniqueNodeVisits:     5,
			TotalNodeVisits:      6,
			UniqueResourceStates: 4,
			DurationNS:           25,
		},
		nil,
		[]string{"max depth reached", "no eligible views for ControllerX"},
	))
	require.NoError(t, writeCampaignMetricsDumpWithStateErrors(
		t,
		filepath.Join(dir, "runner-error.jsonl"),
		"inv-a",
		nil,
		map[string]string{
			"status":        "error",
			"error_phase":   "run_scenario",
			"error_message": "boom",
		},
		nil,
	))

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := RunAnalyze([]string{"campaign-metrics", dir}, &stdout, &stderr)
	require.Equal(t, 0, code)

	parsed := parseCampaignMetricsBlockOutput(t, stdout.String())
	require.Equal(t, "2", parsed["inv-a"]["aborted states"])
	require.Equal(t, "1", parsed["inv-a"]["max-depth aborted states"])
	require.Equal(t, "0s", parsed["inv-a"]["duration"])
}

func writeCampaignMetricsDump(t *testing.T, path, invocationID string, metrics *analysis.CampaignMetrics) error {
	return writeCampaignMetricsDumpWithStateErrors(t, path, invocationID, metrics, nil, nil)
}

func writeCampaignMetricsDumpWithStateErrors(
	t *testing.T,
	path, invocationID string,
	metrics *analysis.CampaignMetrics,
	extraAttributes map[string]string,
	stateErrors []string,
) error {
	t.Helper()

	attributes := map[string]string{}
	if invocationID != "" {
		attributes["invocation_id"] = invocationID
	}
	for key, value := range extraAttributes {
		attributes[key] = value
	}
	states := make([]analysis.DumpResultState, 0, len(stateErrors))
	for idx, errMsg := range stateErrors {
		states = append(states, analysis.DumpResultState{
			ID:    "state-" + string(rune('a'+idx)),
			Error: errMsg,
			State: analysis.DumpStateNode{
				Contents: analysis.DumpStateSnapshot{},
			},
		})
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
		States:          states,
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

func parseCampaignMetricsBlockOutput(t *testing.T, output string) map[string]map[string]string {
	t.Helper()

	lines := strings.Split(output, "\n")
	out := map[string]map[string]string{}
	currentInvocation := ""
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, "invocation: ") {
			currentInvocation = strings.TrimPrefix(trimmed, "invocation: ")
			if _, ok := out[currentInvocation]; !ok {
				out[currentInvocation] = map[string]string{}
			}
			continue
		}
		if currentInvocation == "" {
			continue
		}
		parts := strings.SplitN(trimmed, ":", 2)
		if len(parts) != 2 {
			continue
		}
		out[currentInvocation][strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
	}
	require.NotEmpty(t, out)
	return out
}
