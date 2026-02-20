package interactive

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tgoodwin/kamera/pkg/analysis"
)

func TestLoadDumpCatalogEntries(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "readme.txt"), []byte("not a dump"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "stats.json"), []byte(`{"totalNodeVisits":42}`), 0o644))
	writeCatalogDump(t, filepath.Join(dir, "workflow_alpha_2.jsonl"), "KPA", "RevisionController")

	entries, err := LoadDumpCatalogEntries(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)

	entry := entries[0]
	require.Equal(t, "workflow_alpha_2.jsonl", entry.File)
	require.Equal(t, "workflow_alpha", entry.Scenario)
	require.Equal(t, "workflow alpha", entry.ScenarioLabel)
	require.Equal(t, 2, entry.RunIndex)
	require.Equal(t, 1, entry.States)
	require.Equal(t, 2, entry.Steps)
	require.Equal(t, "KPA", entry.InitialController)
	require.Equal(t, 1, entry.InitialObjects)
	require.Contains(t, entry.Controllers, "KPA")
	require.Contains(t, entry.Controllers, "RevisionController")
}

func TestLoadDumpCatalogEntriesPrefersExplicitDumpContext(t *testing.T) {
	dir := t.TempDir()
	dumpPath := filepath.Join(dir, "opaque_file_name_99.jsonl")
	writeCatalogDumpWithContext(t, dumpPath, "KPA", "RevisionController")

	entries, err := LoadDumpCatalogEntries(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)

	entry := entries[0]
	require.Equal(t, "Generated Scenario A", entry.Scenario)
	require.Equal(t, "Generated Scenario A", entry.ScenarioLabel)
	require.Equal(t, 42, entry.RunIndex)
	require.Equal(t, "input-workflow-v1", entry.ScenarioWorkflow)
	require.Equal(t, "inputs.json#generated-a", entry.ScenarioInputRef)
	require.Equal(t, "golden", entry.ScenarioAttributes["suite"])
}

func TestRenderDumpCatalogTable(t *testing.T) {
	table := RenderDumpCatalogTable([]DumpCatalogEntry{
		{
			File:              "workflow_alpha_2.jsonl",
			ScenarioLabel:     "workflow alpha",
			RunIndex:          2,
			States:            3,
			ConvergedStates:   2,
			AbortedStates:     1,
			Paths:             5,
			Steps:             7,
			ScenarioWorkflow:  "workflow-from-context",
			InitialController: "KPA",
			InitialObjects:    4,
		},
	})

	require.Contains(t, table, "file")
	require.Contains(t, table, "workflow_alpha_2.jsonl")
	require.Contains(t, table, "workflow alpha")
	require.Contains(t, table, "workflow-from-context")
}

func writeCatalogDump(t *testing.T, path string, controllers ...string) {
	t.Helper()
	steps := make([]analysis.DumpReconcileResult, 0, len(controllers))
	for idx, controller := range controllers {
		step := analysis.DumpReconcileResult{ControllerID: controller}
		if idx == 0 {
			step.StateBefore = []analysis.DumpObjectVersion{{}}
		}
		steps = append(steps, step)
	}

	dump := analysis.Dump{
		States: []analysis.DumpResultState{
			{
				ID: "state-0",
				State: analysis.DumpStateNode{
					Contents: analysis.DumpStateSnapshot{},
				},
				Paths: [][]analysis.DumpReconcileResult{steps},
			},
		},
	}

	data, err := json.Marshal(dump)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o644))
}

func writeCatalogDumpWithContext(t *testing.T, path string, controllers ...string) {
	t.Helper()
	steps := make([]analysis.DumpReconcileResult, 0, len(controllers))
	for idx, controller := range controllers {
		step := analysis.DumpReconcileResult{ControllerID: controller}
		if idx == 0 {
			step.StateBefore = []analysis.DumpObjectVersion{{}}
		}
		steps = append(steps, step)
	}

	runIndex := 42
	dump := analysis.Dump{
		Context: &analysis.DumpContext{
			Scenario: &analysis.DumpScenarioContext{
				Name:     "Generated Scenario A",
				RunIndex: &runIndex,
				Workflow: "input-workflow-v1",
				InputRef: "inputs.json#generated-a",
				Attributes: map[string]string{
					"suite": "golden",
				},
			},
		},
		States: []analysis.DumpResultState{
			{
				ID: "state-0",
				State: analysis.DumpStateNode{
					Contents: analysis.DumpStateSnapshot{},
				},
				Paths: [][]analysis.DumpReconcileResult{steps},
			},
		},
	}

	data, err := json.Marshal(dump)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o644))
}
