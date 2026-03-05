package interactive

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tgoodwin/kamera/pkg/analysis"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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

func TestLoadDumpCatalogEntriesPrefersMonteCarloAggregateEntries(t *testing.T) {
	dir := t.TempDir()

	writeCatalogDumpWithContextAndAttrs(
		t,
		filepath.Join(dir, "trial.jsonl"),
		"Scenario Alpha",
		1,
		map[string]string{
			"search_mode": "monte_carlo",
			"mc_group_id": "inputs.json#alpha",
			"mc_role":     "trial",
		},
		"KPA",
	)
	writeCatalogDumpWithContextAndAttrs(
		t,
		filepath.Join(dir, "aggregate.jsonl"),
		"Scenario Alpha",
		2,
		map[string]string{
			"search_mode": "monte_carlo",
			"mc_group_id": "inputs.json#alpha",
			"mc_role":     "aggregate",
		},
		"KPA",
	)
	writeCatalogDumpWithContextAndAttrs(
		t,
		filepath.Join(dir, "regular.jsonl"),
		"Scenario Beta",
		3,
		map[string]string{
			"suite": "baseline",
		},
		"RevisionController",
	)

	entries, err := LoadDumpCatalogEntries(dir)
	require.NoError(t, err)
	require.Len(t, entries, 2)

	files := []string{entries[0].File, entries[1].File}
	require.Contains(t, files, "aggregate.jsonl")
	require.Contains(t, files, "regular.jsonl")
	require.NotContains(t, files, "trial.jsonl")
}

func TestLoadDumpCatalogEntriesKeepsNewestExplicitMonteCarloAggregate(t *testing.T) {
	dir := t.TempDir()

	trialPath := filepath.Join(dir, "trial.jsonl")
	oldAggPath := filepath.Join(dir, "aggregate_old.jsonl")
	newAggPath := filepath.Join(dir, "aggregate_new.jsonl")

	writeCatalogDumpWithContextAndAttrs(
		t,
		trialPath,
		"Scenario Alpha",
		1,
		map[string]string{
			"search_mode": "monte_carlo",
			"mc_group_id": "inputs.json#alpha",
			"mc_role":     "trial",
		},
		"KPA",
	)
	writeCatalogDumpWithContextAndAttrs(
		t,
		oldAggPath,
		"Scenario Alpha",
		2,
		map[string]string{
			"search_mode": "monte_carlo",
			"mc_group_id": "inputs.json#alpha",
			"mc_role":     "aggregate",
		},
		"KPA",
	)
	writeCatalogDumpWithContextAndAttrs(
		t,
		newAggPath,
		"Scenario Alpha",
		3,
		map[string]string{
			"search_mode": "monte_carlo",
			"mc_group_id": "inputs.json#alpha",
			"mc_role":     "aggregate",
		},
		"KPA",
	)

	oldTime := time.Now().Add(-1 * time.Hour)
	newTime := time.Now()
	require.NoError(t, os.Chtimes(oldAggPath, oldTime, oldTime))
	require.NoError(t, os.Chtimes(newAggPath, newTime, newTime))

	entries, err := LoadDumpCatalogEntries(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "aggregate_new.jsonl", entries[0].File)
	require.Equal(t, "aggregate", entries[0].ScenarioAttributes["mc_role"])
}

func TestLoadDumpCatalogEntriesSynthesizesMonteCarloAggregateWhenNoAggregateFile(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticTrialDump(t, filepath.Join(dir, "trial_0.jsonl"), syntheticTrialSpec{
		ScenarioName: "Scenario Alpha",
		GroupID:      "inputs.json#alpha",
		TrialIndex:   0,
		TrialCount:   2,
		StateHash:    "state-a",
		Controllers:  []string{"KPA"},
	})
	writeSyntheticTrialDump(t, filepath.Join(dir, "trial_1.jsonl"), syntheticTrialSpec{
		ScenarioName: "Scenario Alpha",
		GroupID:      "inputs.json#alpha",
		TrialIndex:   1,
		TrialCount:   2,
		StateHash:    "state-b",
		Controllers:  []string{"RevisionController"},
	})

	entries, err := LoadDumpCatalogEntries(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "aggregate", entries[0].ScenarioAttributes["mc_role"])
	require.Len(t, entries[0].AggregateMemberPaths, 2)
}

func TestLoadInspectorDumpForCatalogEntryAggregatesTrialMemberPaths(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticTrialDump(t, filepath.Join(dir, "trial_0.jsonl"), syntheticTrialSpec{
		ScenarioName: "Scenario Alpha",
		GroupID:      "inputs.json#alpha",
		TrialIndex:   0,
		TrialCount:   3,
		StateHash:    "state-a",
		Controllers:  []string{"KPA"},
	})
	writeSyntheticTrialDump(t, filepath.Join(dir, "trial_1.jsonl"), syntheticTrialSpec{
		ScenarioName: "Scenario Alpha",
		GroupID:      "inputs.json#alpha",
		TrialIndex:   1,
		TrialCount:   3,
		StateHash:    "state-a",
		Controllers:  []string{"KPA", "RevisionController"},
	})
	writeSyntheticTrialDump(t, filepath.Join(dir, "trial_2.jsonl"), syntheticTrialSpec{
		ScenarioName: "Scenario Alpha",
		GroupID:      "inputs.json#alpha",
		TrialIndex:   2,
		TrialCount:   3,
		StateHash:    "state-b",
		Controllers:  []string{"Autoscaler"},
	})

	entries, err := LoadDumpCatalogEntries(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)

	states, _, err := LoadInspectorDumpForCatalogEntry(entries[0])
	require.NoError(t, err)
	require.Len(t, states, 2)

	totalPaths := 0
	for _, state := range states {
		totalPaths += len(state.Paths)
	}
	require.Equal(t, 3, totalPaths)
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

func writeCatalogDumpWithContextAndAttrs(
	t *testing.T,
	path string,
	scenarioName string,
	runIndex int,
	attrs map[string]string,
	controllers ...string,
) {
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
		Context: &analysis.DumpContext{
			Scenario: &analysis.DumpScenarioContext{
				Name:       scenarioName,
				RunIndex:   &runIndex,
				Attributes: attrs,
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

type syntheticTrialSpec struct {
	ScenarioName string
	GroupID      string
	TrialIndex   int
	TrialCount   int
	StateHash    string
	Controllers  []string
}

func writeSyntheticTrialDump(t *testing.T, path string, spec syntheticTrialSpec) {
	t.Helper()

	compositeKey := snapshot.NewCompositeKeyWithGroup("example.dev", "Thing", "default", "sample", "obj-1")
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]interface{}{
				"name":        "sample",
				"namespace":   "default",
				"annotations": map[string]interface{}{"synthetic-state": spec.StateHash},
			},
		},
	}
	versionHash, err := snapshot.NewDefaultHasher().Hash(obj)
	require.NoError(t, err)
	object := analysis.DumpObject{
		Hash:   versionHash,
		Object: obj.Object,
	}

	steps := make([]analysis.DumpReconcileResult, 0, len(spec.Controllers))
	for _, controller := range spec.Controllers {
		steps = append(steps, analysis.DumpReconcileResult{
			ControllerID: controller,
			Changes: analysis.DumpChanges{
				ObjectVersions: []analysis.DumpObjectVersion{
					{
						Key:  compositeKey,
						Hash: versionHash,
					},
				},
				Effects: []tracecheck.Effect{
					{Key: compositeKey, Version: versionHash},
				},
			},
		})
	}

	runIndex := spec.TrialIndex
	dump := analysis.Dump{
		Context: &analysis.DumpContext{
			Scenario: &analysis.DumpScenarioContext{
				Name:     spec.ScenarioName,
				RunIndex: &runIndex,
				Attributes: map[string]string{
					"search_mode":    "monte_carlo",
					"mc_group_id":    spec.GroupID,
					"mc_role":        "trial",
					"mc_trial_index": strconv.Itoa(spec.TrialIndex),
					"mc_trial_count": strconv.Itoa(spec.TrialCount),
					"mc_seed":        strconv.Itoa(1000 + spec.TrialIndex),
				},
			},
		},
		Objects: []analysis.DumpObject{object},
		States: []analysis.DumpResultState{
			{
				ID: "state-0",
				State: analysis.DumpStateNode{
					Contents: analysis.DumpStateSnapshot{
						Objects: []analysis.DumpObjectVersion{
							{
								Key:  compositeKey,
								Hash: versionHash,
							},
						},
						KindSequences: tracecheck.KindSequences{
							compositeKey.CanonicalGroupKind(): 1,
						},
					},
				},
				Paths: [][]analysis.DumpReconcileResult{steps},
			},
		},
	}

	data, err := json.Marshal(dump)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o644))
}
