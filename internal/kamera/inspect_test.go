package kamera

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tgoodwin/kamera/pkg/analysis"
	"github.com/tgoodwin/kamera/pkg/analyze"
	"github.com/tgoodwin/kamera/pkg/interactive"
)

func TestRunInspectHelp(t *testing.T) {
	code, err := RunInspect([]string{"--help"})
	require.NoError(t, err)
	require.Equal(t, 0, code)
}

func TestRunInspectDependencyGraphHelp(t *testing.T) {
	code, err := RunInspect([]string{"dependency-graph", "--help"})
	require.NoError(t, err)
	require.Equal(t, 0, code)
}

func TestRunInspectExplorationHelp(t *testing.T) {
	code, err := RunInspect([]string{"exploration", "--help"})
	require.NoError(t, err)
	require.Equal(t, 0, code)
}

func TestRunInspectHotspotsHelp(t *testing.T) {
	code, err := RunInspect([]string{"hotspots", "--help"})
	require.NoError(t, err)
	require.Equal(t, 0, code)
}

func TestRunInspectExplorationArgs(t *testing.T) {
	code, err := RunInspect([]string{"exploration"})
	require.NoError(t, err)
	require.NotEqual(t, 0, code)
}

func TestRunInspectExplorationDirectoryHeadlessListsDumpSummaries(t *testing.T) {
	dir := t.TempDir()
	writeInspectTempDump(t, dir, "workflow_alpha_0.jsonl", "KPA")
	writeInspectTempDump(t, dir, "workflow_beta_1.jsonl", "RevisionController")

	output := inspectCaptureStdout(t, func() {
		code, err := RunInspect([]string{"exploration", "--interactive=false", dir})
		require.NoError(t, err)
		require.Equal(t, 0, code)
	})

	require.Contains(t, output, "workflow_alpha_0.jsonl")
	require.Contains(t, output, "workflow_beta_1.jsonl")
	require.Contains(t, output, "KPA")
	require.Contains(t, output, "RevisionController")
}

func TestRunInspectExplorationDirectoryHeadlessRequiresSingleDumpForDot(t *testing.T) {
	dir := t.TempDir()
	writeInspectTempDump(t, dir, "workflow_alpha_0.jsonl", "KPA")

	code, err := RunInspect([]string{"exploration", "--interactive=false", "--dot", filepath.Join(dir, "graph.dot"), dir})
	require.NoError(t, err)
	require.NotEqual(t, 0, code)
}

func TestRunInspectExplorationDirectoryInteractiveUsesIntegratedUI(t *testing.T) {
	dir := t.TempDir()
	writeInspectTempDump(t, dir, "workflow_alpha_0.jsonl", "KPA")

	oldDirectoryUI := runDirectoryInspectorUI
	t.Cleanup(func() {
		runDirectoryInspectorUI = oldDirectoryUI
	})

	uiCalls := 0
	runDirectoryInspectorUI = func(entries []interactive.DumpCatalogEntry) error {
		uiCalls++
		require.Len(t, entries, 1)
		require.Equal(t, "workflow_alpha_0.jsonl", entries[0].File)
		return nil
	}

	code, err := RunInspect([]string{"exploration", dir})
	require.NoError(t, err)
	require.Equal(t, 0, code)
	require.Equal(t, 1, uiCalls)
}

func TestDependencyGraphDOT(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)

	graphPath := filepath.Join(wd, "..", "..", "pkg", "analyze", "testdata", "graph.json")
	dot, err := DependencyGraphDOT(graphPath)
	require.NoError(t, err)

	data, err := os.ReadFile(graphPath)
	require.NoError(t, err)

	raw, err := analyze.ParseRawGraphJSON(data)
	require.NoError(t, err)

	graph, err := analyze.BuildGraphFromRaw(raw)
	require.NoError(t, err)

	expected := analyze.RenderDependencyGraphDOT(graph)
	require.Equal(t, expected, dot)
}

func TestOpenArgsUsesDefaultViewer(t *testing.T) {
	switch runtime.GOOS {
	case "darwin":
		require.Equal(t, []string{"open", "/tmp/example.pdf"}, OpenArgs("/tmp/example.pdf"))
	case "linux":
		require.Equal(t, []string{"xdg-open", "/tmp/example.pdf"}, OpenArgs("/tmp/example.pdf"))
	case "windows":
		require.Equal(t, []string{"cmd", "/c", "start", "", "/tmp/example.pdf"}, OpenArgs("/tmp/example.pdf"))
	default:
		t.Skip("unsupported OS")
	}
}

func TestRunInspectHotspotsJSON(t *testing.T) {
	graphPath := writeInspectTempGraph(t)
	output := inspectCaptureStdout(t, func() {
		code, err := RunInspect([]string{"hotspots", graphPath})
		require.NoError(t, err)
		require.Equal(t, 0, code)
	})

	var payload []map[string]any
	err := json.Unmarshal([]byte(output), &payload)
	require.NoError(t, err)
	require.NotEmpty(t, payload)

	first := payload[0]
	controllers := inspectReadStringSlice(t, first["controllers"])
	resources := inspectReadStringSlice(t, first["resources"])
	for _, controller := range controllers {
		require.False(t, strings.HasPrefix(controller, "c:"))
	}
	for _, resource := range resources {
		require.False(t, strings.HasPrefix(resource, "r:"))
	}
}

func inspectCaptureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	fn()
	_ = w.Close()
	os.Stdout = old
	var buf bytes.Buffer
	_, err = io.Copy(&buf, r)
	require.NoError(t, err)
	_ = r.Close()
	return buf.String()
}

func writeInspectTempGraph(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "graph.json")

	raw := analyze.RawGraph{
		Nodes: []analyze.RawNode{
			{Kind: "controller", Name: "ExampleReconciler"},
			{Kind: "resource", GVK: "core/v1/Service"},
		},
		Edges: []analyze.RawEdge{
			{Kind: "reads", From: "ExampleReconciler", To: "core/v1/Service", Target: "spec"},
		},
	}

	data, err := json.Marshal(raw)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0644))
	return path
}

func inspectReadStringSlice(t *testing.T, value any) []string {
	t.Helper()
	items, ok := value.([]any)
	require.True(t, ok)
	out := make([]string, 0, len(items))
	for _, item := range items {
		str, ok := item.(string)
		require.True(t, ok)
		out = append(out, str)
	}
	return out
}

func writeInspectTempDump(t *testing.T, dir, filename, controller string) string {
	t.Helper()
	path := filepath.Join(dir, filename)
	dump := analysis.Dump{
		States: []analysis.DumpResultState{
			{
				ID: "state-0",
				State: analysis.DumpStateNode{
					Contents: analysis.DumpStateSnapshot{},
				},
				Paths: [][]analysis.DumpReconcileResult{
					{
						{
							ControllerID: controller,
						},
					},
				},
			},
		},
	}
	data, err := json.Marshal(dump)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o644))
	return path
}
