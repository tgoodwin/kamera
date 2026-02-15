package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tgoodwin/kamera/pkg/analyze"
)

func TestInspectHotspotsJSON(t *testing.T) {
	graphPath := writeTempGraph(t)
	output := captureStdout(t, func() {
		code, err := runInspect([]string{"hotspots", graphPath})
		require.NoError(t, err)
		require.Equal(t, 0, code)
	})

	var payload []map[string]any
	err := json.Unmarshal([]byte(output), &payload)
	require.NoError(t, err)
	require.NotEmpty(t, payload)

	first := payload[0]
	controllers := readStringSlice(t, first["controllers"])
	resources := readStringSlice(t, first["resources"])
	for _, controller := range controllers {
		require.False(t, strings.HasPrefix(controller, "c:"))
	}
	for _, resource := range resources {
		require.False(t, strings.HasPrefix(resource, "r:"))
	}
}

func captureStdout(t *testing.T, fn func()) string {
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

func writeTempGraph(t *testing.T) string {
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

func readStringSlice(t *testing.T, value any) []string {
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
