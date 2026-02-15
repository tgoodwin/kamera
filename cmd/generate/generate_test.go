package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tgoodwin/kamera/pkg/analyze"
	"github.com/tgoodwin/kamera/pkg/coverage"
)

func TestGenerateOutputsInputs(t *testing.T) {
	graphPath := writeTempGraph(t)
	inputMapPath := writeTempInputMap(t)
	outPath := filepath.Join(t.TempDir(), "inputs.json")

	code, err := runGenerate([]string{
		"--graph", graphPath,
		"--input-map", inputMapPath,
		"--out", outPath,
	})
	require.NoError(t, err)
	require.Equal(t, 0, code)

	data, err := os.ReadFile(outPath)
	require.NoError(t, err)

	var inputs []coverage.Input
	require.NoError(t, json.Unmarshal(data, &inputs))
	require.NotEmpty(t, inputs)
}

func writeTempGraph(t *testing.T) string {
	t.Helper()
	raw := analyze.RawGraph{
		Nodes: []analyze.RawNode{
			{Kind: "controller", Name: "WriterA"},
			{Kind: "controller", Name: "WriterB"},
			{Kind: "resource", GVK: "core/v1/Service"},
		},
		Edges: []analyze.RawEdge{
			{Kind: "reconciles", From: "WriterA", To: "core/v1/Service"},
			{Kind: "reconciles", From: "WriterB", To: "core/v1/Service"},
			{Kind: "writes", From: "WriterA", To: "core/v1/Service", Target: "status"},
			{Kind: "writes", From: "WriterB", To: "core/v1/Service", Target: "status"},
		},
	}

	data, err := json.Marshal(raw)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "graph.json")
	require.NoError(t, os.WriteFile(path, data, 0644))
	return path
}

func writeTempInputMap(t *testing.T) string {
	t.Helper()
	payload := map[string]any{
		"mapping": map[string]any{
			"core/v1/Service": []any{
				map[string]any{
					"name": "svc",
					"object": map[string]any{
						"apiVersion": "v1",
						"kind":       "Service",
						"metadata": map[string]any{
							"name":      "template",
							"namespace": "default",
						},
					},
				},
			},
		},
	}
	data, err := json.Marshal(payload)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "input-map.json")
	require.NoError(t, os.WriteFile(path, data, 0644))
	return path
}
