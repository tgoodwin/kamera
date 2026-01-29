package main

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tgoodwin/kamera/pkg/analyze"
)

func TestDependencyGraphDOT(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)

	graphPath := filepath.Join(wd, "..", "..", "pkg", "analyze", "testdata", "graph.json")
	dot, err := dependencyGraphDOT(graphPath)
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
		require.Equal(t, []string{"open", "/tmp/example.pdf"}, openArgs("/tmp/example.pdf"))
	case "linux":
		require.Equal(t, []string{"xdg-open", "/tmp/example.pdf"}, openArgs("/tmp/example.pdf"))
	case "windows":
		require.Equal(t, []string{"cmd", "/c", "start", "", "/tmp/example.pdf"}, openArgs("/tmp/example.pdf"))
	default:
		t.Skip("unsupported OS")
	}
}
