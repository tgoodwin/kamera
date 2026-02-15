package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInspectHelp(t *testing.T) {
	code, err := runInspect([]string{"--help"})
	require.NoError(t, err)
	require.Equal(t, 0, code)
}

func TestDependencyGraphHelp(t *testing.T) {
	code, err := runInspect([]string{"dependency-graph", "--help"})
	require.NoError(t, err)
	require.Equal(t, 0, code)
}

func TestExplorationHelp(t *testing.T) {
	code, err := runInspect([]string{"exploration", "--help"})
	require.NoError(t, err)
	require.Equal(t, 0, code)
}

func TestHotspotsHelp(t *testing.T) {
	code, err := runInspect([]string{"hotspots", "--help"})
	require.NoError(t, err)
	require.Equal(t, 0, code)
}
