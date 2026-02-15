package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExplorationArgs(t *testing.T) {
	code, err := runInspect([]string{"exploration"})
	require.NoError(t, err)
	require.NotEqual(t, 0, code)
}
