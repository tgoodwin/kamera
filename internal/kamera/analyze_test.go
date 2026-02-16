package kamera

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunAnalyzeHelp(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := RunAnalyze([]string{"--help"}, &stdout, &stderr)
	require.Equal(t, 0, code)
	require.Contains(t, stdout.String(), "Usage: kamera analyze")
}
