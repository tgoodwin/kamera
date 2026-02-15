package kamera

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunRootHelp(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run([]string{"--help"}, &stdout, &stderr)
	require.Equal(t, 0, code)
	require.Contains(t, stdout.String(), "kamera")
}

func TestRootRunInspectHelp(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run([]string{"inspect", "--help"}, &stdout, &stderr)
	require.Equal(t, 0, code)
}

func TestRunUnknownCommand(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run([]string{"not-a-command"}, &stdout, &stderr)
	require.Equal(t, 1, code)
	require.Contains(t, stderr.String(), "unknown command")
}
