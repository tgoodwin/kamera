package coverage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadInputsOK(t *testing.T) {
	inputs := []Input{{Name: "case-1"}}
	path := writeInputsFile(t, inputs)

	got, err := LoadInputs(path)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "case-1", got[0].Name)
}

func TestLoadInputsBadJSON(t *testing.T) {
	path := writeRawFile(t, []byte("{not json"))
	_, err := LoadInputs(path)
	require.Error(t, err)
}

func TestLoadInputsEmpty(t *testing.T) {
	path := writeInputsFile(t, []Input{})
	_, err := LoadInputs(path)
	require.Error(t, err)
}

func writeInputsFile(t *testing.T, inputs []Input) string {
	t.Helper()
	data, err := json.Marshal(inputs)
	require.NoError(t, err)
	return writeRawFile(t, data)
}

func writeRawFile(t *testing.T, data []byte) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "inputs.json")
	require.NoError(t, os.WriteFile(path, data, 0644))
	return path
}
