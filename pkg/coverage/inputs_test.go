package coverage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestLoadInputsOK(t *testing.T) {
	inputs := []Input{{Name: "case-1", Objects: []*unstructured.Unstructured{inputObject("v1", "ConfigMap")}}}
	path := writeInputsFile(t, inputs)

	got, err := LoadInputs(path)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "case-1", got[0].Name)
}

func TestLoadInputsTopLevelArrayContract(t *testing.T) {
	inputs := []Input{{Name: "case-1", Objects: []*unstructured.Unstructured{inputObject("v1", "ConfigMap")}}}
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

func TestLoadInputsDuplicateScenarioName(t *testing.T) {
	path := writeInputsFile(t, []Input{
		{Name: "case-1", Objects: []*unstructured.Unstructured{inputObject("v1", "ConfigMap")}},
		{Name: "case-1", Objects: []*unstructured.Unstructured{inputObject("v1", "Secret")}},
	})

	_, err := LoadInputs(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate scenario name")
}

func TestLoadInputsMissingScenarioName(t *testing.T) {
	path := writeInputsFile(t, []Input{
		{Name: "   ", Objects: []*unstructured.Unstructured{inputObject("v1", "ConfigMap")}},
	})

	_, err := LoadInputs(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "name must be set")
}

func TestLoadInputsMissingObjects(t *testing.T) {
	path := writeInputsFile(t, []Input{
		{Name: "case-1"},
	})

	_, err := LoadInputs(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must include at least one object")
}

func TestLoadInputsInvalidPendingKey(t *testing.T) {
	path := writeInputsFile(t, []Input{
		{
			Name:    "case-1",
			Objects: []*unstructured.Unstructured{inputObject("v1", "ConfigMap")},
			Pending: []Pending{
				{
					ControllerID: "controller-a",
					Key: NamespacedName{
						Namespace: "default",
						Name:      "   ",
					},
				},
			},
		},
	})

	_, err := LoadInputs(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pending[0].key.name must be set")
}

func TestLoadInputsInvalidObjectGVK(t *testing.T) {
	path := writeInputsFile(t, []Input{
		{
			Name: "case-1",
			Objects: []*unstructured.Unstructured{
				{Object: map[string]any{"kind": "ConfigMap"}},
			},
		},
	})

	_, err := LoadInputs(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "object[0]")
	assert.True(t, strings.Contains(err.Error(), "apiVersion") || strings.Contains(err.Error(), "kind"))
}

func TestLoadInputsInvalidPendingControllerID(t *testing.T) {
	path := writeInputsFile(t, []Input{
		{
			Name:    "case-1",
			Objects: []*unstructured.Unstructured{inputObject("v1", "ConfigMap")},
			Pending: []Pending{
				{
					ControllerID: "   ",
					Key: NamespacedName{
						Namespace: "default",
						Name:      "obj",
					},
				},
			},
		},
	})

	_, err := LoadInputs(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pending[0].controllerId must be set")
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

func inputObject(apiVersion, kind string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": apiVersion,
			"kind":       kind,
			"metadata": map[string]any{
				"name": "obj",
			},
		},
	}
}
