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
	inputs := []Input{{
		Name:             "case-1",
		EnvironmentState: EnvironmentState{Objects: []*unstructured.Unstructured{inputObject("v1", "ConfigMap")}},
	}}
	path := writeInputsFile(t, inputs)

	got, err := LoadInputs(path)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "case-1", got[0].Name)
}

func TestLoadInputsTopLevelArrayContract(t *testing.T) {
	inputs := []Input{{
		Name:             "case-1",
		EnvironmentState: EnvironmentState{Objects: []*unstructured.Unstructured{inputObject("v1", "ConfigMap")}},
	}}
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
		{Name: "case-1", EnvironmentState: EnvironmentState{Objects: []*unstructured.Unstructured{inputObject("v1", "ConfigMap")}}},
		{Name: "case-1", EnvironmentState: EnvironmentState{Objects: []*unstructured.Unstructured{inputObject("v1", "Secret")}}},
	})

	_, err := LoadInputs(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate scenario name")
}

func TestLoadInputsMissingScenarioName(t *testing.T) {
	path := writeInputsFile(t, []Input{
		{
			Name:             "   ",
			EnvironmentState: EnvironmentState{Objects: []*unstructured.Unstructured{inputObject("v1", "ConfigMap")}},
		},
	})

	_, err := LoadInputs(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "name must be set")
}

func TestLoadInputsMissingObjects(t *testing.T) {
	path := writeInputsFile(t, []Input{{Name: "case-1"}})

	_, err := LoadInputs(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must include either environmentState.objects or userInputs")
}

func TestLoadInputsInvalidUserInputsType(t *testing.T) {
	path := writeInputsFile(t, []Input{
		{
			Name:             "case-1",
			EnvironmentState: EnvironmentState{Objects: []*unstructured.Unstructured{inputObject("v1", "ConfigMap")}},
			ExternalInputs: []ExternalInput{{
				OpType: "NOPE",
				Object: inputObject("v1", "ConfigMap"),
			}},
		},
	})

	_, err := LoadInputs(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "userInputs[0].type must be CREATE, UPDATE, or DELETE")
}

func TestLoadInputsInvalidObjectGVK(t *testing.T) {
	path := writeInputsFile(t, []Input{
		{
			Name: "case-1",
			EnvironmentState: EnvironmentState{
				Objects: []*unstructured.Unstructured{
					{Object: map[string]any{"kind": "ConfigMap"}},
				},
			},
		},
	})

	_, err := LoadInputs(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "environmentState.objects[0]")
	assert.True(t, strings.Contains(err.Error(), "apiVersion") || strings.Contains(err.Error(), "kind"))
}

func TestLoadInputsAcceptsSearchTuningMonteCarlo(t *testing.T) {
	seed := int64(4242)
	trials := 7
	path := writeInputsFile(t, []Input{
		{
			Name:             "mc-case",
			EnvironmentState: EnvironmentState{Objects: []*unstructured.Unstructured{inputObject("v1", "ConfigMap")}},
			Tuning: InputTuning{
				Search: InputSearchTuning{
					Mode: "monte_carlo",
					MonteCarlo: InputMonteCarloTuning{
						Seed:   &seed,
						Trials: &trials,
					},
				},
			},
		},
	})

	got, err := LoadInputs(path)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NotNil(t, got[0].Tuning.Search.MonteCarlo.Seed)
	require.NotNil(t, got[0].Tuning.Search.MonteCarlo.Trials)
	assert.Equal(t, int64(4242), *got[0].Tuning.Search.MonteCarlo.Seed)
	assert.Equal(t, 7, *got[0].Tuning.Search.MonteCarlo.Trials)
}

func TestLoadInputsRejectsInvalidSearchMode(t *testing.T) {
	path := writeInputsFile(t, []Input{
		{
			Name:             "bad-search-mode",
			EnvironmentState: EnvironmentState{Objects: []*unstructured.Unstructured{inputObject("v1", "ConfigMap")}},
			Tuning: InputTuning{
				Search: InputSearchTuning{
					Mode: "random_walk",
				},
			},
		},
	})

	_, err := LoadInputs(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tuning.search.mode")
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
