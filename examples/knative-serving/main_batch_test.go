package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/tgoodwin/kamera/pkg/coverage"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestBatchInputsForRunParallelWithoutInputsUsesDefaults(t *testing.T) {
	tmpDir := t.TempDir()
	defaultPath := filepath.Join(tmpDir, "inputs.json")
	data, err := json.Marshal(validInputs("knative-default/base"))
	if err != nil {
		t.Fatalf("marshal default inputs: %v", err)
	}
	if err := os.WriteFile(defaultPath, data, 0o644); err != nil {
		t.Fatalf("write default inputs file: %v", err)
	}
	withDefaultKnativeInputSearchPaths(t, []string{defaultPath})

	inputs, batchMode, err := batchInputsForRun(true, "")
	if err != nil {
		t.Fatalf("batchInputsForRun error = %v", err)
	}
	if !batchMode {
		t.Fatal("expected batch mode")
	}
	if len(inputs) == 0 {
		t.Fatal("expected default inputs")
	}
	if inputs[0].Name != "knative-default/base" {
		t.Fatalf("expected first default scenario name, got %#v", inputs[0].Name)
	}
	if len(inputs[0].EnvironmentState.Objects) == 0 {
		t.Fatalf("expected default input objects, got %#v", inputs[0])
	}
}

func TestBatchInputsForRunNoParallelNoInputsIsNotBatch(t *testing.T) {
	inputs, batchMode, err := batchInputsForRun(false, "")
	if err != nil {
		t.Fatalf("batchInputsForRun error = %v", err)
	}
	if batchMode {
		t.Fatal("expected non-batch mode")
	}
	if inputs != nil {
		t.Fatalf("expected nil inputs, got %#v", inputs)
	}
}

func TestBatchInputsForRunUsesInputsFile(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "inputs.json")

	want := validInputs("from-file")
	data, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal inputs: %v", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write inputs file: %v", err)
	}

	inputs, batchMode, err := batchInputsForRun(false, path)
	if err != nil {
		t.Fatalf("batchInputsForRun error = %v", err)
	}
	if !batchMode {
		t.Fatal("expected batch mode")
	}
	if len(inputs) != 1 || inputs[0].Name != "from-file" {
		t.Fatalf("unexpected loaded inputs: %#v", inputs)
	}
}

func TestLoadDefaultKnativeInputsUsesFirstExisting(t *testing.T) {
	tmpDir := t.TempDir()
	missing := filepath.Join(tmpDir, "missing.json")
	second := filepath.Join(tmpDir, "second.json")
	first := filepath.Join(tmpDir, "first.json")
	withDefaultKnativeInputSearchPaths(t, []string{missing, second, first})

	secondData, err := json.Marshal(validInputs("second"))
	if err != nil {
		t.Fatalf("marshal second inputs: %v", err)
	}
	if err := os.WriteFile(second, secondData, 0o644); err != nil {
		t.Fatalf("write second inputs file: %v", err)
	}

	firstData, err := json.Marshal(validInputs("first"))
	if err != nil {
		t.Fatalf("marshal first inputs: %v", err)
	}
	if err := os.WriteFile(first, firstData, 0o644); err != nil {
		t.Fatalf("write first inputs file: %v", err)
	}

	inputs, err := loadDefaultKnativeInputs()
	if err != nil {
		t.Fatalf("loadDefaultKnativeInputs error = %v", err)
	}
	if len(inputs) != 1 || inputs[0].Name != "second" {
		t.Fatalf("unexpected loaded inputs: %#v", inputs)
	}
}

func TestLoadDefaultKnativeInputsAllMissing(t *testing.T) {
	tmpDir := t.TempDir()
	missingA := filepath.Join(tmpDir, "missing-a.json")
	missingB := filepath.Join(tmpDir, "missing-b.json")
	withDefaultKnativeInputSearchPaths(t, []string{missingA, missingB})

	_, err := loadDefaultKnativeInputs()
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected os.ErrNotExist, got %v", err)
	}
}

func TestBatchInputsForRunParallelWithoutInputsMissingDefaults(t *testing.T) {
	tmpDir := t.TempDir()
	withDefaultKnativeInputSearchPaths(t, []string{
		filepath.Join(tmpDir, "missing-a.json"),
		filepath.Join(tmpDir, "missing-b.json"),
	})

	_, _, err := batchInputsForRun(true, "")
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected os.ErrNotExist, got %v", err)
	}
}

func validInputs(name string) []coverage.Input {
	return []coverage.Input{
		{
			Name: name,
			EnvironmentState: coverage.EnvironmentState{
				Objects: []*unstructured.Unstructured{
					{
						Object: map[string]any{
							"apiVersion": "v1",
							"kind":       "ConfigMap",
							"metadata": map[string]any{
								"name": "sample",
							},
						},
					},
				},
			},
		},
	}
}

func withDefaultKnativeInputSearchPaths(t *testing.T, paths []string) {
	t.Helper()
	origPaths := defaultKnativeInputsSearchPaths
	defaultKnativeInputsSearchPaths = paths
	t.Cleanup(func() {
		defaultKnativeInputsSearchPaths = origPaths
	})
}
