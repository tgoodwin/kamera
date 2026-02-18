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
	if len(inputs) < 2 {
		t.Fatalf("expected expanded default inputs array, got %d input(s)", len(inputs))
	}
	if inputs[0].Name != "knative-default/base" {
		t.Fatalf("expected first default scenario name, got %#v", inputs[0].Name)
	}
	if len(inputs[0].Objects) == 0 {
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

func TestLoadInputsFromSearchPathsUsesFirstExisting(t *testing.T) {
	tmpDir := t.TempDir()
	missing := filepath.Join(tmpDir, "missing.json")
	second := filepath.Join(tmpDir, "second.json")
	first := filepath.Join(tmpDir, "first.json")

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

	inputs, pathUsed, err := loadInputsFromSearchPaths([]string{missing, second, first}, coverage.LoadInputs)
	if err != nil {
		t.Fatalf("loadInputsFromSearchPaths error = %v", err)
	}
	if pathUsed != second {
		t.Fatalf("expected first existing path %q, got %q", second, pathUsed)
	}
	if len(inputs) != 1 || inputs[0].Name != "second" {
		t.Fatalf("unexpected loaded inputs: %#v", inputs)
	}
}

func TestLoadInputsFromSearchPathsAllMissing(t *testing.T) {
	tmpDir := t.TempDir()
	missingA := filepath.Join(tmpDir, "missing-a.json")
	missingB := filepath.Join(tmpDir, "missing-b.json")

	_, _, err := loadInputsFromSearchPaths([]string{missingA, missingB}, coverage.LoadInputs)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected os.ErrNotExist, got %v", err)
	}
}

func TestBatchInputsForRunParallelWithoutInputsMissingDefaults(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatalf("chdir temp: %v", err)
	}
	t.Cleanup(func() {
		if chdirErr := os.Chdir(wd); chdirErr != nil {
			t.Fatalf("restore cwd: %v", chdirErr)
		}
	})

	_, _, err = batchInputsForRun(true, "")
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
	}
}
