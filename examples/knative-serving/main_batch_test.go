package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/tgoodwin/kamera/pkg/coverage"
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

	want := []coverage.Input{
		{
			Name:    "from-file",
			Objects: nil,
			Pending: nil,
		},
	}
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
