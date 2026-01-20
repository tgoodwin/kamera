package analysis

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDump_FileNotFound(t *testing.T) {
	_, err := LoadDump("/nonexistent/path/dump.json")
	if err == nil {
		t.Fatal("expected error for non-existent file")
	}
}

func TestLoadDump_InvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "invalid.json")
	if err := os.WriteFile(path, []byte("not valid json"), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	_, err := LoadDump(path)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestLoadDump_EmptyDump(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "empty.json")
	if err := os.WriteFile(path, []byte(`{"objects":[],"states":[]}`), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	dump, err := LoadDump(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dump == nil {
		t.Fatal("expected non-nil dump")
	}
	if len(dump.Objects) != 0 {
		t.Errorf("expected 0 objects, got %d", len(dump.Objects))
	}
	if len(dump.States) != 0 {
		t.Errorf("expected 0 states, got %d", len(dump.States))
	}
}

func TestLoadDump_ValidDump(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "valid.json")
	content := `{
		"objects": [
			{
				"hash": {"value": "abc123", "strategy": "canonical"},
				"object": {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "test"}}
			}
		],
		"states": [
			{
				"id": "state-1",
				"divergencePoint": "",
				"state": {
					"contents": {
						"objects": [],
						"kindSequences": {}
					}
				},
				"paths": []
			}
		]
	}`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	dump, err := LoadDump(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dump == nil {
		t.Fatal("expected non-nil dump")
	}
	if len(dump.Objects) != 1 {
		t.Errorf("expected 1 object, got %d", len(dump.Objects))
	}
	if dump.Objects[0].Hash.Value != "abc123" {
		t.Errorf("expected hash value 'abc123', got '%s'", dump.Objects[0].Hash.Value)
	}
	if len(dump.States) != 1 {
		t.Errorf("expected 1 state, got %d", len(dump.States))
	}
	if dump.States[0].ID != "state-1" {
		t.Errorf("expected state ID 'state-1', got '%s'", dump.States[0].ID)
	}
}
