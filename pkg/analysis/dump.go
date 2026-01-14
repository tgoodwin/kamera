package analysis

import (
	"encoding/json"
	"fmt"
	"os"
)

// LoadDump reads and unmarshals a kamera dump file from the specified path.
// It returns the raw dump structure without converting to tracecheck types.
// For conversion to tracecheck types, use pkg/interactive.LoadInspectorDump.
func LoadDump(path string) (*Dump, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read dump file: %w", err)
	}

	var dump Dump
	if err := json.Unmarshal(data, &dump); err != nil {
		return nil, fmt.Errorf("unmarshal dump: %w", err)
	}

	return &dump, nil
}
