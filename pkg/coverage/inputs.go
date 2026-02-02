package coverage

import (
	"encoding/json"
	"fmt"
	"os"
)

// LoadInputs reads a scenarios inputs file from disk.
func LoadInputs(path string) ([]Input, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read inputs: %w", err)
	}

	var inputs []Input
	if err := json.Unmarshal(data, &inputs); err != nil {
		return nil, fmt.Errorf("parse inputs: %w", err)
	}
	if len(inputs) == 0 {
		return nil, fmt.Errorf("inputs file contains no scenarios")
	}
	return inputs, nil
}
