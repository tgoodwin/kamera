package coverage

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
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
	if err := validateInputs(inputs); err != nil {
		return nil, err
	}
	return inputs, nil
}

func validateInputs(inputs []Input) error {
	if len(inputs) == 0 {
		return fmt.Errorf("inputs file contains no scenarios")
	}

	seenNames := map[string]struct{}{}
	for i, input := range inputs {
		name := strings.TrimSpace(input.Name)
		if name == "" {
			return fmt.Errorf("input[%d].name must be set", i)
		}
		if _, exists := seenNames[name]; exists {
			return fmt.Errorf("duplicate scenario name %q at input[%d]", name, i)
		}
		seenNames[name] = struct{}{}

		if len(input.Objects) == 0 {
			return fmt.Errorf("input[%d] (%s) must include at least one object", i, name)
		}
		for objIdx, obj := range input.Objects {
			if obj == nil {
				return fmt.Errorf("input[%d] (%s) object[%d] is nil", i, name, objIdx)
			}
			if strings.TrimSpace(obj.GetAPIVersion()) == "" || strings.TrimSpace(obj.GetKind()) == "" {
				return fmt.Errorf("input[%d] (%s) object[%d] must set apiVersion and kind", i, name, objIdx)
			}
		}

		for pendingIdx, pending := range input.Pending {
			if strings.TrimSpace(pending.ControllerID) == "" {
				return fmt.Errorf("input[%d] (%s) pending[%d].controllerId must be set", i, name, pendingIdx)
			}
			if strings.TrimSpace(pending.Key.Name) == "" {
				return fmt.Errorf("input[%d] (%s) pending[%d].key.name must be set", i, name, pendingIdx)
			}
		}
	}
	return nil
}
