package coverage

import (
	"encoding/json"
	"fmt"
	"os"
)

// LoadInputMap reads and validates an input-map.json file.
// Each GVK must map to exactly one template object.
func LoadInputMap(path string) (InputMap, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return InputMap{}, fmt.Errorf("read input map: %w", err)
	}

	var inputMap InputMap
	if err := json.Unmarshal(data, &inputMap); err != nil {
		return InputMap{}, fmt.Errorf("parse input map: %w", err)
	}
	if len(inputMap.Mapping) == 0 {
		return InputMap{}, fmt.Errorf("input map has no mapping entries")
	}

	for gvk, templates := range inputMap.Mapping {
		if len(templates) != 1 {
			return InputMap{}, fmt.Errorf("input map for %s must contain exactly one template", gvk)
		}
		if templates[0].Object == nil {
			return InputMap{}, fmt.Errorf("input map for %s has nil template object", gvk)
		}
	}

	return inputMap, nil
}
