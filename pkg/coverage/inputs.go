package coverage

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/tgoodwin/kamera/pkg/event"
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

		if len(input.EnvironmentState.Objects) == 0 && len(input.ExternalInputs) == 0 {
			return fmt.Errorf("input[%d] (%s) must include either environmentState.objects or userInputs", i, name)
		}

		for objIdx, obj := range input.EnvironmentState.Objects {
			if obj == nil {
				return fmt.Errorf("input[%d] (%s) environmentState.objects[%d] is nil", i, name, objIdx)
			}
			if strings.TrimSpace(obj.GetAPIVersion()) == "" || strings.TrimSpace(obj.GetKind()) == "" {
				return fmt.Errorf("input[%d] (%s) environmentState.objects[%d] must set apiVersion and kind", i, name, objIdx)
			}
		}

		for actionIdx, action := range input.ExternalInputs {
			if action.Object == nil {
				return fmt.Errorf("input[%d] (%s) userInputs[%d].object must be set", i, name, actionIdx)
			}
			if strings.TrimSpace(string(action.OpType)) == "" {
				return fmt.Errorf("input[%d] (%s) userInputs[%d].type must be set", i, name, actionIdx)
			}
			if !isValidUserActionType(action.OpType) {
				return fmt.Errorf("input[%d] (%s) userInputs[%d].type must be CREATE, UPDATE, or DELETE", i, name, actionIdx)
			}
			if strings.TrimSpace(action.Object.GetAPIVersion()) == "" || strings.TrimSpace(action.Object.GetKind()) == "" {
				return fmt.Errorf("input[%d] (%s) userInputs[%d].object must set apiVersion and kind", i, name, actionIdx)
			}
		}

		if err := validateSearchTuning(i, name, input.Tuning.Search); err != nil {
			return err
		}
	}
	return nil
}

func validateSearchTuning(inputIdx int, inputName string, search InputSearchTuning) error {
	mode := strings.TrimSpace(search.Mode)
	switch mode {
	case "", "dfs", "monte_carlo":
	default:
		return fmt.Errorf("input[%d] (%s) tuning.search.mode must be one of: dfs, monte_carlo", inputIdx, inputName)
	}
	if search.MonteCarlo.Trials != nil && *search.MonteCarlo.Trials <= 0 {
		return fmt.Errorf("input[%d] (%s) tuning.search.monteCarlo.trials must be >= 1", inputIdx, inputName)
	}
	if search.MonteCarlo.TrialIndex != nil && *search.MonteCarlo.TrialIndex < 0 {
		return fmt.Errorf("input[%d] (%s) tuning.search.monteCarlo.trialIndex must be >= 0", inputIdx, inputName)
	}
	return nil
}

func isValidUserActionType(op event.OperationType) bool {
	switch op {
	case event.CREATE, event.UPDATE, event.MARK_FOR_DELETION:
		return true
	default:
		return false
	}
}
