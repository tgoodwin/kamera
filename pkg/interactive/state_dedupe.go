package interactive

import (
	"errors"
	"strings"

	"github.com/tgoodwin/kamera/pkg/tracecheck"
)

// dedupeResultStates collapses states with identical hashes, merging their execution paths and errors.
func dedupeResultStates(states []tracecheck.ResultState) []tracecheck.ResultState {
	if len(states) <= 1 {
		return states
	}

	unique := make([]tracecheck.ResultState, 0, len(states))
	indexByHash := make(map[tracecheck.NodeHash]int, len(states))

	for _, state := range states {
		hash := state.State.ConvergenceHash()

		if idx, exists := indexByHash[hash]; exists {
			mergedPaths := append(unique[idx].Paths, state.Paths...)
			unique[idx].Paths = tracecheck.GetUniquePaths(mergedPaths)

			existingErr := ""
			if unique[idx].Error != nil {
				existingErr = unique[idx].Error.Error()
			}
			incomingErr := ""
			if state.Error != nil {
				incomingErr = state.Error.Error()
			}
			mergedErr := mergeErrors(existingErr, incomingErr)
			if mergedErr != "" {
				unique[idx].Error = errors.New(mergedErr)
			}
			continue
		}

		pathsCopy := make([]tracecheck.ExecutionHistory, len(state.Paths))
		copy(pathsCopy, state.Paths)
		state.Paths = tracecheck.GetUniquePaths(pathsCopy)
		unique = append(unique, state)
		indexByHash[hash] = len(unique) - 1
	}

	return unique
}

func mergeErrors(existing, incoming string) string {
	incoming = strings.TrimSpace(incoming)
	if incoming == "" {
		return existing
	}
	if existing == "" {
		return incoming
	}

	for _, errLine := range strings.Split(existing, "\n") {
		if strings.TrimSpace(errLine) == incoming {
			return existing
		}
	}

	return existing + "\n" + incoming
}
