package interactive

import (
	"strings"

	"github.com/tgoodwin/kamera/pkg/tracecheck"
)

// dedupeResultStates collapses states with identical hashes, merging their execution paths and reasons.
func dedupeResultStates(states []tracecheck.ResultState) []tracecheck.ResultState {
	if len(states) <= 1 {
		return states
	}

	unique := make([]tracecheck.ResultState, 0, len(states))
	indexByHash := make(map[tracecheck.StateHash]int, len(states))

	for _, state := range states {
		hash := state.State.Hash()

		if idx, exists := indexByHash[hash]; exists {
			mergedPaths := append(unique[idx].Paths, state.Paths...)
			unique[idx].Paths = tracecheck.GetUniquePaths(mergedPaths)
			unique[idx].Reason = mergeReasons(unique[idx].Reason, state.Reason)
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

func mergeReasons(existing, incoming string) string {
	incoming = strings.TrimSpace(incoming)
	if incoming == "" {
		return existing
	}
	if existing == "" {
		return incoming
	}

	for _, reason := range strings.Split(existing, ",") {
		if strings.EqualFold(strings.TrimSpace(reason), incoming) {
			return existing
		}
	}

	return existing + ", " + incoming
}
