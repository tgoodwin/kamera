package analysis

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// ResourceState represents a single materialized version of a resource. The
// payload is kept as raw JSON so later experiments can diff or patch it
// without pulling in Kubernetes types.
type ResourceState struct {
	// Name is a friendly label like "R0", "R1", etc.
	Name string
	// JSON is the serialized resource at this point in time.
	JSON json.RawMessage
	// Note explains what changed at this step.
	Note string
}

// FindVolatileFields returns, for each state, the JSON Pointer paths whose
// values differ from what survives in any later state. A path is considered
// volatile for state i if that state contains the path and a later state either
// removes it or assigns a different value. The analysis flattens each state
// to JSON Pointer leaves and compares backward, keeping the implementation
// lightweight and schema-agnostic.
func FindVolatileFields(timeline []ResourceState) (map[string][]string, error) {
	n := len(timeline)
	if n == 0 {
		return map[string][]string{}, nil
	}

	flat := make([]map[string]any, n)
	for i, state := range timeline {
		var obj any
		if err := json.Unmarshal(state.JSON, &obj); err != nil {
			return nil, fmt.Errorf("state %s JSON: %w", state.Name, err)
		}
		flat[i] = flatten(obj, "")
	}

	type obs struct {
		idx     int
		value   any
		present bool
	}
	last := map[string]obs{}
	volatile := map[int]map[string]bool{}
	for i := 0; i < n; i++ {
		curr := flat[i]
		seen := map[string]bool{}

		for path, val := range curr {
			seen[path] = true
			if prev, ok := last[path]; ok && prev.present {
				if equalPrimitive(val, prev.value) {
					last[path] = obs{idx: i, value: val, present: true}
					continue
				}
				if volatile[prev.idx] == nil {
					volatile[prev.idx] = map[string]bool{}
				}
				volatile[prev.idx][path] = true
			}
			last[path] = obs{idx: i, value: val, present: true}
		}

		for path, prev := range last {
			if prev.present && !seen[path] {
				if volatile[prev.idx] == nil {
					volatile[prev.idx] = map[string]bool{}
				}
				volatile[prev.idx][path] = true
				last[path] = obs{idx: i, present: false}
			}
		}
	}

	result := map[string][]string{}
	for stateIdx, paths := range volatile {
		stateName := timeline[stateIdx].Name
		for path := range paths {
			result[stateName] = append(result[stateName], path)
		}
	}

	for _, paths := range result {
		sort.Strings(paths)
	}
	return result, nil
}

func escapeJSONPointerToken(token string) string {
	token = strings.ReplaceAll(token, "~", "~0")
	token = strings.ReplaceAll(token, "/", "~1")
	return token
}

func flatten(v any, prefix string) map[string]any {
	out := map[string]any{}
	switch val := v.(type) {
	case map[string]any:
		for k, child := range val {
			childPath := prefix + "/" + escapeJSONPointerToken(k)
			for p, v := range flatten(child, childPath) {
				out[p] = v
			}
		}
	case []any:
		for i, child := range val {
			childPath := fmt.Sprintf("%s/%d", prefix, i)
			for p, v := range flatten(child, childPath) {
				out[p] = v
			}
		}
	default:
		out[prefix] = v
	}
	return out
}

func equalPrimitive(a, b any) bool {
	switch av := a.(type) {
	case float64:
		bv, ok := b.(float64)
		return ok && av == bv
	case string:
		bv, ok := b.(string)
		return ok && av == bv
	case bool:
		bv, ok := b.(bool)
		return ok && av == bv
	case nil:
		return b == nil
	default:
		panic("unexpected primitive type")
	}
}
