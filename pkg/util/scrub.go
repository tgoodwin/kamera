package util

import (
	"strings"
	"time"
)

const scrubbedTimestamp = "1970-01-01T00:00:00Z"

// ScrubTimes walks an arbitrary JSON-like structure (maps/slices) and replaces
// any string that parses as RFC3339 with a deterministic placeholder.
// This is intended to normalize timestamp-bearing fields for deterministic hashing.
func ScrubTimes(v interface{}) {
	scrubTimes(v)
}

func scrubTimes(v interface{}) {
	switch x := v.(type) {
	case map[string]interface{}:
		for k, v2 := range x {
			scrubTimes(v2)
			if s, ok := x[k].(string); ok && likelyTimestamp(s) {
				if _, err := time.Parse(time.RFC3339, s); err == nil {
					x[k] = scrubbedTimestamp
				}
			}
		}
	case []interface{}:
		for i := range x {
			scrubTimes(x[i])
			if s, ok := x[i].(string); ok && likelyTimestamp(s) {
				if _, err := time.Parse(time.RFC3339, s); err == nil {
					x[i] = scrubbedTimestamp
				}
			}
		}
	}
}

func likelyTimestamp(s string) bool {
	// Cheap prefilter to avoid parsing arbitrary strings.
	return strings.Contains(s, "T") && strings.Contains(s, "-")
}
