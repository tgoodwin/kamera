package util

import (
	"testing"
	"time"
)

func TestScrubTimes_ReplacesRFC3339Strings(t *testing.T) {
	input := map[string]interface{}{
		"status": map[string]interface{}{
			"conditions": []interface{}{
				map[string]interface{}{
					"lastTransitionTime": time.Now().Format(time.RFC3339),
					"message":            "ok",
				},
			},
			"nested": map[string]interface{}{
				"lastUpdateTime": "2020-01-02T03:04:05Z",
			},
		},
		"metadata": map[string]interface{}{
			"creationTimestamp": "not-a-time",
			"name":              "demo",
		},
	}

	ScrubTimes(input)

	cond := input["status"].(map[string]interface{})["conditions"].([]interface{})[0].(map[string]interface{})
	if got := cond["lastTransitionTime"]; got != scrubbedTimestamp {
		t.Fatalf("expected scrubbed timestamp, got %v", got)
	}
	nested := input["status"].(map[string]interface{})["nested"].(map[string]interface{})
	if got := nested["lastUpdateTime"]; got != scrubbedTimestamp {
		t.Fatalf("expected scrubbed timestamp, got %v", got)
	}
	meta := input["metadata"].(map[string]interface{})
	if got := meta["creationTimestamp"]; got != "not-a-time" {
		t.Fatalf("non-time string should remain unchanged, got %v", got)
	}
}

func TestScrubTimes_SliceOfTimestamps(t *testing.T) {
	input := []interface{}{"2020-01-01T00:00:00Z", "foo", "2021-02-03T04:05:06Z"}
	ScrubTimes(input)
	if input[0] != scrubbedTimestamp || input[2] != scrubbedTimestamp {
		t.Fatalf("expected timestamps scrubbed: %v", input)
	}
	if input[1] != "foo" {
		t.Fatalf("non-time string should remain unchanged: %v", input[1])
	}
}
