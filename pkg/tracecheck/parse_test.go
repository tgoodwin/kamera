package tracecheck

import (
	"testing"

	"github.com/tgoodwin/sleeve/pkg/event"
)

func TestAssignResourceVersions(t *testing.T) {
	tests := []struct {
		name     string
		input    []StateEvent
		expected []int64
	}{
		{
			name: "single write operation",
			input: []StateEvent{
				{
					Timestamp: "2023-01-01T00:00:01Z",
					Effect:    effect{OpType: event.CREATE},
				},
			},
			expected: []int64{2},
		},
		{
			name: "multiple write operations",
			input: []StateEvent{
				{
					Timestamp: "2023-01-01T00:00:01Z",
					Effect:    effect{OpType: event.CREATE},
				},
				{
					Timestamp: "2023-01-01T00:00:02Z",
					Effect:    effect{OpType: event.UPDATE},
				},
				{
					Timestamp: "2023-01-01T00:00:03Z",
					Effect:    effect{OpType: event.DELETE},
				},
			},
			expected: []int64{2, 3, 4},
		},
		{
			name: "read operations do not increment sequence",
			input: []StateEvent{
				{
					Timestamp: "2023-01-01T00:00:01Z",
					Effect:    effect{OpType: event.GET},
				},
				{
					Timestamp: "2023-01-01T00:00:02Z",
					Effect:    effect{OpType: event.LIST},
				},
			},
			expected: []int64{1, 1},
		},
		{
			name: "mixed operations",
			input: []StateEvent{
				{
					Timestamp: "2023-01-01T00:00:01Z",
					Effect:    effect{OpType: event.CREATE},
				},
				{
					Timestamp: "2023-01-01T00:00:02Z",
					Effect:    effect{OpType: event.GET},
				},
				{
					Timestamp: "2023-01-01T00:00:03Z",
					Effect:    effect{OpType: event.UPDATE},
				},
			},
			expected: []int64{2, 2, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := assignResourceVersions(tt.input)
			if len(result) != len(tt.expected) {
				t.Fatalf("expected %d events, got %d", len(tt.expected), len(result))
			}
			for i, event := range result {
				if event.Sequence != tt.expected[i] {
					t.Errorf("at index %d, expected sequence %d, got %d", i, tt.expected[i], event.Sequence)
				}
			}
		})
	}
}
