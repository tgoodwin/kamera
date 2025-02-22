package tracecheck

import (
	"fmt"
	"testing"

	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
)

type MockVersionResolver struct{}

func (m *MockVersionResolver) Resolve(key event.CausalKey) (string, error) {
	// Return some deterministic object state based on the CausalKey
	return fmt.Sprintf("state-%s-%s", key.ObjectID, string(key.ChangeID)), nil
}

func TestGlobalKnowledgeLoad(t *testing.T) {
	t.Run("basic event sequence", func(t *testing.T) {
		testEvents := []event.Event{
			// Controller reads the declared parent object
			{
				ID:           "evt-2",
				Timestamp:    "2024-02-21T10:00:01Z",
				ReconcileID:  "r2",
				ControllerID: "ctrl-1",
				OpType:       "GET",
				Kind:         "Parent",
				ObjectID:     "parent-1",
				Labels: map[string]string{
					"tracey-uid": "root-1",
				},
			},

			// Controller creates child object
			{
				ID:           "evt-3",
				Timestamp:    "2024-02-21T10:00:02Z",
				ReconcileID:  "r2",
				ControllerID: "ctrl-1",
				OpType:       "CREATE",
				Kind:         "Child",
				ObjectID:     "child-1",
				Labels: map[string]string{
					"discrete.events/root-event-id": "root-1",
					"discrete.events/change-id":     "change-1",
				},
			},

			// Controller updates child
			{
				ID:           "evt-4",
				Timestamp:    "2024-02-21T10:00:03Z",
				ReconcileID:  "r3",
				ControllerID: "ctrl-1",
				OpType:       "UPDATE",
				Kind:         "Child",
				ObjectID:     "child-1",
				Labels: map[string]string{
					"discrete.events/root-event-id": "root-1",
					"discrete.events/change-id":     "change-2",
				},
			},

			// Controller deletes child
			{
				ID:           "evt-5",
				Timestamp:    "2024-02-21T10:00:04Z",
				ReconcileID:  "r4",
				ControllerID: "ctrl-1",
				OpType:       "DELETE",
				Kind:         "Child",
				ObjectID:     "child-1",
				Labels: map[string]string{
					"discrete.events/root-event-id": "root-1",
					"discrete.events/deletion-id":   "del-1",
				},
			},
		}

		// Create a mock version resolver for testing
		mockResolver := &MockVersionResolver{}
		g := NewGlobalKnowledge(mockResolver)
		err := g.Load(testEvents)
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		// Test that sequences were assigned correctly
		if len(g.Kinds["Parent"].EventLog) != 1 {
			t.Errorf("Expected 1 Parent events, got %d", len(g.Kinds["Parent"].EventLog))
		}
		if len(g.Kinds["Child"].EventLog) != 3 {
			t.Errorf("Expected 3 Child events, got %d", len(g.Kinds["Child"].EventLog))
		}

		// Test GetStateAtReconcileID for r2 (should include parent but not child yet)
		state := g.GetStateAtReconcileID("r2")
		if state == nil {
			t.Fatal("Expected state for r2, got nil")
		}

		for k, v := range state.Objects {
			t.Logf("Object %s: %s", k, v)
		}

		parentKey := snapshot.IdentityKey{Kind: "Parent", ObjectID: "parent-1"}
		if _, exists := state.Objects[parentKey]; !exists {
			t.Error("Expected parent object in r2 state")
		}

		childKey := snapshot.IdentityKey{Kind: "Child", ObjectID: "child-1"}
		if _, exists := state.Objects[childKey]; exists {
			t.Error("Child object should not exist in r2 state yet")
		}
	})

	t.Run("interleaved state changes across kinds", func(t *testing.T) {
		events := []event.Event{
			{
				ID:           "evt-2",
				Timestamp:    "2024-02-21T10:00:01Z",
				ReconcileID:  "r2",
				ControllerID: "child-controller",
				OpType:       "GET",
				Kind:         "Parent",
				ObjectID:     "parent-1",
				Labels: map[string]string{
					"tracey-uid": "root-1",
				},
			},
			// Controller creates two children
			{
				ID:           "evt-3",
				Timestamp:    "2024-02-21T10:00:02Z",
				ReconcileID:  "r2",
				ControllerID: "child-controller",
				OpType:       "CREATE",
				Kind:         "Child",
				ObjectID:     "child-1",
				Labels: map[string]string{
					"discrete.events/root-event-id": "root-1",
					"discrete.events/change-id":     "change-1",
				},
			},
			{
				ID:           "evt-4",
				Timestamp:    "2024-02-21T10:00:02Z",
				ReconcileID:  "r2",
				ControllerID: "child-controller",
				OpType:       "CREATE",
				Kind:         "Child",
				ObjectID:     "child-2",
				Labels: map[string]string{
					"discrete.events/root-event-id": "root-1",
					"discrete.events/change-id":     "change-2",
				},
			},
			// More reads that shouldn't affect state
			{
				ID:           "evt-5",
				Timestamp:    "2024-02-21T10:00:03Z",
				ReconcileID:  "r3",
				ControllerID: "monitoring-controller",
				OpType:       "LIST",
				Kind:         "Child",
				ObjectID:     "",
				Labels: map[string]string{
					"discrete.events/root-event-id": "root-1",
					"discrete.events/change-id":     "change-1",
				},
			},
			{
				ID:           "evt-5",
				Timestamp:    "2024-02-21T10:00:03Z",
				ReconcileID:  "r3",
				ControllerID: "monitoring-controller",
				OpType:       "LIST",
				Kind:         "Child",
				ObjectID:     "",
				Labels: map[string]string{
					"discrete.events/root-event-id": "root-1",
					"discrete.events/change-id":     "change-2",
				},
			},
			// Update one of the children
			{
				ID:           "evt-6",
				Timestamp:    "2024-02-21T10:00:04Z",
				ReconcileID:  "r4",
				ControllerID: "child-controller",
				OpType:       "UPDATE",
				Kind:         "Child",
				ObjectID:     "child-1",
				Labels: map[string]string{
					"discrete.events/root-event-id": "root-1",
					"discrete.events/change-id":     "change-3",
				},
			},
		}

		g := NewGlobalKnowledge(&MockVersionResolver{})
		err := g.Load(events)
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		// Verify sequences only reflect state changes
		if len(g.Kinds["Parent"].EventLog) != 1 {
			t.Errorf("Expected 1 Parent state change, got %d", len(g.Kinds["Parent"].EventLog))
		}
		if len(g.Kinds["Child"].EventLog) != 3 {
			t.Errorf("Expected 3 Child state changes, got %d", len(g.Kinds["Child"].EventLog))
		}

		// Check sequence numbers
		if g.Kinds["Parent"].EventLog[0].Sequence != 1 {
			t.Errorf("Expected Parent CREATE to have sequence 0, got %d",
				g.Kinds["Parent"].EventLog[0].Sequence)
		}

		childEvents := g.Kinds["Child"].EventLog
		if childEvents[0].Sequence != 1 || childEvents[1].Sequence != 2 || childEvents[2].Sequence != 3 {
			t.Error("Child events have incorrect sequences")
		}
	})
}
