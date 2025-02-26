package tracecheck

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"github.com/tgoodwin/sleeve/pkg/tag"
)

type MockVersionResolver struct{}

func (m *MockVersionResolver) ResolveVersion(key event.CausalKey) (snapshot.VersionHash, error) {
	// Return some deterministic object state based on the CausalKey
	str := fmt.Sprintf("state-%s-%s", key.ObjectID, string(key.ChangeID))
	return snapshot.VersionHash(str), nil
}

func TestKindKnowledge_AddEvent(t *testing.T) {
	kindKnowledge := NewKindKnowledge()

	events := []event.Event{
		{
			ID:           "evt-1",
			Timestamp:    "2024-02-21T10:00:01Z",
			ReconcileID:  "r1",
			ControllerID: "controller-1",
			OpType:       "CREATE",
			Kind:         "TestKind",
			ObjectID:     "obj-1",
			Labels: map[string]string{
				"tracey-uid": "root-1",
			},
		},
		{
			ID:           "evt-2",
			Timestamp:    "2024-02-21T10:00:02Z",
			ReconcileID:  "r1",
			ControllerID: "controller-1",
			OpType:       "UPDATE",
			Kind:         "TestKind",
			ObjectID:     "obj-1",
			Labels: map[string]string{
				tag.TraceyRootID: "root-1",
				tag.ChangeID:     "change-1",
			},
		},
		{
			ID:           "evt-3",
			Timestamp:    "2024-02-21T10:00:03Z",
			ReconcileID:  "r1",
			ControllerID: "controller-1",
			OpType:       "DELETE",
			Kind:         "TestKind",
			ObjectID:     "obj-1",
			Labels: map[string]string{
				tag.TraceyRootID: "root-1",
				tag.ChangeID:     "change-1",
				tag.DeletionID:   "del-1",
			},
		},
	}

	for i, e := range events {
		stateEvent := kindKnowledge.AddEvent(e)
		expectedSequence := int64(i + 1)
		if stateEvent.Sequence != expectedSequence {
			t.Errorf("Expected sequence %d, got %d", expectedSequence, stateEvent.Sequence)
		}
	}

	if len(kindKnowledge.EventLog) != len(events) {
		t.Errorf("Expected %d events in EventLog, got %d", len(events), len(kindKnowledge.EventLog))
	}

	for i, stateEvent := range kindKnowledge.EventLog {
		expectedSequence := int64(i + 1)
		if stateEvent.Sequence != expectedSequence {
			t.Errorf("Expected sequence %d in EventLog, got %d", expectedSequence, stateEvent.Sequence)
		}
	}
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
		firstParent := g.Kinds["Parent"].EventLog[0]
		if firstParent.Sequence != 1 {
			t.Errorf("Expected Parent GET to have sequence 1, got %d", firstParent.Sequence)
		}
		if len(g.Kinds["Child"].EventLog) != 3 {
			t.Errorf("Expected 3 Child events, got %d", len(g.Kinds["Child"].EventLog))
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
				ObjectID:     "child-1",
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
				ObjectID:     "child-2",
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

func TestGlobalKnowledge_replayEventsToState(t *testing.T) {
	events := []event.Event{
		{
			ID:           "evt-01",
			Timestamp:    "2024-02-21T10:00:01Z",
			ReconcileID:  "r0",
			ControllerID: "pod-controller",
			OpType:       "GET",
			Kind:         "Deployment",
			ObjectID:     "dep-1",
			Labels: map[string]string{
				"tracey-uid": "root-1",
			},
		},
		{
			ID:           "evt-02",
			Timestamp:    "2024-02-21T10:00:02Z",
			ReconcileID:  "r0",
			ControllerID: "pod-controller",
			OpType:       "GET",
			Kind:         "Deployment",
			ObjectID:     "dep-2",
			Labels: map[string]string{
				"tracey-uid": "root-2",
			},
		},
		// Add two pods
		{
			ID:           "evt-1",
			Timestamp:    "2024-02-21T10:00:03Z",
			ReconcileID:  "r1",
			ControllerID: "pod-controller",
			OpType:       "CREATE",
			Kind:         "Pod",
			ObjectID:     "pod-1",
			Labels: map[string]string{
				"discrete.events/root-event-id": "root-1",
				"discrete.events/change-id":     "change-1",
			},
		},
		{
			ID:           "evt-2",
			Timestamp:    "2024-02-21T10:00:04Z",
			ReconcileID:  "r1",
			ControllerID: "pod-controller",
			OpType:       "CREATE",
			Kind:         "Pod",
			ObjectID:     "pod-2",
			Labels: map[string]string{
				"discrete.events/root-event-id": "root-2",
				"discrete.events/change-id":     "change-2",
			},
		},
		// Get pods before updating
		{
			ID:           "evt-3",
			Timestamp:    "2024-02-21T10:00:05Z",
			ReconcileID:  "r2",
			ControllerID: "pod-controller",
			OpType:       "GET",
			Kind:         "Pod",
			ObjectID:     "pod-1",
			Labels: map[string]string{
				"discrete.events/root-event-id": "root-1",
				"discrete.events/change-id":     "change-1",
			},
		},
		{
			ID:           "evt-4",
			Timestamp:    "2024-02-21T10:00:06Z",
			ReconcileID:  "r2",
			ControllerID: "pod-controller",
			OpType:       "GET",
			Kind:         "Pod",
			ObjectID:     "pod-2",
			Labels: map[string]string{
				"discrete.events/root-event-id": "root-2",
				"discrete.events/change-id":     "change-2",
			},
		},
		// Update pods
		{
			ID:           "evt-5",
			Timestamp:    "2024-02-21T10:00:07Z",
			ReconcileID:  "r2",
			ControllerID: "pod-controller",
			OpType:       "UPDATE",
			Kind:         "Pod",
			ObjectID:     "pod-1",
			Labels: map[string]string{
				"discrete.events/root-event-id": "root-1",
				"discrete.events/change-id":     "change-1a",
			},
		},
		{
			ID:           "evt-6",
			Timestamp:    "2024-02-21T10:00:08Z",
			ReconcileID:  "r2",
			ControllerID: "pod-controller",
			OpType:       "UPDATE",
			Kind:         "Pod",
			ObjectID:     "pod-2",
			Labels: map[string]string{
				"discrete.events/root-event-id": "root-2",
				"discrete.events/change-id":     "change-2a",
			},
		},
		// Delete pods
		{
			ID:           "evt-7",
			Timestamp:    "2024-02-21T10:00:09Z",
			ReconcileID:  "r3",
			ControllerID: "pod-controller",
			OpType:       "GET",
			Kind:         "Pod",
			ObjectID:     "pod-1",
			Labels: map[string]string{
				"discrete.events/root-event-id": "root-1",
				"discrete.events/change-id":     "change-1a",
			},
		},
		{
			ID:           "evt-8",
			Timestamp:    "2024-02-21T10:00:10Z",
			ReconcileID:  "r3",
			ControllerID: "pod-controller",
			OpType:       "DELETE",
			Kind:         "Pod",
			ObjectID:     "pod-1",
			Labels: map[string]string{
				"discrete.events/root-event-id": "root-1",
				"discrete.events/change-id":     "change-1a",
				tag.DeletionID:                  "del-1",
			},
		},
		{
			ID:           "evt-9",
			Timestamp:    "2024-02-21T10:00:11Z",
			ReconcileID:  "r4",
			ControllerID: "pod-controller",
			OpType:       "GET",
			Kind:         "Pod",
			ObjectID:     "pod-2",
			Labels: map[string]string{
				"discrete.events/root-event-id": "root-2",
				"discrete.events/change-id":     "change-2a",
			},
		},
		{
			ID:           "evt-10",
			Timestamp:    "2024-02-21T10:00:12Z",
			ReconcileID:  "r4",
			ControllerID: "pod-controller",
			OpType:       "DELETE",
			Kind:         "Pod",
			ObjectID:     "pod-2",
			Labels: map[string]string{
				"discrete.events/root-event-id": "root-2",
				"discrete.events/change-id":     "change-2a",
				tag.DeletionID:                  "del-2",
			},
		},
	}
	t.Run("replay all events", func(t *testing.T) {
		g := NewGlobalKnowledge(&MockVersionResolver{})
		err := g.Load(events)
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		// Replay all events
		state := g.GetStateAtReconcileID("r4")
		if len(state.Objects()) != 3 {
			t.Errorf("Expected 3 objects in state, got %d", len(state.Objects()))
		}

		afterState := g.GetStateAfterReconcileID("r4")
		assert.Equal(t, 2, len(afterState.Objects()))

		expectedKeys := []snapshot.IdentityKey{
			{Kind: "Deployment", ObjectID: "dep-1"},
			{Kind: "Deployment", ObjectID: "dep-2"},
			// Pod 1 was deleted
			{Kind: "Pod", ObjectID: "pod-2"},
		}
		for _, key := range expectedKeys {
			if _, exists := state.Objects()[key]; !exists {
				t.Errorf("Expected %s in state", key)
			}
		}
		// check kind sequence
		if state.KindSequences["Deployment"] != 2 {
			t.Errorf("Expected Deployment sequence 2, got %d", state.KindSequences["Deployment"])
		}
		if state.KindSequences["Pod"] != 5 {
			t.Errorf("Expected Pod sequence 6, got %d", state.KindSequences["Pod"])
		}
	})
	t.Run("reply half the events", func(t *testing.T) {
		g := NewGlobalKnowledge(&MockVersionResolver{})
		err := g.Load(events)
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		state := g.GetStateAtReconcileID("r2")
		if len(state.Objects()) != 4 {
			t.Errorf("Expected 4 objects in state, got %d", len(state.Objects()))
		}
		// ensure the correct objects are in the state
		if _, exists := state.Objects()[snapshot.IdentityKey{Kind: "Pod", ObjectID: "pod-1"}]; !exists {
			t.Error("Expected pod-1 in state")
		}
		if _, exists := state.Objects()[snapshot.IdentityKey{Kind: "Pod", ObjectID: "pod-2"}]; !exists {
			t.Error("Expected pod-2 in state")
		}
		// check kind sequence
		if state.KindSequences["Pod"] != 2 {
			t.Errorf("Expected Pod sequence 2, got %d", state.KindSequences["Pod"])
		}
	})
	t.Run("rewind knowledge", func(t *testing.T) {
		countPods := func(s *StateSnapshot) int {
			count := 0
			for k := range s.Objects() {
				if k.Kind == "Pod" {
					count++
				}
			}
			return count
		}
		g := NewGlobalKnowledge(&MockVersionResolver{})
		err := g.Load(events)
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		// replay all events
		state := g.GetStateAtReconcileID("r4")
		if countPods(state) != 1 {
			t.Errorf("Expected 1 pods in state, got %d", len(state.Objects()))
		}
		rewind, err := g.AdjustKnowledgeForKind(state, "Pod", -1)
		if err != nil {
			t.Fatalf("AdjustKnowledgeForKind failed: %v", err)
		}
		if countPods(rewind) != 2 {
			t.Errorf("Expected 2 pods in state, got %d", len(rewind.Objects()))
		}

		ff, err := g.AdjustKnowledgeForKind(state, "Pod", 1)
		if err != nil {
			t.Fatalf("AdjustKnowledgeForKind failed: %v", err)
		}
		if countPods(ff) != 0 {
			t.Errorf("Expected 0 pods in state, got %d", len(ff.Objects()))
		}
	})
}
