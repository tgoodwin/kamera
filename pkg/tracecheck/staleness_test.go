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
	return snapshot.NewDefaultHash(str), nil
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
		effect := newEffect(
			snapshot.NewCompositeKey(e.Kind, "TODO", "TODO", e.ObjectID),
			snapshot.NewDefaultHash("blah"),
			event.OperationType(e.OpType),
		)
		stateEvent := kindKnowledge.AddEvent(e, effect, ResourceVersion(i))
		expectedSequence := int64(i + 1)
		if stateEvent.Sequence != expectedSequence {
			t.Errorf("Expected sequence %d, got %d", expectedSequence, stateEvent.Sequence)
		}

		if stateEvent.rv != ResourceVersion(i) {
			t.Errorf("Expected ResourceVersion %d, got %d", i, stateEvent.rv)
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
					tag.TraceyRootID: "root-1",
					tag.ChangeID:     "change-1",
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
					tag.TraceyRootID: "root-1",
					tag.ChangeID:     "change-2",
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
					tag.TraceyRootID: "root-1",
					tag.DeletionID:   "del-1",
				},
			},
		}

		// Create a mock version resolver for testing
		mockResolver := &MockVersionResolver{}
		g := NewEventKnowledge(mockResolver)
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

		g := NewEventKnowledge(&MockVersionResolver{})
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
		g := NewEventKnowledge(&MockVersionResolver{})
		err := g.Load(events)
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		// Replay all events
		state := g.GetStateAtReconcileID("r4")
		if len(state.All()) != 3 {
			t.Errorf("Expected 3 objects in state, got %d", len(state.All()))
		}

		afterState := g.GetStateAfterReconcileID("r4")
		assert.Equal(t, 2, len(afterState.All()))

		expectedKeys := []snapshot.CompositeKey{
			snapshot.NewCompositeKey("Deployment", "default", "dep-1", "dep-1"),
			snapshot.NewCompositeKey("Deployment", "default", "dep-2", "dep-2"),
			// Pod 1 was deleted
			snapshot.NewCompositeKey("Pod", "default", "pod-2", "pod-2"),
		}
		for _, key := range expectedKeys {
			if _, exists := state.All()[key]; !exists {
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
		g := NewEventKnowledge(&MockVersionResolver{})
		err := g.Load(events)
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		state := g.GetStateAtReconcileID("r2")
		if len(state.All()) != 4 {
			t.Errorf("Expected 4 objects in state, got %d", len(state.All()))
		}
		// ensure the correct objects are in the state
		if _, exists := state.All()[snapshot.NewCompositeKey("Pod", "default", "pod-1", "pod-1")]; !exists {
			t.Error("Expected pod-1 in state")
		}
		if _, exists := state.All()[snapshot.NewCompositeKey("Pod", "default", "pod-2", "pod-2")]; !exists {
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
			for k := range s.All() {
				if k.IdentityKey.Kind == "Pod" {
					count++
				}
			}
			return count
		}
		g := NewEventKnowledge(&MockVersionResolver{})
		err := g.Load(events)
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		// replay all events
		state := g.GetStateAtReconcileID("r4")
		if countPods(state) != 1 {
			t.Errorf("Expected 1 pods in state, got %d", len(state.All()))
		}
		rewind, err := g.AdjustKnowledgeForResourceType(state, "Pod", -1)
		if err != nil {
			t.Fatalf("AdjustKnowledgeForKind failed: %v", err)
		}
		if countPods(rewind) != 2 {
			t.Errorf("Expected 2 pods in state, got %d", len(rewind.All()))
		}

		ff, err := g.AdjustKnowledgeForResourceType(state, "Pod", 1)
		if err != nil {
			t.Fatalf("AdjustKnowledgeForKind failed: %v", err)
		}
		if countPods(ff) != 0 {
			t.Errorf("Expected 0 pods in state, got %d", len(ff.All()))
		}
	})
}
func TestReplayEventsToState(t *testing.T) {
	tests := []struct {
		name          string
		events        []StateEvent
		expectedState ObjectVersions
		expectedSeq   map[string]int64
	}{
		{
			name: "single create event",
			events: []StateEvent{
				{
					ReconcileID: "r1",
					Timestamp:   "2024-02-21T10:00:01Z",
					Effect: effect{
						Key:     snapshot.NewCompositeKey("Pod", "default", "pod-1", "pod-1"),
						Version: snapshot.NewDefaultHash("v1"),
						OpType:  event.CREATE,
					},
					Sequence: 1,
				},
			},
			expectedState: map[snapshot.CompositeKey]snapshot.VersionHash{
				snapshot.NewCompositeKey("Pod", "default", "pod-1", "pod-1"): snapshot.NewDefaultHash("v1"),
			},
			expectedSeq: map[string]int64{
				"Pod": 1,
			},
		},
		{
			name: "create and delete event",
			events: []StateEvent{
				{
					ReconcileID: "r1",
					Timestamp:   "2024-02-21T10:00:01Z",
					Effect: effect{
						Key:     snapshot.NewCompositeKey("Pod", "default", "pod-1", "pod-1"),
						Version: snapshot.NewDefaultHash("v1"),
						OpType:  event.CREATE,
					},
					Sequence: 1,
				},
				{
					ReconcileID: "r2",
					Timestamp:   "2024-02-21T10:00:02Z",
					Effect: effect{
						Key:    snapshot.NewCompositeKey("Pod", "default", "pod-1", "pod-1"),
						OpType: event.DELETE,
					},
					Sequence: 2,
				},
			},
			expectedState: map[snapshot.CompositeKey]snapshot.VersionHash{},
			expectedSeq: map[string]int64{
				"Pod": 2,
			},
		},
		{
			name: "multiple kinds",
			events: []StateEvent{
				{
					ReconcileID: "r1",
					Timestamp:   "2024-02-21T10:00:01Z",
					Effect: effect{
						Key:     snapshot.NewCompositeKey("Pod", "default", "pod-1", "pod-1"),
						Version: snapshot.NewDefaultHash("v1"),
						OpType:  event.CREATE,
					},
					Sequence: 1,
				},
				{
					ReconcileID: "r2",
					Timestamp:   "2024-02-21T10:00:02Z",
					Effect: effect{
						Key:     snapshot.NewCompositeKey("Service", "default", "svc-1", "svc-1"),
						Version: snapshot.NewDefaultHash("v2"),
						OpType:  event.CREATE,
					},
					Sequence: 1,
				},
			},
			expectedState: map[snapshot.CompositeKey]snapshot.VersionHash{
				snapshot.NewCompositeKey("Pod", "default", "pod-1", "pod-1"):     snapshot.NewDefaultHash("v1"),
				snapshot.NewCompositeKey("Service", "default", "svc-1", "svc-1"): snapshot.NewDefaultHash("v2"),
			},
			expectedSeq: map[string]int64{
				"Pod":     1,
				"Service": 1,
			},
		},
		{
			name: "update event",
			events: []StateEvent{
				{
					ReconcileID: "r1",
					Timestamp:   "2024-02-21T10:00:01Z",
					Effect: effect{
						Key:     snapshot.NewCompositeKey("Pod", "default", "pod-1", "pod-1"),
						Version: snapshot.NewDefaultHash("v1"),
						OpType:  event.CREATE,
					},
					Sequence: 1,
				},
				{
					ReconcileID: "r2",
					Timestamp:   "2024-02-21T10:00:02Z",
					Effect: effect{
						Key:     snapshot.NewCompositeKey("Pod", "default", "pod-1", "pod-1"),
						Version: snapshot.NewDefaultHash("v2"),
						OpType:  event.UPDATE,
					},
					Sequence: 2,
				},
			},
			expectedState: map[snapshot.CompositeKey]snapshot.VersionHash{
				snapshot.NewCompositeKey("Pod", "default", "pod-1", "pod-1"): snapshot.NewDefaultHash("v2"),
			},
			expectedSeq: map[string]int64{
				"Pod": 2,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := replayEventSequenceToState(tt.events)

			assert.Equal(t, tt.expectedState, state.All())
			assert.Equal(t, tt.expectedSeq, state.KindSequences)
			assert.Equal(t, tt.events, state.stateEvents)
		})
	}
}
func TestReplayEventsAtSequence(t *testing.T) {
	events := []StateEvent{
		{
			ReconcileID: "r1",
			Timestamp:   "2024-02-21T10:00:01Z",
			Effect: effect{
				Key:     snapshot.NewCompositeKey("Pod", "default", "pod-1", "pod-1"),
				Version: snapshot.NewDefaultHash("v1"),
				OpType:  event.CREATE,
			},
			Sequence: 1,
		},
		{
			ReconcileID: "r2",
			Timestamp:   "2024-02-21T10:00:02Z",
			Effect: effect{
				Key:     snapshot.NewCompositeKey("Pod", "default", "pod-1", "pod-1"),
				Version: snapshot.NewDefaultHash("v2"),
				OpType:  event.UPDATE,
			},
			Sequence: 2,
		},
		{
			ReconcileID: "r3",
			Timestamp:   "2024-02-21T10:00:03Z",
			Effect: effect{
				Key:    snapshot.NewCompositeKey("Pod", "default", "pod-1", "pod-1"),
				OpType: event.DELETE,
			},
			Sequence: 3,
		},
		{
			ReconcileID: "r4",
			Timestamp:   "2024-02-21T10:00:04Z",
			Effect: effect{
				Key:     snapshot.NewCompositeKey("Pod", "default", "pod-2", "pod-2"),
				Version: snapshot.NewDefaultHash("v1"),
				OpType:  event.CREATE,
			},
			Sequence: 4,
		},
		{
			ReconcileID: "r5",
			Timestamp:   "2024-02-21T10:00:05Z",
			Effect: effect{
				Key:     snapshot.NewCompositeKey("Pod", "default", "pod-2", "pod-2"),
				Version: snapshot.NewDefaultHash("v2"),
				OpType:  event.UPDATE,
			},
			Sequence: 5,
		},
		{
			ReconcileID: "r6",
			Timestamp:   "2024-02-21T10:00:06Z",
			Effect: effect{
				Key:    snapshot.NewCompositeKey("Pod", "default", "pod-2", "pod-2"),
				OpType: event.DELETE,
			},
			Sequence: 6,
		},
		{
			ReconcileID: "r7",
			Timestamp:   "2024-02-21T10:00:07Z",
			Effect: effect{
				Key:     snapshot.NewCompositeKey("Service", "default", "svc-1", "svc-1"),
				Version: snapshot.NewDefaultHash("v1"),
				OpType:  event.CREATE,
			},
			Sequence: 1,
		},
		{
			ReconcileID: "r8",
			Timestamp:   "2024-02-21T10:00:08Z",
			Effect: effect{
				Key:     snapshot.NewCompositeKey("Service", "default", "svc-1", "svc-1"),
				Version: snapshot.NewDefaultHash("v2"),
				OpType:  event.UPDATE,
			},
			Sequence: 2,
		},
		{
			ReconcileID: "r9",
			Timestamp:   "2024-02-21T10:00:09Z",
			Effect: effect{
				Key:    snapshot.NewCompositeKey("Service", "default", "svc-1", "svc-1"),
				OpType: event.DELETE,
			},
			Sequence: 3,
		},
		{
			ReconcileID: "r10",
			Timestamp:   "2024-02-21T10:00:10Z",
			Effect: effect{
				Key:     snapshot.NewCompositeKey("Service", "default", "svc-2", "svc-2"),
				Version: snapshot.NewDefaultHash("v1"),
				OpType:  event.CREATE,
			},
			Sequence: 4,
		},
		{
			ReconcileID: "r11",
			Timestamp:   "2024-02-21T10:00:11Z",
			Effect: effect{
				Key:     snapshot.NewCompositeKey("Service", "default", "svc-2", "svc-2"),
				Version: snapshot.NewDefaultHash("v2"),
				OpType:  event.UPDATE,
			},
			Sequence: 5,
		},
		{
			ReconcileID: "r12",
			Timestamp:   "2024-02-21T10:00:12Z",
			Effect: effect{
				Key:    snapshot.NewCompositeKey("Service", "default", "svc-2", "svc-2"),
				OpType: event.DELETE,
			},
			Sequence: 6,
		},
	}

	tests := []struct {
		name            string
		sequencesByKind map[string]int64
		expectedState   ObjectVersions
		expectedSeq     map[string]int64
	}{
		{
			name: "initial state",
			sequencesByKind: map[string]int64{
				"Pod":     0,
				"Service": 0,
			},
			expectedState: ObjectVersions{},
			expectedSeq:   map[string]int64{},
		},
		{
			name: "after first pod create",
			sequencesByKind: map[string]int64{
				"Pod":     1,
				"Service": 0,
			},
			expectedState: ObjectVersions{
				snapshot.NewCompositeKey("Pod", "default", "pod-1", "pod-1"): snapshot.NewDefaultHash("v1"),
			},
			expectedSeq: map[string]int64{
				"Pod": 1,
			},
		},
		{
			name: "after first pod update",
			sequencesByKind: map[string]int64{
				"Pod":     2,
				"Service": 0,
			},
			expectedState: ObjectVersions{
				snapshot.NewCompositeKey("Pod", "default", "pod-1", "pod-1"): snapshot.NewDefaultHash("v2"),
			},
			expectedSeq: map[string]int64{
				"Pod": 2,
			},
		},
		{
			name: "after first pod delete",
			sequencesByKind: map[string]int64{
				"Pod":     3,
				"Service": 0,
			},
			expectedState: ObjectVersions{},
			expectedSeq: map[string]int64{
				"Pod": 3,
			},
		},
		{
			name: "after second pod create",
			sequencesByKind: map[string]int64{
				"Pod":     4,
				"Service": 0,
			},
			expectedState: ObjectVersions{
				snapshot.NewCompositeKey("Pod", "default", "pod-2", "pod-2"): snapshot.NewDefaultHash("v1"),
			},
			expectedSeq: map[string]int64{
				"Pod": 4,
			},
		},
		{
			name: "after second pod update",
			sequencesByKind: map[string]int64{
				"Pod":     5,
				"Service": 0,
			},
			expectedState: ObjectVersions{
				snapshot.NewCompositeKey("Pod", "default", "pod-2", "pod-2"): snapshot.NewDefaultHash("v2"),
			},
			expectedSeq: map[string]int64{
				"Pod": 5,
			},
		},
		{
			name: "after second pod delete",
			sequencesByKind: map[string]int64{
				"Pod":     6,
				"Service": 0,
			},
			expectedState: ObjectVersions{},
			expectedSeq: map[string]int64{
				"Pod": 6,
			},
		},
		{
			name: "after first service create",
			sequencesByKind: map[string]int64{
				"Pod":     1,
				"Service": 1,
			},
			expectedState: ObjectVersions{
				snapshot.NewCompositeKey("Pod", "default", "pod-1", "pod-1"):     snapshot.NewDefaultHash("v1"),
				snapshot.NewCompositeKey("Service", "default", "svc-1", "svc-1"): snapshot.NewDefaultHash("v1"),
			},
			expectedSeq: map[string]int64{
				"Pod":     1,
				"Service": 1,
			},
		},
		{
			name: "multi object update",
			sequencesByKind: map[string]int64{
				"Pod":     2,
				"Service": 2,
			},
			expectedState: ObjectVersions{
				snapshot.NewCompositeKey("Pod", "default", "pod-1", "pod-1"):     snapshot.NewDefaultHash("v2"),
				snapshot.NewCompositeKey("Service", "default", "svc-1", "svc-1"): snapshot.NewDefaultHash("v2"),
			},
			expectedSeq: map[string]int64{
				"Pod":     2,
				"Service": 2,
			},
		},
		{
			name: "after second service create",
			sequencesByKind: map[string]int64{
				"Pod":     2,
				"Service": 4,
			},
			expectedState: ObjectVersions{
				snapshot.NewCompositeKey("Pod", "default", "pod-1", "pod-1"):     snapshot.NewDefaultHash("v2"),
				snapshot.NewCompositeKey("Service", "default", "svc-2", "svc-2"): snapshot.NewDefaultHash("v1"),
			},
			expectedSeq: map[string]int64{
				"Pod":     2,
				"Service": 4,
			},
		},
		{
			name: "after second service update",
			sequencesByKind: map[string]int64{
				"Pod":     2,
				"Service": 5,
			},
			expectedState: ObjectVersions{
				snapshot.NewCompositeKey("Pod", "default", "pod-1", "pod-1"):     snapshot.NewDefaultHash("v2"),
				snapshot.NewCompositeKey("Service", "default", "svc-2", "svc-2"): snapshot.NewDefaultHash("v2"),
			},
			expectedSeq: map[string]int64{
				"Pod":     2,
				"Service": 5,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := replayEventsAtSequence(events, tt.sequencesByKind)

			assert.Equal(t, tt.expectedState, state.All())
			assert.Equal(t, tt.expectedSeq, state.KindSequences)
		})
	}
}
func TestGetAllPossibleStaleViews(t *testing.T) {
	events := []StateEvent{
		{
			ReconcileID: "r1",
			Timestamp:   "2024-02-21T10:00:01Z",
			Effect: effect{
				Key:     snapshot.NewCompositeKey("Pod", "default", "pod-1", "pod-1"),
				Version: snapshot.NewDefaultHash("v1"),
				OpType:  event.CREATE,
			},
			Sequence: 1,
		},
		{
			ReconcileID: "r2",
			Timestamp:   "2024-02-21T10:00:02Z",
			Effect: effect{
				Key:     snapshot.NewCompositeKey("Pod", "default", "pod-1", "pod-1"),
				Version: snapshot.NewDefaultHash("v2"),
				OpType:  event.UPDATE,
			},
			Sequence: 2,
		},
		{
			ReconcileID: "r3",
			Timestamp:   "2024-02-21T10:00:03Z",
			Effect: effect{
				Key:     snapshot.NewCompositeKey("Service", "default", "svc-1", "svc-1"),
				Version: snapshot.NewDefaultHash("v1"),
				OpType:  event.CREATE,
			},
			Sequence: 1,
		},
		{
			ReconcileID: "r4",
			Timestamp:   "2024-02-21T10:00:04Z",
			Effect: effect{
				Key:     snapshot.NewCompositeKey("Service", "default", "svc-1", "svc-1"),
				Version: snapshot.NewDefaultHash("v2"),
				OpType:  event.UPDATE,
			},
			Sequence: 2,
		},
	}

	state := &StateSnapshot{
		contents: ObjectVersions{
			snapshot.NewCompositeKey("Pod", "default", "pod-1", "pod-1"):     snapshot.NewDefaultHash("v2"),
			snapshot.NewCompositeKey("Service", "default", "svc-1", "svc-1"): snapshot.NewDefaultHash("v2"),
		},
		KindSequences: map[string]int64{
			"Pod":     2,
			"Service": 2,
		},
		stateEvents: events,
	}

	expectedStaleViews := []*StateSnapshot{
		{
			contents: ObjectVersions{
				snapshot.NewCompositeKey("Pod", "default", "pod-1", "pod-1"):     snapshot.NewDefaultHash("v1"),
				snapshot.NewCompositeKey("Service", "default", "svc-1", "svc-1"): snapshot.NewDefaultHash("v1"),
			},
			KindSequences: map[string]int64{
				"Pod":     1,
				"Service": 1,
			},
			stateEvents: events,
		},
		{
			contents: ObjectVersions{
				snapshot.NewCompositeKey("Pod", "default", "pod-1", "pod-1"):     snapshot.NewDefaultHash("v1"),
				snapshot.NewCompositeKey("Service", "default", "svc-1", "svc-1"): snapshot.NewDefaultHash("v2"),
			},
			KindSequences: map[string]int64{
				"Pod":     1,
				"Service": 2,
			},
			stateEvents: events,
		},
		{
			contents: ObjectVersions{
				snapshot.NewCompositeKey("Pod", "default", "pod-1", "pod-1"):     snapshot.NewDefaultHash("v2"),
				snapshot.NewCompositeKey("Service", "default", "svc-1", "svc-1"): snapshot.NewDefaultHash("v1"),
			},
			KindSequences: map[string]int64{
				"Pod":     2,
				"Service": 1,
			},
			stateEvents: events,
		},
		{
			contents: ObjectVersions{
				snapshot.NewCompositeKey("Pod", "default", "pod-1", "pod-1"):     snapshot.NewDefaultHash("v2"),
				snapshot.NewCompositeKey("Service", "default", "svc-1", "svc-1"): snapshot.NewDefaultHash("v2"),
			},
			KindSequences: map[string]int64{
				"Pod":     2,
				"Service": 2,
			},
			stateEvents: events,
		},
	}

	staleViews := getAllPossibleViews(state, []string{"Pod", "Service"})

	assert.Equal(t, len(expectedStaleViews), len(staleViews))

	expectedMap := make(map[string]*StateSnapshot)
	for _, expected := range expectedStaleViews {
		key := fmt.Sprintf("%v", expected.KindSequences)
		expectedMap[key] = expected
	}

	for _, staleView := range staleViews {
		key := fmt.Sprintf("%v", staleView.KindSequences)
		expected, exists := expectedMap[key]
		if !exists {
			t.Errorf("Unexpected stale view with KindSequences %v", staleView.KindSequences)
			continue
		}

		// check that the stale view has the expected "ground truth" objects
		assert.Equal(t, state.All(), staleView.All())

		expectedObjects := expected.All()
		staleViewObjects := staleView.Observable()

		assert.Equal(t, len(expectedObjects), len(staleViewObjects))
		for key, expectedVersion := range expectedObjects {
			staleVersion, exists := staleViewObjects[key]
			if !exists {
				t.Errorf("Expected %s in stale view", key)
			}
			assert.Equal(t, expectedVersion, staleVersion)
		}
		assert.Equal(t, len(expected.KindSequences), len(staleView.KindSequences))
		for kind, seq := range expected.KindSequences {
			assert.Equal(t, seq, staleView.KindSequences[kind])
		}
	}
}

func Test_getAllCombos(t *testing.T) {
	values := map[string][]int64{
		"a": {1, 2, 3},
		"b": {10, 20},
	}

	combos := getAllCombos(values)
	expected := []map[string]int64{
		{"a": 1, "b": 10},
		{"a": 1, "b": 20},
		{"a": 2, "b": 10},
		{"a": 2, "b": 20},
		{"a": 3, "b": 10},
		{"a": 3, "b": 20},
	}
	assert.ElementsMatch(t, expected, combos)
}
