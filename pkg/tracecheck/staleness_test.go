package tracecheck

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/util"
)

type MockVersionResolver struct{}

func (m *MockVersionResolver) ResolveVersion(key event.CausalKey) (snapshot.VersionHash, error) {
	// Return some deterministic object state based on the CausalKey
	str := fmt.Sprintf("state-%s-%s", key.ObjectID, string(key.ChangeID))
	return snapshot.NewDefaultHash(str), nil
}

func canonical(group, kind string) string {
	return util.CanonicalGroupKind(group, kind)
}

func assignGVK(events []event.Event, group, version string, kinds ...string) {
	kindSet := make(map[string]struct{}, len(kinds))
	for _, k := range kinds {
		kindSet[k] = struct{}{}
	}
	apiVersion := formatAPIVersion(group, version)
	for i := range events {
		if len(kindSet) == 0 {
			events[i].Group = group
			events[i].APIVersion = apiVersion
			continue
		}
		if _, ok := kindSet[events[i].Kind]; ok {
			events[i].Group = group
			events[i].APIVersion = apiVersion
		}
	}
}

func formatAPIVersion(group, version string) string {
	if version == "" {
		version = "v1"
	}
	if group == "" {
		return version
	}
	return group + "/" + version
}

var (
	appsDeployment = canonical("apps", "Deployment")
	appsReplicaSet = canonical("apps", "ReplicaSet")
	corePod        = canonical("", "Pod")
	coreService    = canonical("", "Service")
	coreEndpoints  = canonical("", "Endpoints")
	exampleParent  = canonical("example.dev", "Parent")
	exampleChild   = canonical("example.dev", "Child")
)

type gvSpec struct {
	group   string
	version string
}

func assignGVKByKind(events []event.Event, specs map[string]gvSpec) {
	for i := range events {
		if spec, ok := specs[events[i].Kind]; ok {
			events[i].Group = spec.group
			events[i].APIVersion = formatAPIVersion(spec.group, spec.version)
		}
	}
}

func groupForKind(kind string) string {
	switch kind {
	case "Pod", "Service", "Endpoints":
		return ""
	case "Deployment", "ReplicaSet", "StatefulSet", "DaemonSet":
		return "apps"
	case "Job":
		return "batch"
	case "Parent", "Child":
		return "example.dev"
	default:
		return ""
	}
}

func compositeKey(kind, namespace, name, objectID string) snapshot.CompositeKey {
	return snapshot.NewCompositeKeyWithGroup(groupForKind(kind), kind, namespace, name, objectID)
}

func canonicalizeSeq(seq KindSequences) KindSequences {
	if seq == nil {
		return nil
	}
	out := make(KindSequences, len(seq))
	for k, v := range seq {
		if strings.Contains(k, "/") {
			out[k] = v
			continue
		}
		out[canonical(groupForKind(k), k)] = v
	}
	return out
}

func canonicalSeqSlices(seq map[string][]int64) map[string][]int64 {
	if seq == nil {
		return nil
	}
	out := make(map[string][]int64, len(seq))
	for k, v := range seq {
		key := k
		if !strings.Contains(k, "/") {
			key = canonical(groupForKind(k), k)
		}
		out[key] = v
	}
	return out
}

func canonicalLookbackLimits(l LookbackLimits) LookbackLimits {
	if l == nil {
		return nil
	}
	out := make(LookbackLimits, len(l))
	for k, v := range l {
		key := k
		if !strings.Contains(k, "/") {
			key = canonical(groupForKind(k), k)
		}
		out[key] = v
	}
	return out
}

// TODO cleanup this approach is deprecated
func TestKindKnowledge_AddEvent(t *testing.T) {
	const (
		testGroup = "example.dev"
		testKind  = "Widget"
	)

	kindKnowledge := NewKindKnowledge()

	events := []event.Event{
		{
			APIVersion:   testGroup + "/v1",
			ID:           "evt-1",
			Timestamp:    "2024-02-21T10:00:01Z",
			ReconcileID:  "r1",
			ControllerID: "controller-1",
			OpType:       "CREATE",
			Group:        testGroup,
			Kind:         testKind,
			ObjectID:     "obj-1",
			Labels: map[string]string{
				tag.TraceyWebhookLabel: "root-1",
			},
		},
		{
			APIVersion:   testGroup + "/v1",
			ID:           "evt-2",
			Timestamp:    "2024-02-21T10:00:02Z",
			ReconcileID:  "r1",
			ControllerID: "controller-1",
			OpType:       "UPDATE",
			Group:        testGroup,
			Kind:         testKind,
			ObjectID:     "obj-1",
			Labels: map[string]string{
				tag.TraceyRootID: "root-1",
				tag.ChangeID:     "change-1",
			},
		},
		{
			APIVersion:   testGroup + "/v1",
			ID:           "evt-3",
			Timestamp:    "2024-02-21T10:00:03Z",
			ReconcileID:  "r1",
			ControllerID: "controller-1",
			OpType:       "DELETE",
			Group:        testGroup,
			Kind:         testKind,
			ObjectID:     "obj-1",
			Labels: map[string]string{
				tag.TraceyRootID: "root-1",
				tag.ChangeID:     "change-1",
				tag.DeletionID:   "del-1",
			},
		},
	}
	assignGVK(events, testGroup, "v1", testKind)

	for i, e := range events {
		effect := newEffect(
			snapshot.NewCompositeKeyWithGroup(e.Group, e.Kind, "TODO", "TODO", e.ObjectID),
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
					tag.TraceyWebhookLabel: "root-1",
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

			// Controller REMOVEs child
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
		assignGVKByKind(testEvents, map[string]gvSpec{
			"Parent": {group: "example.dev", version: "v1"},
			"Child":  {group: "example.dev", version: "v1"},
		})
		mockResolver := &MockVersionResolver{}
		g := NewEventKnowledge(mockResolver)
		err := g.Load(testEvents)
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		// Test that sequences were assigned correctly
		parentKey := canonical("example.dev", "Parent")
		childKey := canonical("example.dev", "Child")

		if len(g.Kinds[parentKey].EventLog) != 1 {
			t.Errorf("Expected 1 Parent events, got %d", len(g.Kinds[parentKey].EventLog))
		}
		firstParent := g.Kinds[parentKey].EventLog[0]
		if firstParent.Sequence != 1 {
			t.Errorf("Expected Parent GET to have sequence 1, got %d", firstParent.Sequence)
		}
		if len(g.Kinds[childKey].EventLog) != 3 {
			t.Errorf("Expected 3 Child events, got %d", len(g.Kinds[childKey].EventLog))
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
					tag.TraceyWebhookLabel: "root-1",
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
					tag.TraceyRootID: "root-1",
					tag.ChangeID:     "change-1",
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
					tag.TraceyRootID: "root-1",
					tag.ChangeID:     "change-2",
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
					tag.TraceyRootID: "root-1",
					tag.ChangeID:     "change-1",
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
					tag.TraceyRootID: "root-1",
					tag.ChangeID:     "change-2",
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
					tag.TraceyRootID: "root-1",
					tag.ChangeID:     "change-3",
				},
			},
		}
		assignGVK(events, "example.dev", "v1", "Parent", "Child")

		g := NewEventKnowledge(&MockVersionResolver{})
		assignGVKByKind(events, map[string]gvSpec{
			"Parent": {group: "example.dev", version: "v1"},
			"Child":  {group: "example.dev", version: "v1"},
		})
		err := g.Load(events)
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		// Verify sequences only reflect state changes
		parentKey := canonical("example.dev", "Parent")
		childKey := canonical("example.dev", "Child")

		if len(g.Kinds[parentKey].EventLog) != 1 {
			t.Errorf("Expected 1 Parent state change, got %d", len(g.Kinds[parentKey].EventLog))
		}
		if len(g.Kinds[childKey].EventLog) != 3 {
			t.Errorf("Expected 3 Child state changes, got %d", len(g.Kinds[childKey].EventLog))
		}

		// Check sequence numbers
		if g.Kinds[parentKey].EventLog[0].Sequence != 1 {
			t.Errorf("Expected Parent CREATE to have sequence 0, got %d",
				g.Kinds[parentKey].EventLog[0].Sequence)
		}

		childEvents := g.Kinds[childKey].EventLog
		if childEvents[0].Sequence != 1 || childEvents[1].Sequence != 2 || childEvents[2].Sequence != 3 {
			t.Error("Child events have incorrect sequences")
		}
	})
}

func TestGlobalKnowledge_replayEventsToState(t *testing.T) {
	t.Skip("skipping test for now - we need to refactor this code but there isnt time right now")
	events := []event.Event{
		{
			ID:           "evt-00",
			Timestamp:    "2024-02-21T10:00:00Z",
			ReconcileID:  "external",
			ControllerID: "TraceyWebhook",
			OpType:       "CREATE",
			Kind:         "Deployment",
			ObjectID:     "dep-1",
			Labels: map[string]string{
				tag.TraceyWebhookLabel: "root-1",
			},
		},
		{
			ID:           "evt-001",
			Timestamp:    "2024-02-21T10:00:00Z",
			ReconcileID:  "external",
			ControllerID: "TraceyWebhook",
			OpType:       "CREATE",
			Kind:         "Deployment",
			ObjectID:     "dep-2",
			Labels: map[string]string{
				tag.TraceyWebhookLabel: "root-2",
			},
		},
		{
			ID:           "evt-01",
			Timestamp:    "2024-02-21T10:00:01Z",
			ReconcileID:  "r0",
			ControllerID: "pod-controller",
			OpType:       "GET",
			Kind:         "Deployment",
			ObjectID:     "dep-1",
			Labels: map[string]string{
				tag.TraceyWebhookLabel: "root-1",
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
				tag.TraceyWebhookLabel: "root-2",
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
				tag.TraceyRootID: "root-1",
				tag.ChangeID:     "change-1",
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
				tag.TraceyRootID: "root-2",
				tag.ChangeID:     "change-2",
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
				tag.TraceyRootID: "root-1",
				tag.ChangeID:     "change-1",
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
				tag.TraceyRootID: "root-2",
				tag.ChangeID:     "change-2",
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
				tag.TraceyRootID: "root-1",
				tag.ChangeID:     "change-1a",
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
				tag.TraceyRootID: "root-2",
				tag.ChangeID:     "change-2a",
			},
		},
		// REMOVE pods
		{
			ID:           "evt-7",
			Timestamp:    "2024-02-21T10:00:09Z",
			ReconcileID:  "r3",
			ControllerID: "pod-controller",
			OpType:       "GET",
			Kind:         "Pod",
			ObjectID:     "pod-1",
			Labels: map[string]string{
				tag.TraceyRootID: "root-1",
				tag.ChangeID:     "change-1a",
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
				tag.TraceyRootID: "root-1",
				// tag.ChangeID:     "change-1a",
				tag.DeletionID: "del-1",
			},
		},
		{
			ID:           "evt-8a",
			Timestamp:    "2024-02-21T10:00:10Z",
			ReconcileID:  "r3",
			ControllerID: "api-server",
			OpType:       "REMOVE",
			Kind:         "Pod",
			ObjectID:     "pod-1",
			Labels: map[string]string{
				tag.TraceyRootID: "root-1",
				// tag.ChangeID:     "change-1a",
				tag.DeletionID: "del-1",
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
				tag.TraceyRootID: "root-2",
				tag.ChangeID:     "change-2a",
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
				tag.TraceyRootID: "root-2",
				// tag.ChangeID:     "change-2a",
				tag.DeletionID: "del-2",
			},
		},
		{
			ID:           "evt-10a",
			Timestamp:    "2024-02-21T10:00:14Z",
			ReconcileID:  "r4",
			ControllerID: "pod-controller",
			OpType:       "REMOVE",
			Kind:         "Pod",
			ObjectID:     "pod-2",
			Labels: map[string]string{
				tag.TraceyRootID: "root-2",
				// tag.ChangeID:     "change-2a",
				tag.DeletionID: "del-2",
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
			compositeKey("Deployment", "default", "dep-1", "dep-1"),
			compositeKey("Deployment", "default", "dep-2", "dep-2"),
			// Pod 1 was REMOVEd
			compositeKey("Pod", "default", "pod-2", "pod-2"),
		}
		for _, key := range expectedKeys {
			if _, exists := state.All()[key]; !exists {
				t.Errorf("Expected %s in state", key)
			}
		}
		// check kind sequence
		if state.KindSequences[appsDeployment] != 2 {
			t.Errorf("Expected Deployment sequence 2, got %d", state.KindSequences[appsDeployment])
		}
		if state.KindSequences[corePod] != 6 {
			t.Errorf("Expected Pod sequence 6, got %d", state.KindSequences[corePod])
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
		if _, exists := state.All()[compositeKey("Pod", "default", "pod-1", "pod-1")]; !exists {
			t.Error("Expected pod-1 in state")
		}
		if _, exists := state.All()[compositeKey("Pod", "default", "pod-2", "pod-2")]; !exists {
			t.Error("Expected pod-2 in state")
		}
		// check kind sequence
		if state.KindSequences[corePod] != 2 {
			t.Errorf("Expected Pod sequence 2, got %d", state.KindSequences[corePod])
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
		rewind, err := g.AdjustKnowledgeForResourceType(state, corePod, -1)
		if err != nil {
			t.Fatalf("AdjustKnowledgeForKind failed: %v", err)
		}
		if countPods(rewind) != 2 {
			t.Errorf("Expected 2 pods in state, got %d", len(rewind.All()))
		}

		// advance forward to apply the deletion as well as the removal
		ff, err := g.AdjustKnowledgeForResourceType(state, corePod, 2)
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
		expectedSeq   KindSequences
	}{
		{
			name: "single create event",
			events: []StateEvent{
				{
					ReconcileID: "r1",
					Timestamp:   "2024-02-21T10:00:01Z",
					Effect: Effect{
						Key:     compositeKey("Pod", "default", "pod-1", "pod-1"),
						Version: snapshot.NewDefaultHash("v1"),
						OpType:  event.CREATE,
					},
					Sequence: 1,
				},
			},
			expectedState: map[snapshot.CompositeKey]snapshot.VersionHash{
				compositeKey("Pod", "default", "pod-1", "pod-1"): snapshot.NewDefaultHash("v1"),
			},
			expectedSeq: KindSequences{corePod: 1},
		},
		{
			name: "create and deleteevent",
			events: []StateEvent{
				{
					ReconcileID: "r1",
					Timestamp:   "2024-02-21T10:00:01Z",
					Effect: Effect{
						Key:     compositeKey("Pod", "default", "pod-1", "pod-1"),
						Version: snapshot.NewDefaultHash("v1"),
						OpType:  event.CREATE,
					},
					Sequence: 1,
				},
				{
					ReconcileID: "r2",
					Timestamp:   "2024-02-21T10:00:02Z",
					Effect: Effect{
						Key:    compositeKey("Pod", "default", "pod-1", "pod-1"),
						OpType: event.MARK_FOR_DELETION,
					},
					Sequence: 2,
				},
				{
					ReconcileID: "r3",
					Timestamp:   "2024-02-21T10:00:03Z",
					Effect: Effect{
						Key:    compositeKey("Pod", "default", "pod-1", "pod-1"),
						OpType: event.REMOVE,
					},
					Sequence: 3,
				},
			},
			expectedState: map[snapshot.CompositeKey]snapshot.VersionHash{},
			expectedSeq:   KindSequences{corePod: 3},
		},
		{
			name: "multiple kinds",
			events: []StateEvent{
				{
					ReconcileID: "r1",
					Timestamp:   "2024-02-21T10:00:01Z",
					Effect: Effect{
						Key:     compositeKey("Pod", "default", "pod-1", "pod-1"),
						Version: snapshot.NewDefaultHash("v1"),
						OpType:  event.CREATE,
					},
					Sequence: 1,
				},
				{
					ReconcileID: "r2",
					Timestamp:   "2024-02-21T10:00:02Z",
					Effect: Effect{
						Key:     compositeKey("Service", "default", "svc-1", "svc-1"),
						Version: snapshot.NewDefaultHash("v2"),
						OpType:  event.CREATE,
					},
					Sequence: 2,
				},
			},
			expectedState: map[snapshot.CompositeKey]snapshot.VersionHash{
				compositeKey("Pod", "default", "pod-1", "pod-1"):     snapshot.NewDefaultHash("v1"),
				compositeKey("Service", "default", "svc-1", "svc-1"): snapshot.NewDefaultHash("v2"),
			},
			expectedSeq: KindSequences{corePod: 1,
				"Service": 2,
			},
		},
		{
			name: "update event",
			events: []StateEvent{
				{
					ReconcileID: "r1",
					Timestamp:   "2024-02-21T10:00:01Z",
					Effect: Effect{
						Key:     compositeKey("Pod", "default", "pod-1", "pod-1"),
						Version: snapshot.NewDefaultHash("v1"),
						OpType:  event.CREATE,
					},
					Sequence: 1,
				},
				{
					ReconcileID: "r2",
					Timestamp:   "2024-02-21T10:00:02Z",
					Effect: Effect{
						Key:     compositeKey("Pod", "default", "pod-1", "pod-1"),
						Version: snapshot.NewDefaultHash("v2"),
						OpType:  event.UPDATE,
					},
					Sequence: 2,
				},
			},
			expectedState: map[snapshot.CompositeKey]snapshot.VersionHash{
				compositeKey("Pod", "default", "pod-1", "pod-1"): snapshot.NewDefaultHash("v2"),
			},
			expectedSeq: KindSequences{corePod: 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := replayEventSequenceToState(tt.events)

			assert.Equal(t, tt.expectedState, state.All())
			assert.Equal(t, canonicalizeSeq(tt.expectedSeq), state.KindSequences)
			assert.Equal(t, tt.events, state.stateEvents)
		})
	}
}

func TestReplyaEventsAtSequence_DeletionSemantics(t *testing.T) {
	// helper to create state events
	var reconcileInt int = 1
	newStateEvent := func(kind, name, version string, op event.OperationType, sequence int64) StateEvent {
		s := StateEvent{
			ReconcileID: fmt.Sprintf("r%d", reconcileInt),
			Timestamp:   fmt.Sprintf("t%d", reconcileInt),
			Effect: Effect{
				Key:     compositeKey(kind, "default", name, name),
				Version: snapshot.NewDefaultHash(version),
				OpType:  op,
			},
			Sequence: sequence,
		}
		reconcileInt++
		return s
	}
	events := []StateEvent{
		newStateEvent("Pod", "pod-1", "v1", event.CREATE, 1),
		newStateEvent("Pod", "pod-1", "v2", event.MARK_FOR_DELETION, 2),
		newStateEvent("Pod", "pod-2", "v1", event.CREATE, 3),
		newStateEvent("Pod", "pod-1", "v2", event.REMOVE, 4),
		// illegal event, pod-2 was not marked for deletion
		newStateEvent("Pod", "pod-2", "v1", event.REMOVE, 5),
	}
	testCases := []struct {
		name          string
		sequences     KindSequences
		expectedState ObjectVersions
		expectError   bool
	}{
		{
			name:      "pod-1 fully deleted",
			sequences: KindSequences{corePod: 4},
			expectedState: ObjectVersions{
				compositeKey("Pod", "default", "pod-2", "pod-2"): snapshot.NewDefaultHash("v1"),
			},
		},
		{
			name:      "pod-1 marked for deletion",
			sequences: KindSequences{corePod: 3},
			expectedState: ObjectVersions{
				compositeKey("Pod", "default", "pod-1", "pod-1"): snapshot.NewDefaultHash("v2"),
				compositeKey("Pod", "default", "pod-2", "pod-2"): snapshot.NewDefaultHash("v1"),
			},
		},
		{
			name:        "pod-2 not marked for deletion",
			sequences:   KindSequences{corePod: 5},
			expectError: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					if !tc.expectError {
						t.Errorf("Unexpected panic: %v", r)
					}
				} else if tc.expectError {
					t.Error("Expected a panic but did not get one")
				}
			}()
			state := replayEventsAtSequence(events, canonicalizeSeq(tc.sequences))
			for k, v := range state.All() {
				assert.Equal(t, v, tc.expectedState[k])
			}
		})
	}
}

func TestReplayEventsAtSequence(t *testing.T) {
	// Helper to create state events
	var reconcileInt int = 1
	newStateEvent := func(kind, name, version string, op event.OperationType, sequence int64) StateEvent {
		s := StateEvent{
			ReconcileID: fmt.Sprintf("r%d", reconcileInt),
			Timestamp:   fmt.Sprintf("t%d", reconcileInt),
			Effect: Effect{
				Key:     compositeKey(kind, "default", name, name),
				Version: snapshot.NewDefaultHash(version),
				OpType:  op,
			},
			Sequence: sequence,
		}
		reconcileInt++
		return s
	}

	events := []StateEvent{
		newStateEvent("Pod", "pod-1", "v1", event.CREATE, 1),
		newStateEvent("Pod", "pod-1", "v2", event.UPDATE, 2),
		newStateEvent("Pod", "pod-1", "v3", event.MARK_FOR_DELETION, 3),
		newStateEvent("Pod", "pod-2", "v1", event.CREATE, 4),
		newStateEvent("Pod", "pod-2", "v2", event.UPDATE, 5),
		newStateEvent("Pod", "pod-2", "v3", event.MARK_FOR_DELETION, 6),
		newStateEvent("Service", "svc-1", "v1", event.CREATE, 7),
		newStateEvent("Service", "svc-1", "v2", event.UPDATE, 8),
		newStateEvent("Service", "svc-1", "v3", event.MARK_FOR_DELETION, 9),
		newStateEvent("Service", "svc-2", "v1", event.CREATE, 10),
		newStateEvent("Service", "svc-2", "v2", event.UPDATE, 11),
		newStateEvent("Service", "svc-2", "v3", event.MARK_FOR_DELETION, 12),
	}

	tests := []struct {
		name            string
		sequencesByKind KindSequences
		expectedState   ObjectVersions
		expectedSeq     KindSequences
	}{
		{
			name: "initial state",
			sequencesByKind: KindSequences{corePod: 0,
				"Service": 0,
			},
			expectedState: ObjectVersions{},
			expectedSeq:   KindSequences{},
		},
		{
			name: "after first pod create",
			sequencesByKind: KindSequences{corePod: 1,
				"Service": 0,
			},
			expectedState: ObjectVersions{
				compositeKey("Pod", "default", "pod-1", "pod-1"): snapshot.NewDefaultHash("v1"),
			},
			expectedSeq: KindSequences{corePod: 1},
		},
		{
			name: "after first pod update",
			sequencesByKind: KindSequences{corePod: 2,
				"Service": 0,
			},
			expectedState: ObjectVersions{
				compositeKey("Pod", "default", "pod-1", "pod-1"): snapshot.NewDefaultHash("v2"),
			},
			expectedSeq: KindSequences{corePod: 2},
		},
		{
			name: "after first pod marked for deletion",
			sequencesByKind: KindSequences{corePod: 3,
				"Service": 0,
			},
			expectedState: ObjectVersions{
				// marked for deletion, but still here
				compositeKey("Pod", "default", "pod-1", "pod-1"): snapshot.NewDefaultHash("v3"),
			},
			expectedSeq: KindSequences{corePod: 3},
		},
		{
			name: "after second pod create",
			sequencesByKind: KindSequences{corePod: 4,
				"Service": 0,
			},
			expectedState: ObjectVersions{
				compositeKey("Pod", "default", "pod-1", "pod-1"): snapshot.NewDefaultHash("v3"),
				compositeKey("Pod", "default", "pod-2", "pod-2"): snapshot.NewDefaultHash("v1"),
			},
			expectedSeq: KindSequences{corePod: 4},
		},
		{
			name: "after second pod update",
			sequencesByKind: KindSequences{corePod: 5,
				"Service": 0,
			},
			expectedState: ObjectVersions{
				compositeKey("Pod", "default", "pod-1", "pod-1"): snapshot.NewDefaultHash("v3"),
				compositeKey("Pod", "default", "pod-2", "pod-2"): snapshot.NewDefaultHash("v2"),
			},
			expectedSeq: KindSequences{corePod: 5},
		},
		{
			name: "after second pod marked for deletion",
			sequencesByKind: KindSequences{corePod: 6,
				"Service": 0,
			},
			expectedState: ObjectVersions{
				compositeKey("Pod", "default", "pod-1", "pod-1"): snapshot.NewDefaultHash("v3"),
				compositeKey("Pod", "default", "pod-2", "pod-2"): snapshot.NewDefaultHash("v3"),
			},
			expectedSeq: KindSequences{corePod: 6},
		},
		{
			name: "after first service create",
			sequencesByKind: KindSequences{corePod: 1,
				"Service": 7,
			},
			expectedState: ObjectVersions{
				compositeKey("Pod", "default", "pod-1", "pod-1"):     snapshot.NewDefaultHash("v1"),
				compositeKey("Service", "default", "svc-1", "svc-1"): snapshot.NewDefaultHash("v1"),
			},
			expectedSeq: KindSequences{corePod: 1,
				"Service": 7,
			},
		},
		{
			name: "multi object update",
			sequencesByKind: KindSequences{corePod: 2,
				"Service": 8,
			},
			expectedState: ObjectVersions{
				compositeKey("Pod", "default", "pod-1", "pod-1"):     snapshot.NewDefaultHash("v2"),
				compositeKey("Service", "default", "svc-1", "svc-1"): snapshot.NewDefaultHash("v2"),
			},
			expectedSeq: KindSequences{corePod: 2,
				"Service": 8,
			},
		},
		{
			name: "after second service create",
			sequencesByKind: KindSequences{corePod: 2,
				"Service": 10,
			},
			expectedState: ObjectVersions{
				compositeKey("Pod", "default", "pod-1", "pod-1"): snapshot.NewDefaultHash("v2"),
				// not deleted yet
				compositeKey("Service", "default", "svc-1", "svc-1"): snapshot.NewDefaultHash("v3"),
				compositeKey("Service", "default", "svc-2", "svc-2"): snapshot.NewDefaultHash("v1"),
			},
			expectedSeq: KindSequences{corePod: 2,
				"Service": 10,
			},
		},
		{
			name: "after second service update",
			sequencesByKind: KindSequences{corePod: 2,
				"Service": 11,
			},
			expectedState: ObjectVersions{
				compositeKey("Pod", "default", "pod-1", "pod-1"):     snapshot.NewDefaultHash("v2"),
				compositeKey("Service", "default", "svc-1", "svc-1"): snapshot.NewDefaultHash("v3"),
				compositeKey("Service", "default", "svc-2", "svc-2"): snapshot.NewDefaultHash("v2"),
			},
			expectedSeq: KindSequences{corePod: 2,
				"Service": 11,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := replayEventsAtSequence(events, canonicalizeSeq(tt.sequencesByKind))

			assert.Equal(t, tt.expectedState, state.All())
			assert.Equal(t, canonicalizeSeq(tt.expectedSeq), state.KindSequences)
		})
	}
}

func TestGetAllPossibleViewsWithKindBounds(t *testing.T) {
	// Helper to create state events
	var reconcileInt int = 1
	newStateEvent := func(kind, name, version string, op event.OperationType) StateEvent {
		s := StateEvent{
			ReconcileID: fmt.Sprintf("r%d", reconcileInt),
			Timestamp:   fmt.Sprintf("t%d", reconcileInt),
			Effect: Effect{
				Key:     compositeKey(kind, "default", name, name),
				Version: snapshot.NewDefaultHash(version),
				OpType:  op,
			},
			Sequence: int64(reconcileInt),
		}
		reconcileInt++
		return s
	}

	// Define reusable keys and values
	pod1Key := compositeKey("Pod", "default", "pod-1", "pod-1")
	pod2Key := compositeKey("Pod", "default", "pod-2", "pod-2")
	svc1Key := compositeKey("Service", "default", "svc-1", "svc-1")
	svc2Key := compositeKey("Service", "default", "svc-2", "svc-2")
	v1 := snapshot.NewDefaultHash("v1")
	v2 := snapshot.NewDefaultHash("v2")

	// Create events
	events := []StateEvent{
		newStateEvent("Pod", "pod-1", "v1", event.CREATE),
		newStateEvent("Pod", "pod-1", "v2", event.UPDATE),
		newStateEvent("Pod", "pod-2", "v1", event.CREATE),
		newStateEvent("Pod", "pod-2", "v2", event.UPDATE),
		newStateEvent("Service", "svc-1", "v1", event.CREATE),
		newStateEvent("Service", "svc-1", "v2", event.UPDATE),
		newStateEvent("Service", "svc-2", "v1", event.CREATE),
		newStateEvent("Service", "svc-2", "v2", event.UPDATE),
	}

	// Initial state
	state := &StateSnapshot{
		contents: ObjectVersions{
			pod1Key: v2, pod2Key: v2,
			svc1Key: v2, svc2Key: v2,
		},
		KindSequences: KindSequences{corePod: 4, coreService: 8},
		stateEvents:   events,
	}

	tests := []struct {
		name           string
		kindBounds     LookbackLimits
		expectedStates []struct {
			versions ObjectVersions
			seqs     KindSequences
		}
	}{
		{
			name: "no bounds",
			kindBounds: LookbackLimits{
				corePod:     NoLimit,
				coreService: NoLimit,
			},
			expectedStates: []struct {
				versions ObjectVersions
				seqs     KindSequences
			}{
				{
					versions: ObjectVersions{pod1Key: v1, svc1Key: v1},
					seqs:     KindSequences{corePod: 1, coreService: 5},
				},
				{
					versions: ObjectVersions{pod1Key: v1, svc1Key: v2},
					seqs:     KindSequences{corePod: 1, coreService: 6},
				},
				{
					versions: ObjectVersions{pod1Key: v1, svc1Key: v2, svc2Key: v1},
					seqs:     KindSequences{corePod: 1, coreService: 7},
				},
				{
					versions: ObjectVersions{pod1Key: v1, svc1Key: v2, svc2Key: v2},
					seqs:     KindSequences{corePod: 1, coreService: 8},
				},
				{
					versions: ObjectVersions{pod1Key: v2, svc1Key: v1},
					seqs:     KindSequences{corePod: 2, coreService: 5},
				},
				{
					versions: ObjectVersions{pod1Key: v2, svc1Key: v2},
					seqs:     KindSequences{corePod: 2, coreService: 6},
				},
				{
					versions: ObjectVersions{pod1Key: v2, svc1Key: v2, svc2Key: v1},
					seqs:     KindSequences{corePod: 2, coreService: 7},
				},
				{
					versions: ObjectVersions{pod1Key: v2, svc1Key: v2, svc2Key: v2},
					seqs:     KindSequences{corePod: 2, coreService: 8},
				},
				{
					versions: ObjectVersions{pod1Key: v2, pod2Key: v1, svc1Key: v1},
					seqs:     KindSequences{corePod: 3, coreService: 5},
				},
				{
					versions: ObjectVersions{pod1Key: v2, pod2Key: v1, svc1Key: v2},
					seqs:     KindSequences{corePod: 3, coreService: 6},
				},
				{
					versions: ObjectVersions{pod1Key: v2, pod2Key: v1, svc1Key: v2, svc2Key: v1},
					seqs:     KindSequences{corePod: 3, coreService: 7},
				},
				{
					versions: ObjectVersions{pod1Key: v2, pod2Key: v1, svc1Key: v2, svc2Key: v2},
					seqs:     KindSequences{corePod: 3, coreService: 8},
				},
				{
					versions: ObjectVersions{pod1Key: v2, pod2Key: v2, svc1Key: v1},
					seqs:     KindSequences{corePod: 4, coreService: 5},
				},
				{
					versions: ObjectVersions{pod1Key: v2, pod2Key: v2, svc1Key: v2},
					seqs:     KindSequences{corePod: 4, coreService: 6},
				},
				{
					versions: ObjectVersions{pod1Key: v2, pod2Key: v2, svc1Key: v2, svc2Key: v1},
					seqs:     KindSequences{corePod: 4, coreService: 7},
				},
				{
					versions: ObjectVersions{pod1Key: v2, pod2Key: v2, svc1Key: v2, svc2Key: v2},
					seqs:     KindSequences{corePod: 4, coreService: 8},
				},
			},
		},
		{
			name: "limit = 2 for Pods and Services",
			kindBounds: LookbackLimits{
				corePod:     2,
				coreService: 2,
			},
			expectedStates: []struct {
				versions ObjectVersions
				seqs     KindSequences
			}{
				{
					versions: ObjectVersions{pod1Key: v2, pod2Key: v1, svc1Key: v2, svc2Key: v1},
					seqs:     KindSequences{corePod: 3, coreService: 7},
				},
				{
					versions: ObjectVersions{pod1Key: v2, pod2Key: v1, svc1Key: v2, svc2Key: v2},
					seqs:     KindSequences{corePod: 3, coreService: 8},
				},
				{
					versions: ObjectVersions{pod1Key: v2, pod2Key: v2, svc1Key: v2, svc2Key: v1},
					seqs:     KindSequences{corePod: 4, coreService: 7},
				},
				{
					versions: ObjectVersions{pod1Key: v2, pod2Key: v2, svc1Key: v2, svc2Key: v2},
					seqs:     KindSequences{corePod: 4, coreService: 8},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			staleViews := getAllPossibleViews(state, []string{corePod, coreService}, canonicalLookbackLimits(tt.kindBounds))

			assert.Equal(t, len(tt.expectedStates), len(staleViews))
			for _, expected := range tt.expectedStates {
				found := false
				for _, view := range staleViews {
					if assert.ObjectsAreEqual(expected.versions, view.Observable()) &&
						assert.ObjectsAreEqual(expected.seqs, view.KindSequences) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Expected state %+v not found in stale views", expected)
				}
			}
		})
	}
}
func Test_getAllCombos(t *testing.T) {
	values := map[string][]int64{
		"a": {1, 2, 3},
		"b": {10, 20},
	}

	combos := getAllCombos(values)
	expected := []KindSequences{
		{"a": 1, "b": 10},
		{"a": 1, "b": 20},
		{"a": 2, "b": 10},
		{"a": 2, "b": 20},
		{"a": 3, "b": 10},
		{"a": 3, "b": 20},
	}
	assert.ElementsMatch(t, expected, combos)
}
func TestLimitEventHistory(t *testing.T) {
	tests := []struct {
		name           string
		seqByKind      map[string][]int64
		limit          LookbackLimits
		expectedResult map[string][]int64
	}{
		{
			name: "no limits applied",
			seqByKind: map[string][]int64{
				"Pod":     {1, 2, 3, 4},
				"Service": {1, 2, 3},
			},
			limit: nil,
			expectedResult: map[string][]int64{
				"Pod":     {1, 2, 3, 4},
				"Service": {1, 2, 3},
			},
		},
		{
			name: "limit applied to one kind",
			seqByKind: map[string][]int64{
				"Pod":     {1, 2, 3, 4},
				"Service": {1, 2, 3},
			},
			limit: LookbackLimits{
				"Pod": 2,
			},
			expectedResult: map[string][]int64{
				"Pod":     {3, 4},
				"Service": {1, 2, 3},
			},
		},
		{
			name: "limit applied to multiple kinds",
			seqByKind: map[string][]int64{
				"Pod":     {1, 2, 3, 4},
				"Service": {1, 2, 3},
			},
			limit: LookbackLimits{
				"Pod":     2,
				"Service": 1,
			},
			expectedResult: map[string][]int64{
				"Pod":     {3, 4},
				"Service": {3},
			},
		},
		{
			name: "limit exceeds sequence length",
			seqByKind: map[string][]int64{
				"Pod":     {1, 2},
				"Service": {1},
			},
			limit: LookbackLimits{
				"Pod":     5,
				"Service": 3,
			},
			expectedResult: map[string][]int64{
				"Pod":     {1, 2},
				"Service": {1},
			},
		},
		{
			name: "limit is zero",
			seqByKind: map[string][]int64{
				"Pod":     {1, 2},
				"Service": {1},
			},
			limit: LookbackLimits{
				"Pod":     0,
				"Service": 0,
			},
			expectedResult: map[string][]int64{
				"Pod":     {1, 2},
				"Service": {1},
			},
		},
		{
			name: "empty sequences",
			seqByKind: map[string][]int64{
				"Pod":     {},
				"Service": {},
			},
			limit: LookbackLimits{
				"Pod":     2,
				"Service": 1,
			},
			expectedResult: map[string][]int64{
				"Pod":     {},
				"Service": {},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := limitEventHistory(canonicalSeqSlices(tt.seqByKind), canonicalLookbackLimits(tt.limit))
			assert.Equal(t, canonicalSeqSlices(tt.expectedResult), result)
		})
	}
}
func TestFilterEventsAtSequence(t *testing.T) {
	// Helper to create state events
	var reconcileInt int = 1
	newStateEvent := func(kind, name, version string, op event.OperationType, sequence int64) StateEvent {
		s := StateEvent{
			ReconcileID: fmt.Sprintf("r%d", reconcileInt),
			Timestamp:   fmt.Sprintf("t%d", reconcileInt),
			Effect: Effect{
				Key:     compositeKey(kind, "default", name, name),
				Version: snapshot.NewDefaultHash(version),
				OpType:  op,
			},
			Sequence: sequence,
		}
		reconcileInt++
		return s
	}

	events := []StateEvent{
		newStateEvent("Pod", "pod-1", "v1", event.CREATE, 1),
		newStateEvent("Pod", "pod-1", "v2", event.UPDATE, 2),
		newStateEvent("Pod", "pod-2", "v1", event.CREATE, 3),
		newStateEvent("Service", "svc-1", "v1", event.CREATE, 4),
		newStateEvent("Service", "svc-1", "v2", event.UPDATE, 5),
		newStateEvent("Service", "svc-2", "v1", event.CREATE, 6),
	}

	tests := []struct {
		name            string
		sequencesByKind KindSequences
		expectedEvents  []StateEvent
		expectPanic     bool
	}{
		{
			name: "filter events for Pod up to sequence 2",
			sequencesByKind: KindSequences{corePod: 2,
				coreService: 6,
			},
			expectedEvents: []StateEvent{
				events[0],
				events[1],
				events[3],
				events[4],
				events[5],
			},
		},
		{
			name: "filter events for Service up to sequence 5",
			sequencesByKind: KindSequences{corePod: 3,
				coreService: 5,
			},
			expectedEvents: []StateEvent{
				events[0],
				events[1],
				events[2],
				events[3],
				events[4],
			},
		},
		{
			name: "filter events for both Pod and Service",
			sequencesByKind: KindSequences{corePod: 2,
				coreService: 5,
			},
			expectedEvents: []StateEvent{
				events[0],
				events[1],
				events[3],
				events[4],
			},
		},
		{
			name:            "no sequence for a kind",
			sequencesByKind: KindSequences{corePod: 2},
			expectPanic:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					if !tt.expectPanic {
						t.Errorf("Unexpected panic: %v", r)
					}
				} else if tt.expectPanic {
					t.Error("Expected a panic but did not get one")
				}
			}()

			filteredEvents := filterEventsAtSequence(events, canonicalizeSeq(tt.sequencesByKind))
			assert.Equal(t, tt.expectedEvents, filteredEvents)
		})
	}
}
