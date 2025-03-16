package tracecheck

import (
	"fmt"
	"time"

	"github.com/samber/lo"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"github.com/tgoodwin/sleeve/pkg/tag"
	"github.com/tgoodwin/sleeve/pkg/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// StateEventBuilder helps create a sequence of state events
type StateEventBuilder struct {
	store        *snapshot.Store
	events       []StateEvent
	sequence     int64
	currentTime  time.Time
	reconcileIDs map[string]int
}

func NewStateEventBuilder(store *snapshot.Store) *StateEventBuilder {
	return &StateEventBuilder{
		store:        store,
		events:       []StateEvent{},
		sequence:     0,
		currentTime:  time.Now(),
		reconcileIDs: make(map[string]int),
	}
}

// AddStateEvent adds a new state event with a specific object
func (b *StateEventBuilder) AddStateEvent(kind, objectID string, obj *unstructured.Unstructured,
	opType event.OperationType, controllerID string) {
	// Set unique reconcileID
	reconcileNum, ok := b.reconcileIDs[controllerID]
	if !ok {
		reconcileNum = 0
	}
	reconcileID := fmt.Sprintf("%s-%d", controllerID, reconcileNum)
	b.reconcileIDs[controllerID] = reconcileNum + 1

	// Capture time with small increments
	b.currentTime = b.currentTime.Add(1 * time.Second)
	timeStr := b.currentTime.Format(time.RFC3339)

	// Store object in snapshot store
	if obj != nil {
		if err := b.store.StoreObject(obj); err != nil {
			panic(fmt.Sprintf("Failed to store object: %v", err))
		}
	}

	// Create version hash
	var versionHash snapshot.VersionHash
	if obj != nil {
		vHash := b.store.PublishWithStrategy(obj, snapshot.AnonymizedHash)
		versionHash = vHash
	}

	// Create effect
	key := snapshot.NewCompositeKey(kind, obj.GetNamespace(), obj.GetName(), objectID)
	effect := newEffect(key, versionHash, opType)

	// Increment sequence
	b.sequence++

	// Create state event
	stateEvent := StateEvent{
		ReconcileID: reconcileID,
		Timestamp:   timeStr,
		effect:      effect,
		Sequence:    b.sequence,
	}

	b.events = append(b.events, stateEvent)
}

func (b *StateEventBuilder) Build() StateNode {
	snapshot := replayEventSequenceToState(b.events)
	pending := []PendingReconcile{}
	return StateNode{
		Contents:          *snapshot,
		PendingReconciles: pending,
	}
}

func (b *StateEventBuilder) AddTopLevelObject(obj client.Object, dependentControllers ...string) StateNode {
	r, err := snapshot.AsRecord(obj, "start")
	if err != nil {
		panic("converting to unstructured: " + err.Error())
	}
	u := r.ToUnstructured()
	vHash := b.store.PublishWithStrategy(u, snapshot.AnonymizedHash)
	sleeveObjectID := tag.GetSleeveObjectID(obj)
	ikey := snapshot.IdentityKey{Kind: util.GetKind(obj), ObjectID: sleeveObjectID}

	dependent := lo.Map(dependentControllers, func(s string, _ int) PendingReconcile {
		return PendingReconcile{
			ReconcilerID: s,
			Request: reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: obj.GetNamespace(),
					Name:      obj.GetName(),
				},
			},
		}
	})

	key := snapshot.NewCompositeKey(ikey.Kind, obj.GetNamespace(), obj.GetName(), sleeveObjectID)

	return StateNode{
		Contents: StateSnapshot{
			contents: ObjectVersions{key: vHash},
			KindSequences: map[string]int64{
				ikey.Kind: 1,
			},
			stateEvents: []StateEvent{
				{
					ReconcileID: "TOP",
					Timestamp:   event.FormatTimeStr(time.Now()),
					Sequence:    1,
					effect:      newEffect(key, vHash, event.CREATE),
				},
			},
		},
		PendingReconciles: dependent,
	}
}
