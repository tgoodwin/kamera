package tracecheck

import (
	"fmt"
	"time"

	"github.com/samber/lo"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
	scheme       *runtime.Scheme
	reconcileIDs map[string]int
}

func NewStateEventBuilder(store *snapshot.Store, scheme *runtime.Scheme) *StateEventBuilder {
	return &StateEventBuilder{
		store:        store,
		events:       []StateEvent{},
		sequence:     0,
		currentTime:  time.Now(),
		scheme:       scheme,
		reconcileIDs: make(map[string]int),
	}
}

// AddStateEvent adds a new state event with a specific object
func (b *StateEventBuilder) AddStateEvent(kind, sleeveObjectID string, obj *unstructured.Unstructured,
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
	gvk := util.GetGroupVersionKind(obj)
	key := snapshot.NewCompositeKeyWithGroup(gvk.Group, kind, obj.GetNamespace(), obj.GetName(), sleeveObjectID)
	effect := newEffect(key, versionHash, opType)

	// Increment sequence
	b.sequence++

	// Create state event
	stateEvent := StateEvent{
		// TODO refactor
		Event: &event.Event{
			APIVersion:   gvk.GroupVersion().String(),
			Group:        gvk.Group,
			Kind:         kind,
			ID:           fmt.Sprintf("%s-%d", kind, b.sequence),
			ReconcileID:  reconcileID,
			ControllerID: controllerID,
			RootEventID:  "",
			OpType:       string(opType),
			ObjectID:     sleeveObjectID,
			Version:      obj.GetResourceVersion(),
			Labels:       tag.GetSleeveLabels(obj),
			Timestamp:    timeStr,
		},
		ReconcileID: reconcileID,
		Timestamp:   timeStr,
		Effect:      effect,
		Sequence:    b.sequence,
	}

	b.events = append(b.events, stateEvent)
}

func (b *StateEventBuilder) Build() StateNode {
	snapshot := replayEventSequenceToState(b.events)
	pending := []PendingReconcile{}
	return StateNode{
		ID:                "TOP",
		Contents:          *snapshot,
		PendingReconciles: pending,
	}
}

func ensureObjectGVK(obj client.Object, scheme *runtime.Scheme) schema.GroupVersionKind {
	if obj == nil {
		return schema.GroupVersionKind{}
	}

	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Kind != "" && gvk.Version != "" && gvk.Group != "" {
		return gvk
	}

	if scheme != nil {
		if gvks, _, err := scheme.ObjectKinds(obj); err == nil {
			for _, candidate := range gvks {
				if candidate.Kind == "" || candidate.Version == "" {
					continue
				}
				obj.GetObjectKind().SetGroupVersionKind(candidate)
				return candidate
			}
		}
	}

	gvk = util.GetGroupVersionKind(obj)
	obj.GetObjectKind().SetGroupVersionKind(gvk)
	return gvk
}

func (b *StateEventBuilder) AddTopLevelObject(obj client.Object, dependentControllers ...string) StateNode {
	gvk := ensureObjectGVK(obj, b.scheme)

	r, err := snapshot.AsRecord(obj, "start")
	if err != nil {
		panic("converting to unstructured: " + err.Error())
	}
	u, err := r.ToUnstructured()
	if err != nil {
		panic("converting to unstructured: " + err.Error())
	}
	vHash := b.store.PublishWithStrategy(u, snapshot.AnonymizedHash)
	sleeveObjectID := tag.GetSleeveObjectID(obj)
	ikey := snapshot.IdentityKey{Group: gvk.Group, Kind: gvk.Kind, ObjectID: sleeveObjectID}

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

	key := snapshot.NewCompositeKeyWithGroup(gvk.Group, ikey.Kind, obj.GetNamespace(), obj.GetName(), sleeveObjectID)

	return StateNode{
		Contents: StateSnapshot{
			contents: ObjectVersions{key: vHash},
			KindSequences: KindSequences{
				util.CanonicalGroupKind(ikey.Group, ikey.Kind): 1,
			},
			stateEvents: []StateEvent{
				{
					ReconcileID: "TOP",
					Timestamp:   event.FormatTimeStr(time.Now()),
					Sequence:    1,
					Effect:      newEffect(key, vHash, event.CREATE),
				},
			},
		},
		PendingReconciles: dependent,
	}
}
