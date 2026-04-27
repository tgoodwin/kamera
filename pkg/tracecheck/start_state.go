package tracecheck

import (
	"fmt"
	"time"

	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/util"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// buildStartStateFromObjects constructs a StateNode from concrete objects and an initial pending list.
// It publishes the supplied objects into the provided snapshot store so downstream explorers can resolve them.
func buildStartStateFromObjects(store *snapshot.Store, scheme *runtime.Scheme, objs []client.Object, pending []PendingReconcile, resourceVersions map[snapshot.VersionHash]int64) (StateNode, error) {
	if len(objs) == 0 {
		return StateNode{}, fmt.Errorf("no objects supplied for start state")
	}

	contents := make(ObjectVersions, len(objs))
	kindSeq := make(KindSequences)
	stateEvents := make([]StateEvent, 0, len(objs))

	// First pass: assign deterministic UIDs to all objects.
	for idx, obj := range objs {
		if obj == nil {
			return StateNode{}, fmt.Errorf("object %d is nil", idx)
		}
		tag.EnsureDeterministicIdentity(obj)
	}
	// Second pass: fix up ownerReference UIDs to match the deterministic UIDs
	// assigned in the first pass. Workflow JSONs often specify ownerReferences
	// with empty UIDs since the real UID isn't known at authoring time.
	fixupOwnerReferenceUIDs(objs)

	// Use a deterministic starting timestamp and increment so sequences are strictly increasing.
	now := time.Now()
	for idx, obj := range objs {

		gvk := ensureObjectGVK(obj, scheme)
		u, err := util.ConvertToUnstructured(obj)
		if err != nil {
			return StateNode{}, fmt.Errorf("converting object %d to unstructured: %w", idx, err)
		}

		vHash := store.PublishWithStrategy(u, snapshot.AnonymizedHash)
		sleeveObjectID := tag.GetSleeveObjectID(obj)
		key := snapshot.NewCompositeKeyWithGroup(gvk.Group, gvk.Kind, obj.GetNamespace(), obj.GetName(), sleeveObjectID)

		sequence := int64(idx + 1)
		contents[key] = vHash
		kindSeq[util.CanonicalGroupKind(gvk.Group, gvk.Kind)] = sequence
		stateEvents = append(stateEvents, StateEvent{
			ReconcileID: "TOP",
			Timestamp:   event.FormatTimeStr(now.Add(time.Duration(idx) * time.Second)),
			Sequence:    sequence,
			Effect:      newEffect(key, vHash, event.CREATE),
		})
		if resourceVersions != nil {
			resourceVersions[vHash] = sequence
		}
	}

	snapshot := NewStateSnapshot(contents, kindSeq, stateEvents)
	return StateNode{
		Contents:          snapshot,
		PendingReconciles: pending,
	}, nil
}

// fixupOwnerReferenceUIDs patches ownerReference UIDs on objects whose owners
// are in the same set. After deterministic UIDs are assigned, any ownerReference
// with an empty or mismatched UID is corrected to point to the owner's actual UID.
func fixupOwnerReferenceUIDs(objs []client.Object) {
	// Build an index: (apiVersion, kind, name) -> UID
	type ownerKey struct {
		apiVersion string
		kind       string
		name       string
	}
	uidIndex := make(map[ownerKey]types.UID, len(objs))
	for _, obj := range objs {
		gvk := util.GetGroupVersionKind(obj)
		av := schema.GroupVersion{Group: gvk.Group, Version: gvk.Version}.String()
		uidIndex[ownerKey{apiVersion: av, kind: gvk.Kind, name: obj.GetName()}] = obj.GetUID()
	}

	for _, obj := range objs {
		refs := obj.GetOwnerReferences()
		if len(refs) == 0 {
			continue
		}
		changed := false
		for i, ref := range refs {
			key := ownerKey{apiVersion: ref.APIVersion, kind: ref.Kind, name: ref.Name}
			if ownerUID, ok := uidIndex[key]; ok && ref.UID != ownerUID {
				refs[i].UID = ownerUID
				changed = true
			}
		}
		if changed {
			obj.SetOwnerReferences(refs)
		}
	}
}
