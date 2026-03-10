package tracecheck

import (
	"fmt"
	"time"

	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/util"
	"k8s.io/apimachinery/pkg/runtime"
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

	// Use a deterministic starting timestamp and increment so sequences are strictly increasing.
	now := time.Now()
	for idx, obj := range objs {
		if obj == nil {
			return StateNode{}, fmt.Errorf("object %d is nil", idx)
		}
		tag.EnsureDeterministicIdentity(obj)

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
