package tracecheck

import (
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/util"
)

func replayEventSequenceToState(events []StateEvent) *StateSnapshot {
	contents := make(ObjectVersions)
	KindSequences := make(KindSequences)
	stateEvents := make([]StateEvent, 0)

	// track which objects were marked for deletion
	deletions := make(map[snapshot.ResourceKey]bool)
	// ensure we only keep a single entry per resource identity (group/kind/ns/name)
	resourceIndex := make(map[snapshot.ResourceKey]snapshot.CompositeKey)

	for _, e := range events {
		// ensure that we are only applying write ops
		// if !event.IsWriteOp(e.Effect.OpType) {
		// 	continue
		// }
		iKey := e.Effect.Key.IdentityKey

		resourceKey := e.Effect.Key.ResourceKey
		if existingKey, ok := resourceIndex[resourceKey]; ok && existingKey != e.Effect.Key {
			delete(contents, existingKey)
		}

		switch e.Effect.OpType {
		case event.MARK_FOR_DELETION:
			deletions[resourceKey] = true
			version := e.Effect.Version
			contents[e.Effect.Key] = version
			resourceIndex[resourceKey] = e.Effect.Key
		case event.REMOVE:
			if _, wasMarkedForDeletion := deletions[resourceKey]; !wasMarkedForDeletion {
				// special case, we treat GarbageCollector REMOVEs as a DELETE + REMOVE
				// since it cleans up things that nobody explicitly marked for deletion
				isGarbageCollector := e.Event != nil && e.Event.ControllerID == util.GarbageCollectorName
				if !isGarbageCollector {
					logger.WithValues(
						"Key", resourceKey,
					).Error(nil, "attempting to remove an object that was not marked for deletion first")
					panic("removing object that was not marked for deletion")
				}
			}
			if existingKey, ok := resourceIndex[resourceKey]; ok {
				delete(contents, existingKey)
			} else {
				delete(contents, e.Effect.Key)
			}
			delete(resourceIndex, resourceKey)
			delete(deletions, resourceKey)
		case event.CREATE, event.UPDATE, event.PATCH:
			version := e.Effect.Version
			contents[e.Effect.Key] = version
			resourceIndex[resourceKey] = e.Effect.Key

		case event.GET, event.LIST:
			contents[e.Effect.Key] = e.Effect.Version
			resourceIndex[resourceKey] = e.Effect.Key
		default:
			panic("unknown op type")
		}

		KindSequences[iKey.CanonicalGroupKind()] = e.Sequence
		stateEvents = append(stateEvents, e)
	}
	out := NewStateSnapshot(contents, KindSequences, stateEvents)
	return &out
}
