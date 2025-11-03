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
	deletions := make(map[snapshot.CompositeKey]bool)

	for _, e := range events {
		// ensure that we are only applying write ops
		// if !event.IsWriteOp(e.Effect.OpType) {
		// 	continue
		// }
		iKey := e.Effect.Key.IdentityKey
		// if _, wasMarkedForDeletion := deletions[e.Effect.Key]; wasMarkedForDeletion {
		// 	// if the object was deleted, don't need to apply any more changes.
		// 	// TODO its unclear what to do when we observe update events after
		// 	// a deletion event. For now, ignore them.
		// 	if e.Effect.OpType != event.REMOVE {
		// 		if e.Effect.OpType == event.MARK_FOR_DELETION {
		// 			logger.V(2).Info("object being marked for deletion again", "Key", iKey)
		// 			continue
		// 		}
		// 		continue
		// 	}
		// }

		switch e.Effect.OpType {
		case event.MARK_FOR_DELETION:
			deletions[e.Effect.Key] = true
			version := e.Effect.Version
			contents[e.Effect.Key] = version
		case event.REMOVE:
			if _, wasMarkedForDeletion := deletions[e.Effect.Key]; !wasMarkedForDeletion {
				// special case, we treat GarbageCollector REMOVEs as a DELETE + REMOVE
				// since it cleans up things that nobody explicitly marked for deletion
				isGarbageCollector := e.Event != nil && e.Event.ControllerID == util.GarbageCollectorName
				if !isGarbageCollector {
					logger.WithValues(
						"Key", e.Effect.Key.ResourceKey,
					).Error(nil, "attempting to remove an object that was not marked for deletion first")
					panic("removing object that was not marked for deletion")
				}
			}
			delete(contents, e.Effect.Key)
		case event.CREATE, event.UPDATE, event.PATCH:
			version := e.Effect.Version
			contents[e.Effect.Key] = version

		case event.GET, event.LIST:
			contents[e.Effect.Key] = e.Effect.Version
		default:
			panic("unknown op type")
		}

		KindSequences[iKey.CanonicalGroupKind()] = e.Sequence
		stateEvents = append(stateEvents, e)
	}
	out := NewStateSnapshot(contents, KindSequences, stateEvents)
	return &out
}
