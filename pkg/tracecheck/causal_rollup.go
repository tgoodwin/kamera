package tracecheck

import (
	"fmt"

	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/util"
)

func replayCausalEventSequenceToState(events []StateEvent) *StateSnapshot {
	contents := make(ObjectVersions)
	KindSequences := make(KindSequences)
	stateEvents := make([]StateEvent, 0)

	var globalRV int64 = 0

	// track which objects were marked for deletion
	// TODO this is unused
	deletions := make(map[snapshot.CompositeKey]bool)

	// track which objects were actually deleted
	// TODO this is unused
	removals := make(map[snapshot.CompositeKey]bool)

	removalChangeIDs := make(map[event.ChangeID]StateEvent)

	seenUpdateDeleteChangeIDs := make(map[event.ChangeID]StateEvent)

	seenChangeIDs := make(map[event.ChangeID]string)

	seenDeletionIDs := make(map[string]string)

	appliedReads := make(map[string]struct{})

	// ordered by physical timestamp
	for _, e := range events {
		iKey := e.Effect.Key.IdentityKey
		switch e.Effect.OpType {
		case event.MARK_FOR_DELETION:
			globalRV++
			deletions[e.Effect.Key] = true

			changeID := e.Event.Labels[tag.ChangeID]
			seenUpdateDeleteChangeIDs[event.ChangeID(changeID)] = e
			seenDeletionIDs[changeID] = e.Effect.Version.Value
			contents[e.Effect.Key] = e.Effect.Version

			e.Sequence = globalRV
			KindSequences[iKey.CanonicalGroupKind()] = e.Sequence
			stateEvents = append(stateEvents, e)

			if correspondingRemoveEvt, ok := removalChangeIDs[event.ChangeID(changeID)]; ok {
				logger.WithValues(
					"Key", e.Effect.Key,
				).V(1).Info("applying saved remove event following delete event")
				// we want to apply the change to state, append the event to the sequence,
				// and then apply the removal event and add it to the sequence after.
				delete(contents, e.Effect.Key)
				removals[e.Effect.Key] = true
				globalRV++
				KindSequences[iKey.CanonicalGroupKind()] = globalRV
				correspondingRemoveEvt.Sequence = globalRV
				stateEvents = append(stateEvents, correspondingRemoveEvt)
			}

		case event.REMOVE:
			if _, removed := removals[e.Effect.Key]; removed {
				// we've already seen this removal event
				// there can be duplicate garbage collection REMOVEs + apiserver purge removes
				continue
			}
			// try and get the sleeve changeID label off the event
			changeID := e.MustGetChangeID()
			if _, seenPrecedingUpdate := seenUpdateDeleteChangeIDs[changeID]; seenPrecedingUpdate {
				// happy case - we've already seen the causally preceding update/delete event
				// we want to apply the change to state, append the event to the sequence,
				globalRV++
				delete(contents, e.Effect.Key)
				removals[e.Effect.Key] = true
				e.Sequence = globalRV
				KindSequences[iKey.CanonicalGroupKind()] = e.Sequence
				stateEvents = append(stateEvents, e)
			} else if e.Event != nil && e.Event.ControllerID == util.GarbageCollectorName {
				// garbage collection is a special case where there are no causally preceding
				// DELETE events (it cleans up things that nobody explicitly marked for deletion).
				// So, no need to wait for a preceding DELETE event.
				delete(contents, e.Effect.Key)
				globalRV++
				e.Sequence = globalRV
				removals[e.Effect.Key] = true
				KindSequences[iKey.CanonicalGroupKind()] = e.Sequence
				stateEvents = append(stateEvents, e)
			} else {
				logger.WithValues(
					"Key", e.Effect.Key,
					"changeID", e.MustGetChangeID(),
				).V(1).Info("saving remove event for later")
				removalChangeIDs[changeID] = e
				// we need to wait for the causally preceding update/delete event
				// to arrive before we can apply this removal
				// fmt.Println("waiting for preceding update/delete event")
			}

		case event.CREATE:
			globalRV++
			e.Sequence = globalRV
			contents[e.Effect.Key] = e.Effect.Version
			KindSequences[iKey.CanonicalGroupKind()] = e.Sequence

			seenChangeIDs[e.MustGetChangeID()] = e.Effect.Version.Value
			stateEvents = append(stateEvents, e)

		case event.UPDATE, event.PATCH:
			changeID := e.MustGetChangeID()
			seenUpdateDeleteChangeIDs[changeID] = e
			seenChangeIDs[changeID] = e.Effect.Version.Value

			if _, sawRemovalForChange := removalChangeIDs[changeID]; sawRemovalForChange {
				// we want to apply the change to state, append the event to the sequence,
				// and then apply the removal event and add it to the sequence after.
				contents[e.Effect.Key] = e.Effect.Version
				globalRV++
				e.Sequence = globalRV
				KindSequences[iKey.CanonicalGroupKind()] = e.Sequence
				stateEvents = append(stateEvents, e)

				// we need to increment the sequence number for the removal event
				removalEvent := removalChangeIDs[changeID]
				delete(contents, removalEvent.Effect.Key)
				globalRV++
				removalEvent.Sequence = globalRV
				removals[removalEvent.Effect.Key] = true
				KindSequences[removalEvent.Effect.Key.IdentityKey.CanonicalGroupKind()] = removalEvent.Sequence
				stateEvents = append(stateEvents, removalEvent)
			} else {
				// happy case - we've already seen the causally preceding update/delete event
				// we want to apply the change to state, append the event to the sequence,
				contents[e.Effect.Key] = e.Effect.Version
				globalRV++
				e.Sequence = globalRV
				KindSequences[iKey.CanonicalGroupKind()] = e.Sequence
				stateEvents = append(stateEvents, e)
			}
			// if _, wasRemoved := removals[e.Effect.Key]; wasRemoved {
			// 	if _, haveremovalChangeID := removalChangeIDs[e.ChangeID()]; haveremovalChangeID {
			// 		fmt.Println("Saw a 'REMOVE' event for this key already but now encountering an update that causally precedes the removal")
			// 		fmt.Printf("event: %v\n", e.Effect.Key)

			// 	} else {
			// 		fmt.Print("WARNING: encountered a write after an object was removed (staleness)")
			// 		fmt.Printf("event: %v\n", e.Event)
			// 		continue
			// 	}
			// }

		case event.GET, event.LIST:
			prevValue, seen := seenChangeIDs[e.MustGetChangeID()]
			if seen {
				currValue := e.Effect.Version.Value
				_, applied := appliedReads[currValue]
				if prevValue != currValue && !applied {
					// we're reading back the effects of a previous write,
					// but the version has changed. This means the API server has added some
					// new state that we haven't seen yet, so let's add it.
					logger.WithValues(
						"Key", e.Effect.Key,
						"changeID", e.MustGetChangeID(),
					).V(1).Info("Applying effects of a prior write that have not been seen yet")
					contents[e.Effect.Key] = e.Effect.Version
					globalRV++
					e.Sequence = globalRV
					KindSequences[iKey.CanonicalGroupKind()] = e.Sequence
					stateEvents = append(stateEvents, e)
					appliedReads[currValue] = struct{}{}
				}
			}
		default:
			panic(fmt.Sprintf("unknown op type: %s", e.Effect.OpType))
		}

		// KindSequences[iKey.CanonicalGroupKind()] = e.Sequence
		// stateEvents = append(stateEvents, e)
	}
	out := NewStateSnapshot(contents, KindSequences, stateEvents)
	return &out
}
