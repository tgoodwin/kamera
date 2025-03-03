package tracecheck

import (
	"fmt"
	"sort"

	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
)

type StateSnapshot struct {
	contents ObjectVersions

	// per-kind sequence info for computing relative states
	KindSequences map[string]int64

	// knowledgeManager *EventKnowledge
}

type ResourceVersion int

func (s *StateSnapshot) Objects() ObjectVersions {
	return s.contents
}

type StateEvent struct {
	*event.Event
	ReconcileID string
	Timestamp   string
	effect      effect
	// ChangeID    event.ChangeID
	Sequence int64 // the sequence within the kind

	rv ResourceVersion // model etcd resource version
}

// Tracks the complete history of a single object
type ObjectHistory struct {
	ObjectID         string
	IsDeleted        bool
	Events           []StateEvent // every change event that has happened to this object
	EventsBySequence map[int64]StateEvent
}

type KindKnowledge struct {
	Kind     string
	Objects  map[string]*ObjectHistory
	EventLog []StateEvent

	// track the latest change sequence number for this kind
	CurrentSequence int64

	resourceVersions []ResourceVersion

	changeEventByResourceVersion map[ResourceVersion]StateEvent

	SequenceIndex map[int64]StateEvent

	ChangeIDIndex map[event.ChangeID]StateEvent

	// finding events by reconcileID
	ReconcileIndex map[string][]StateEvent
}

func NewKindKnowledge() *KindKnowledge {
	return &KindKnowledge{
		Objects:                      make(map[string]*ObjectHistory),
		EventLog:                     make([]StateEvent, 0),
		SequenceIndex:                make(map[int64]StateEvent),
		ChangeIDIndex:                make(map[event.ChangeID]StateEvent),
		ReconcileIndex:               make(map[string][]StateEvent),
		resourceVersions:             make([]ResourceVersion, 0),
		changeEventByResourceVersion: make(map[ResourceVersion]StateEvent),
	}
}

func (k *KindKnowledge) Summarize() {
	// for key, obj := range k.Objects {
	// 	fmt.Println(key)
	// 	fmt.Println("isDeleted", obj.IsDeleted, "num lifecycle events", len(obj.Events))
	// }
}

func (k *KindKnowledge) AddEvent(e event.Event, eff effect, rv ResourceVersion) StateEvent {
	// Increment sequence
	k.CurrentSequence++

	// Create StateEvent with new sequence number
	stateEvent := StateEvent{
		Event:       &e,
		ReconcileID: e.ReconcileID,
		Timestamp:   e.Timestamp,
		// contains redundant info as E
		effect: eff,
		// ChangeID: e.ChangeID(),
		Sequence: k.CurrentSequence,
		rv:       rv,
	}

	// Add to event log and index
	k.EventLog = append(k.EventLog, stateEvent)
	k.SequenceIndex[k.CurrentSequence] = stateEvent

	// do the same thing for global resource version
	k.resourceVersions = append(k.resourceVersions, rv)
	k.changeEventByResourceVersion[rv] = stateEvent

	if _, ok := k.ChangeIDIndex[e.ChangeID()]; ok && !event.IsTopLevel(e) {
		logger.WithValues(
			"changeID", e.ChangeID(),
			"eventID", e.ID,
			"existingEventID", k.ChangeIDIndex[e.ChangeID()].ID,
		).Error(nil, "duplicate change ID")
		panic("duplicate change ID for state change event")
	}
	k.ChangeIDIndex[e.ChangeID()] = stateEvent

	k.ReconcileIndex[e.ReconcileID] = append(k.ReconcileIndex[e.ReconcileID], stateEvent)

	// Update or create ObjectHistory
	if _, exists := k.Objects[e.ObjectID]; !exists {
		k.Objects[e.ObjectID] = &ObjectHistory{
			Events:           make([]StateEvent, 0),
			EventsBySequence: make(map[int64]StateEvent),
		}
	}

	objHistory := k.Objects[e.ObjectID]
	objHistory.Events = append(objHistory.Events, stateEvent)
	objHistory.EventsBySequence[k.CurrentSequence] = stateEvent
	if e.OpType == "DELETE" {
		objHistory.IsDeleted = true
	}

	return stateEvent
}

type VersionResolver interface {
	ResolveVersion(causalKey event.CausalKey) (snapshot.VersionHash, error)
}

type KnowledgeManager struct {
	snapStore *snapshot.Store
	*EventKnowledge
	eventKeyToVersion map[event.CausalKey]snapshot.VersionHash
}

func NewKnowledgeManager(snapStore *snapshot.Store) *KnowledgeManager {
	return &KnowledgeManager{
		snapStore:         snapStore,
		EventKnowledge:    NewEventKnowledge(nil),
		eventKeyToVersion: make(map[event.CausalKey]snapshot.VersionHash),
	}
}

// func (km *KnowledgeManager) AddObject(e event.Event, obj unstructured.Unstructured) error {
// 	// TODO
// 	// ckey := event.CausalKey()
// 	// Store object in snapshot store
// 	if err := km.snapStore.StoreObject(&obj); err != nil {
// 		return errors.Wrap(err, "adding object to knowledge manager")
// 	}
// }

type EventKnowledge struct {
	Kinds    map[string]*KindKnowledge
	resolver VersionResolver
	// TODO refactor
	allEvents []event.Event

	// model etcd resource version
	globalResourceVersion ResourceVersion
}

func NewEventKnowledge(resolver VersionResolver) *EventKnowledge {
	return &EventKnowledge{
		Kinds:                 make(map[string]*KindKnowledge),
		resolver:              resolver,
		globalResourceVersion: 0,
	}
}

func (g *EventKnowledge) Load(events []event.Event) error {
	g.allEvents = events

	// first pass -- ensure KindKnowledge exists for each kind we encounter
	for _, e := range events {
		if _, exists := g.Kinds[e.Kind]; !exists {
			g.Kinds[e.Kind] = NewKindKnowledge()
		}
	}

	// second pass -- process events in chronological order and assign sequences
	sortedEvents := make([]event.Event, len(events))
	copy(sortedEvents, events)
	sort.Slice(sortedEvents, func(i, j int) bool {
		return sortedEvents[i].Timestamp < sortedEvents[j].Timestamp
	})

	// we only want to track state change events. This includes top-level state declaration events.
	changeEvents := lo.Filter(sortedEvents, func(e event.Event, _ int) bool {
		return event.IsWriteOp(e) || event.IsTopLevel(e)
	})

	// process each event
	for _, e := range changeEvents {
		version, err := g.resolver.ResolveVersion(e.CausalKey())
		if err != nil {
			return errors.Wrap(err, "resolving version")
		}
		effect := newEffect(
			e.Kind,
			e.ObjectID,
			version,
			event.OperationType(e.OpType),
		)

		g.globalResourceVersion++
		g.Kinds[e.Kind].AddEvent(e, effect, g.globalResourceVersion)
	}

	return nil
}

func (g *EventKnowledge) replayEventsToState(events []StateEvent) *StateSnapshot {
	state := &StateSnapshot{
		contents:      make(ObjectVersions),
		KindSequences: make(map[string]int64),
		// knowledgeManager: g,
	}

	for _, e := range events {
		cKey := e.CausalKey()
		iKey := snapshot.IdentityKey{
			Kind:     cKey.Kind,
			ObjectID: cKey.ObjectID,
		}
		if e.OpType == "DELETE" {
			delete(state.contents, iKey)
		} else {
			// use resolver to get object version
			version, err := g.resolver.ResolveVersion(cKey)
			if err != nil {
				panic("error resolving version")
			}
			state.contents[iKey] = version
		}
		state.KindSequences[cKey.Kind] = e.Sequence
	}

	return state
}

func (g *EventKnowledge) eventsBeforeTimestamp(ts string) []StateEvent {
	// First find all events at this timestamp
	var relevantEvents []StateEvent
	for _, kindKnowledge := range g.Kinds {
		precedingKindEvents := lo.Filter(kindKnowledge.EventLog, func(e StateEvent, _ int) bool {
			return e.Timestamp < ts
		})
		relevantEvents = append(relevantEvents, precedingKindEvents...)
	}

	// Sort by timestamp for replay since sequences aren't comparable across kinds
	sort.Slice(relevantEvents, func(i, j int) bool {
		return relevantEvents[i].Timestamp < relevantEvents[j].Timestamp
	})

	return relevantEvents
}

func (g *EventKnowledge) GetStateAtReconcileID(reconcileID string) *StateSnapshot {
	// First find all events in this reconcile

	reconcileEvents := lo.Filter(g.allEvents, func(e event.Event, _ int) bool {
		return e.ReconcileID == reconcileID
	})

	if len(reconcileEvents) == 0 {
		panic(fmt.Sprintf("no events found for reconcileID: %s", reconcileID))
	}

	// physical time based heuristic
	earliestTimestamp := reconcileEvents[0].Timestamp
	for _, e := range reconcileEvents {
		if e.Timestamp < earliestTimestamp {
			earliestTimestamp = e.Timestamp
		}
	}
	relevantEvents := g.eventsBeforeTimestamp(earliestTimestamp)
	return g.replayEventsToState(relevantEvents)
}

func (g *EventKnowledge) GetStateAfterReconcileID(reconcileID string) *StateSnapshot {
	reconcileEvents := lo.Filter(g.allEvents, func(e event.Event, _ int) bool {
		return e.ReconcileID == reconcileID
	})

	if len(reconcileEvents) == 0 {
		panic(fmt.Sprintf("no events found for reconcileID: %s", reconcileID))
	}

	latestTimestamp := reconcileEvents[0].Timestamp
	for _, e := range reconcileEvents {
		if e.Timestamp > latestTimestamp {
			latestTimestamp = e.Timestamp
		}
	}
	// physical time based heuristic
	earliestTimestamp := reconcileEvents[0].Timestamp
	relevantEvents := g.eventsBeforeTimestamp(earliestTimestamp)
	reconcileStateEvents := []StateEvent{}

	// TODO refactor indexing strategy
	for _, kindKnowledge := range g.Kinds {
		for _, e := range kindKnowledge.EventLog {
			if e.ReconcileID == reconcileID {
				reconcileStateEvents = append(reconcileStateEvents, e)
			}
		}
	}
	// all events preceding and including the reconcile events
	events := lo.Union(relevantEvents, reconcileStateEvents)
	sort.Slice(events, func(i, j int) bool {
		return events[i].Timestamp < events[j].Timestamp
	})

	return g.replayEventsToState(events)
}

// ErrInsufficientEvents indicates we can't adjust knowledge by requested steps
type ErrInsufficientEvents struct {
	Kind         string
	CurrentSeq   int64
	RequestedSeq int64
	Steps        int64
}

func (e ErrInsufficientEvents) Error() string {
	return fmt.Sprintf("cannot adjust knowledge for kind %s by %d steps: would require sequence %d but valid range is [0, %d]",
		e.Kind, e.Steps, e.RequestedSeq, e.CurrentSeq)
}

func (g *EventKnowledge) AdjustKnowledgeForResourceType(snapshot *StateSnapshot, kind string, steps int64) (*StateSnapshot, error) {
	kindKnowledge, exists := g.Kinds[kind]
	if !exists {
		return nil, fmt.Errorf("unknown kind: %s", kind)
	}

	currentSeq := snapshot.KindSequences[kind]
	targetSeq := currentSeq + steps
	logger.V(2).WithValues(
		"currentSeq", currentSeq,
		"targetSeq", targetSeq,
	).Info("Adjusting knowledge for kind")

	// Check bounds
	if targetSeq < 0 {
		return nil, &ErrInsufficientEvents{
			Kind:         kind,
			CurrentSeq:   currentSeq,
			RequestedSeq: targetSeq,
			Steps:        steps,
		}
	}
	if targetSeq > kindKnowledge.CurrentSequence {
		return nil, &ErrInsufficientEvents{
			Kind:         kind,
			CurrentSeq:   kindKnowledge.CurrentSequence,
			RequestedSeq: targetSeq,
			Steps:        steps,
		}
	}

	// Collect all events up to target sequence
	var relevantEvents []StateEvent
	for kindName, kKnowledge := range g.Kinds {
		for _, event := range kKnowledge.EventLog {
			if kindName == kind {
				// For the target kind, only include events up to targetSeq
				if event.Sequence <= targetSeq {
					relevantEvents = append(relevantEvents, event)
				}
			} else {
				// For other kinds, maintain their current sequences
				if event.Sequence <= snapshot.KindSequences[kindName] {
					relevantEvents = append(relevantEvents, event)
				}
			}
		}
	}

	// Sort by sequence to ensure correct replay order
	sort.Slice(relevantEvents, func(i, j int) bool {
		return relevantEvents[i].Sequence < relevantEvents[j].Sequence
	})

	return g.replayEventsToState(relevantEvents), nil
}
