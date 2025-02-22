package tracecheck

import (
	"fmt"
	"sort"

	"github.com/samber/lo"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
)

type StateSnapshot struct {
	// TODO this is ObjectVersions
	Objects map[snapshot.IdentityKey]string

	// per-kind sequence info for computing relative states
	KindSequences map[string]int64
}

type StateEvent struct {
	*event.Event
	ChangeID string
	Sequence int64
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

	SequenceIndex map[int64]StateEvent

	// finding events by reconcileID
	ReconcileIndex map[string][]StateEvent
}

func NewKindKnowledge() *KindKnowledge {
	return &KindKnowledge{
		Objects:        make(map[string]*ObjectHistory),
		EventLog:       make([]StateEvent, 0),
		SequenceIndex:  make(map[int64]StateEvent),
		ReconcileIndex: make(map[string][]StateEvent),
	}
}

func (k *KindKnowledge) AddEvent(event event.Event) StateEvent {
	// Increment sequence
	k.CurrentSequence++

	// Create StateEvent with new sequence number
	stateEvent := StateEvent{
		Event:    &event,
		Sequence: k.CurrentSequence,
	}

	// Add to event log and index
	k.EventLog = append(k.EventLog, stateEvent)
	k.SequenceIndex[k.CurrentSequence] = stateEvent

	k.ReconcileIndex[event.ReconcileID] = append(k.ReconcileIndex[event.ReconcileID], stateEvent)

	// Update or create ObjectHistory
	if _, exists := k.Objects[event.ObjectID]; !exists {
		k.Objects[event.ObjectID] = &ObjectHistory{
			Events:           make([]StateEvent, 0),
			EventsBySequence: make(map[int64]StateEvent),
		}
	}

	objHistory := k.Objects[event.ObjectID]
	objHistory.Events = append(objHistory.Events, stateEvent)
	objHistory.EventsBySequence[k.CurrentSequence] = stateEvent
	if event.OpType == "DELETE" {
		objHistory.IsDeleted = true
	}

	return stateEvent
}

type VersionResolver interface {
	Resolve(causalKey event.CausalKey) (string, error)
}

type GlobalKnowledge struct {
	Kinds    map[string]*KindKnowledge
	resolver VersionResolver
}

func NewGlobalKnowledge(resolver VersionResolver) *GlobalKnowledge {
	return &GlobalKnowledge{
		Kinds:    make(map[string]*KindKnowledge),
		resolver: resolver,
	}
}

func (g *GlobalKnowledge) Load(events []event.Event) error {
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
		g.Kinds[e.Kind].AddEvent(e)
	}

	return nil
}

func (g *GlobalKnowledge) replayEventsToState(events []StateEvent) *StateSnapshot {
	state := &StateSnapshot{
		Objects:       make(map[snapshot.IdentityKey]string),
		KindSequences: make(map[string]int64),
	}

	for _, e := range events {
		cKey := e.CausalKey()
		iKey := snapshot.IdentityKey{
			Kind:     cKey.Kind,
			ObjectID: cKey.ObjectID,
		}
		if e.OpType == "DELETE" {
			delete(state.Objects, iKey)
		} else {
			// use resolver to get object version
			version, err := g.resolver.Resolve(cKey)
			if err != nil {
				panic("error resolving version")
			}
			state.Objects[iKey] = version
		}
		state.KindSequences[cKey.Kind] = e.Sequence
	}

	return state
}

func (g *GlobalKnowledge) GetStateAtReconcileID(reconcileID string) *StateSnapshot {
	// First find all events in this reconcile
	var reconcileEvents []StateEvent
	for _, kind := range g.Kinds {
		if events, ok := kind.ReconcileIndex[reconcileID]; ok {
			reconcileEvents = append(reconcileEvents, events...)
		}
	}

	if len(reconcileEvents) == 0 {
		return nil
	}

	// Find the highest sequence number for each kind among read operations
	maxReadSequences := make(map[string]int64)
	for _, e := range reconcileEvents {
		if event.IsReadOp(*e.Event) {
			kind := e.Event.Kind
			if e.Sequence > maxReadSequences[kind] {
				maxReadSequences[kind] = e.Sequence
			}
		}
	}

	// Collect all events up to and including the last read for each kind
	var relevantEvents []StateEvent
	for kindName, kind := range g.Kinds {
		maxSeq := maxReadSequences[kindName]
		for _, event := range kind.EventLog {
			if event.Sequence <= maxSeq {
				relevantEvents = append(relevantEvents, event)
			}
		}
	}

	// Sort by timestamp for replay since sequences aren't comparable across kinds
	sort.Slice(relevantEvents, func(i, j int) bool {
		return relevantEvents[i].Timestamp < relevantEvents[j].Timestamp
	})

	return g.replayEventsToState(relevantEvents)
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

func (g *GlobalKnowledge) AdjustKnowledgeForKind(snapshot *StateSnapshot, kind string, steps int64) (*StateSnapshot, error) {
	kindKnowledge, exists := g.Kinds[kind]
	if !exists {
		return nil, fmt.Errorf("unknown kind: %s", kind)
	}

	currentSeq := snapshot.KindSequences[kind]
	targetSeq := currentSeq + steps

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
