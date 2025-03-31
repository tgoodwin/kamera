package tracecheck

import (
	"fmt"
	"maps"
	"sort"

	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"github.com/tgoodwin/sleeve/pkg/util"
)

type KindSequences map[string]int64

type StateSnapshot struct {
	contents ObjectVersions

	mode string // original or adjusted

	// per-kind sequence info for computing relative states
	// possibly stale with respect to the contents of stateEvents
	// but represents the contents of ObjectVersions
	KindSequences KindSequences

	// TODO populate -- used to quickly tell whether a snapshot is stale or not
	// but this could be computed from stateEvents at any time.
	TrueSequences KindSequences

	stateEvents []StateEvent // the changes that led to the current objectVersions
}

func newStateSnapshot(contents ObjectVersions, kindSequences KindSequences, stateEvents []StateEvent) StateSnapshot {
	if len(contents) > 0 && len(kindSequences) == 0 {
		panic("kind sequences must be non-empty if contents are non-empty")
	}
	// do some validation
	stateKinds := lo.Map(stateEvents, func(e StateEvent, _ int) string {
		return e.Effect.Key.IdentityKey.Kind
	})
	stateKindSet := util.NewSet(stateKinds...)
	seqKinds := lo.Keys(kindSequences)
	if len(stateKindSet) != len(seqKinds) {
		logger.WithValues(
			"contentKeys", stateKindSet.List(),
			"sequenceKeys", seqKinds,
		).Error(nil, "expected a sequence # for every kind in contents")
	}

	// assert that the sequence numbers on each state event are increasing monotonically
	for i := 1; i < len(stateEvents); i++ {
		if stateEvents[i].Sequence <= stateEvents[i-1].Sequence {
			logger.WithValues(
				"currentSequence", stateEvents[i].Sequence,
				"previousSequence", stateEvents[i-1].Sequence,
			).Error(nil, "sequence numbers must be increasing monotonically")
			panic("sequence numbers must be increasing monotonically")
		}
	}

	return StateSnapshot{
		contents:      contents,
		KindSequences: kindSequences,
		stateEvents:   stateEvents,
	}
}

type ResourceVersion int

func (s *StateSnapshot) All() ObjectVersions {
	return s.contents
}

func (s *StateSnapshot) Observable() ObjectVersions {
	ss := replayEventsAtSequence(s.stateEvents, s.KindSequences)
	return ss.contents
}

func (s *StateSnapshot) ObserveAt(staleSequences KindSequences) ObjectVersions {
	// for kinds not specified in staleSequences, we use the latest sequence number
	// this models how controllers can be up to date on some kinds but not others
	for k, v := range s.KindSequences {
		if _, exists := staleSequences[k]; !exists {
			staleSequences[k] = v
		}
	}
	filteredEvents := filterEventsAtSequence(s.stateEvents, staleSequences)
	rollup := replayEventSequenceToState(filteredEvents)
	return rollup.contents
}

func (s *StateSnapshot) FixAt(ks KindSequences) StateSnapshot {
	for k, v := range s.KindSequences {
		if _, exists := ks[k]; !exists {
			ks[k] = v
		}
	}
	filtered := filterEventsAtSequence(s.stateEvents, ks)
	// using causal cause 'FixAt' is usually used when parsing traces.
	// TODO refactor to better encapusalte the causal OOO / regular replay logic
	fixedView := replayCausalEventSequenceToState(filtered)
	ss := newStateSnapshot(fixedView.contents, ks, s.stateEvents)
	return ss
}

func (s *StateSnapshot) DumpContents() {
	keys := lo.Keys(s.contents)
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].IdentityKey.Kind < keys[j].IdentityKey.Kind
	})
	for _, key := range keys {
		val := s.contents[key]
		fmt.Println(key, util.ShortenHash(val.Value))
	}
}

func (s *StateSnapshot) Debug() {
	fmt.Println("State events:")
	for _, e := range s.stateEvents {
		sleeveObjectID := e.Effect.Key.IdentityKey.ObjectID
		fmt.Printf("ts:%s (%d) frameID:%s controller=%s op=%s item=%s:%s %s\n", e.Timestamp, e.Sequence, util.Shorter(e.ReconcileID), e.ControllerID, e.OpType, e.Kind, util.Shorter(sleeveObjectID), util.ShortenHash(e.Effect.Version.Value))
	}
	fmt.Println("contents:")
	s.DumpContents()
	fmt.Println("KindSequences:")
	for key, value := range s.KindSequences {
		fmt.Println(key, value)
	}

}

// generateCombos recursively generates all possible combinations
func generateCombos(values map[string][]int64, keys []string, index int, current KindSequences, result *[]KindSequences) {
	if index == len(keys) {
		// Create a copy of the current combination and store it
		comboCopy := make(KindSequences)
		for k, v := range current {
			comboCopy[k] = v
		}
		*result = append(*result, comboCopy)
		return
	}

	key := keys[index]
	for _, value := range values[key] {
		current[key] = value
		generateCombos(values, keys, index+1, current, result)
	}
}

// getAllCombos returns a slice of all possible maps
func getAllCombos(values map[string][]int64) []KindSequences {
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}

	var result []KindSequences
	generateCombos(values, keys, 0, make(KindSequences), &result)
	return result
}

func (s *StateSnapshot) Adjust(kind string, steps int64) (*StateSnapshot, error) {
	currSequences := make(KindSequences)
	maps.Copy(currSequences, s.KindSequences)

	if _, exists := currSequences[kind]; !exists {
		return nil, fmt.Errorf("unknown kind: %s", kind)
	}
	currSeqForKind := currSequences[kind]

	eventsForKind := lo.Filter(s.stateEvents, func(e StateEvent, _ int) bool {
		return e.Effect.Key.IdentityKey.Kind == kind
	})
	earlierEventsForKind := lo.Filter(eventsForKind, func(e StateEvent, _ int) bool {
		return e.Sequence < currSeqForKind
	})
	previousEventForKind, ok := lo.Last(earlierEventsForKind)
	if !ok {
		return nil, &ErrInsufficientEvents{
			Kind:         kind,
			CurrentSeq:   currSeqForKind,
			RequestedSeq: currSeqForKind - steps,
			Steps:        steps,
		}
	}
	fmt.Println("adjusting sequence from", currSeqForKind, "by", steps, "steps to", previousEventForKind.Sequence)
	currSequences[kind] = previousEventForKind.Sequence

	// adjust the sequence relatively using steps.
	// -1 would select the previous state for this kind

	adjusted := replayEventsAtSequence(s.stateEvents, currSequences)
	return &StateSnapshot{
		mode:          "adjusted",
		contents:      adjusted.contents,
		KindSequences: currSequences,
		stateEvents:   s.stateEvents,
	}, nil
}

type StateEvent struct {
	*event.Event
	ReconcileID string
	Timestamp   string
	Effect      Effect
	Sequence    int64 // the sequence within the kind

	rv ResourceVersion // model etcd resource version
}

func (s *StateEvent) Key() string {
	return s.Effect.Key.IdentityKey.ObjectID
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

func (k *KindKnowledge) AddEvent(e event.Event, eff Effect, rv ResourceVersion) StateEvent {
	// Increment sequence
	k.CurrentSequence++

	// Create StateEvent with new sequence number
	stateEvent := StateEvent{
		Event:       &e,
		ReconcileID: e.ReconcileID,
		Timestamp:   e.Timestamp,
		// contains redundant info as E
		Effect: eff,
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

	if _, ok := k.ChangeIDIndex[e.MustGetChangeID()]; ok && !event.IsTopLevel(e) {
		logger.WithValues(
			"changeID", e.MustGetChangeID(),
			"eventID", e.ID,
			"existingEventID", k.ChangeIDIndex[e.MustGetChangeID()].ID,
		).Error(nil, "duplicate change ID")
		panic("duplicate change ID for state change event")
	}
	k.ChangeIDIndex[e.MustGetChangeID()] = stateEvent

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
		return event.IsWriteOp(event.OperationType(e.OpType)) || event.IsTopLevel(e)
	})

	// process each event
	for _, e := range changeEvents {
		version, err := g.resolver.ResolveVersion(e.CausalKey())
		if err != nil {
			return errors.Wrap(err, "resolving version")
		}
		// TODO fix the whole ResolveVersion business THIS IS A BLOODY HACK
		key := snapshot.NewCompositeKey(
			e.Kind,
			"default",
			e.ObjectID, // this is supposed to be NAME
			e.ObjectID, // this is supposed to be SLEEVE OBJECT ID
		)
		effect := newEffect(
			key,
			version,
			event.OperationType(e.OpType),
		)

		g.globalResourceVersion++
		g.Kinds[e.Kind].AddEvent(e, effect, g.globalResourceVersion)
	}

	return nil
}

func CausalRollup(events []StateEvent) *StateSnapshot {
	for _, e := range events {
		if e.Event == nil {
			panic("event.Event is nil, causal rollup wont work")
		}
	}
	sequencedEvents := AssignResourceVersions(events)
	return replayCausalEventSequenceToState(sequencedEvents)
}

func filterEventsAtSequence(events []StateEvent, sequencesForEachKind KindSequences) []StateEvent {
	eventsByKind := lo.GroupBy(events, func(e StateEvent) string {
		return e.Effect.Key.IdentityKey.Kind
	})
	for kind := range eventsByKind {
		_, exists := sequencesForEachKind[kind]
		if !exists {
			panic(fmt.Sprintf("no sequence found for kind: %s, %v", kind, sequencesForEachKind))
		}
	}
	toReplay := lo.Filter(events, func(e StateEvent, _ int) bool {
		kindLimit := sequencesForEachKind[e.Effect.Key.IdentityKey.Kind]
		keep := e.Sequence <= kindLimit
		if !keep {
			logger.V(2).WithValues(
				"Sequence", e.Sequence,
				"Kind", e.Effect.Key.IdentityKey.Kind,
				"KindLimit", kindLimit,
			).Info("Dropping event")
		}
		return keep
	})
	return toReplay
}

func replayEventsAtSequence(events []StateEvent, sequencesByKind KindSequences) *StateSnapshot {
	toReplay := filterEventsAtSequence(events, sequencesByKind)
	return replayEventSequenceToState(toReplay)
}

func limitEventHistory(seqByKind map[string][]int64, kindBounds LookbackLimits) map[string][]int64 {
	if kindBounds == nil {
		return seqByKind
	}
	out := make(map[string][]int64)
	maps.Copy(out, seqByKind)
	for k, v := range out {
		kindBound, ok := kindBounds[k]
		if ok {
			// define zero as no bounds
			if kindBound == NoLimit {
				continue
			}
			if len(v) > int(kindBound) {
				out[k] = v[len(v)-int(kindBound):]
			}
		}
	}

	return out
}

type LookbackLimit int

const NoLimit LookbackLimit = 0

// LookbackLimits is a map of kind to the number of preceding RVs to consider in the history
// when producing stale views. A value of 0 means no bound (all RVs considered).
type LookbackLimits map[string]LookbackLimit

func getAllPossibleViews(snapshot *StateSnapshot, relevantKinds []string, kindBounds LookbackLimits) []*StateSnapshot {
	var staleViews []*StateSnapshot

	staleViews = append(staleViews, snapshot)

	eventsByKind := lo.GroupBy(snapshot.stateEvents, func(e StateEvent) string {
		return e.Effect.Key.IdentityKey.Kind
	})
	seqByKind := lo.MapValues(eventsByKind, func(events []StateEvent, key string) []int64 {
		return lo.Map(events, func(e StateEvent, _ int) int64 {
			return e.Sequence
		})
	})

	if len(kindBounds) > 0 {
		seqByKind = limitEventHistory(seqByKind, kindBounds)
	}

	filtered := make(map[string][]int64)
	for k, v := range seqByKind {
		// since the number of stale states can explode so quickly, we require users
		// to explicitly include the dimensions (kinds) they want to consider.
		_, kindConfiguredForStaleness := kindBounds[k]
		if lo.Contains(relevantKinds, k) && kindConfiguredForStaleness {
			filtered[k] = v
		}
	}

	allPossibleKindSequences := getAllCombos(filtered)
	for _, possibleCombo := range allPossibleKindSequences {
		// there may be duplicates in the generated kind sequences
		if maps.Equal(possibleCombo, snapshot.KindSequences) {
			continue
		}

		staleSequences := make(KindSequences)
		maps.Copy(staleSequences, snapshot.KindSequences)
		// State for kinds outside of relevantKinds is included at the latest sequence.
		// Only the relevant kinds are adjusted to the stale sequence.
		for k, v := range possibleCombo {
			staleSequences[k] = v
		}

		// we preserve the original state but adjust the sequence numbers
		// to reflect the new view among all possible stale views.
		// the stale view must be "observed" via the Observe() method
		out := newStateSnapshot(snapshot.contents, staleSequences, snapshot.stateEvents)
		staleViews = append(staleViews, &out)
		logger.V(2).WithValues(
			"lookbackLimits", kindBounds,
			"staleSequences", staleSequences,
		).Info("adding stale view")
	}

	return staleViews
}

func getAllViewsForController(snapshot *StateSnapshot, reconcilerID string, deps ResourceDeps, kindBounds LookbackLimits) ([]*StateSnapshot, error) {
	controllerDeps, err := deps.ForReconciler(reconcilerID)
	if err != nil {
		return nil, err
	}

	// Get the current sequence for the kind
	staleViews := getAllPossibleViews(snapshot, controllerDeps, kindBounds)
	return staleViews, nil
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
	return replayEventSequenceToState(relevantEvents)
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

	return replayEventSequenceToState(events)
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

	return replayEventSequenceToState(relevantEvents), nil
}
