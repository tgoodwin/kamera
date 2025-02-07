package replay

import (
	"fmt"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"github.com/tgoodwin/sleeve/pkg/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func ParseTrace(traceData []byte) (*Builder, error) {
	b := &Builder{}
	if err := b.fromTrace(traceData); err != nil {
		return nil, err
	}
	return b, nil
}

// TODO
// - need to index snapshot records by Kind somehow to support List operations
// - have the client hold some map of reconcileID -> map of NamespacedName => client.Object
// - probably want to have the Client's Get / List methods infer the Kind of the object from the object itself
// - and then use that to key into the map of snapshot records
// - and perhaps in the client we can pull the reconcileID out of the context
// - and we will set the reconcileID in the context in the Replayer

// Builder handles the replaying of a sequence of frames to a given reconciler.
type Builder struct {
	// object versions found in the trace
	*replayStore

	// controller operations found in the trace
	events []event.Event

	// for bookkeeping and validation
	ReconcilerIDs util.Set[string]
}

func (b *Builder) Store() Store {
	return b.replayStore.store
}

func (b *Builder) Events() []event.Event {
	return b.events
}
func (b *Builder) Debug() {
	fmt.Println("--- store contents ---")
	b.DumpKeys()
	fmt.Println("--- end store contents ---")
	fmt.Println("--- events ---")
	seenKeys := make(util.Set[event.CausalKey])
	eventsMissingRecords := make(util.Set[event.CausalKey])
	for _, e := range b.events {
		seenKeys.Add(e.CausalKey())
		if _, ok := b.Store()[e.CausalKey()]; !ok {
			eventsMissingRecords.Add(e.CausalKey())
		}
	}
	for key := range seenKeys {
		fmt.Println(key)
	}
	fmt.Println("--- end events ---")
	fmt.Println("--- events missing records ---")
	for key := range eventsMissingRecords {
		fmt.Println(key)
	}
	fmt.Println("--- end events missing records ---")
}

func (b *Builder) AnalyzeReconcile(reconcileID string) {
	reconcileEvents := lo.Filter(b.events, func(e event.Event, _ int) bool {
		return strings.HasPrefix(e.ReconcileID, reconcileID)
	})
	sort.Slice(reconcileEvents, func(i, j int) bool {
		return reconcileEvents[i].Timestamp < reconcileEvents[j].Timestamp
	})
	reads, writes := event.FilterReadsWrites(reconcileEvents)
	fmt.Println("ReconcileID:", reconcileID)
	fmt.Println("Reads:")
	for _, e := range reads {
		fmt.Println(e)
	}
	fmt.Println("Writes:")
	for _, e := range writes {
		fmt.Println(e)
	}
}

func (b *Builder) AnalyzeObject(objectID string) {
	objectEvents := lo.Filter(b.events, func(e event.Event, _ int) bool {
		return strings.HasPrefix(e.ObjectID, objectID)
	})
	sort.Slice(objectEvents, func(i, j int) bool {
		return objectEvents[i].Timestamp < objectEvents[j].Timestamp
	})
	var previousVersion *unstructured.Unstructured
	var prevKey event.CausalKey
	for _, e := range objectEvents {
		ckey := e.CausalKey()
		if event.IsWriteOp(e) {
			currentVersion, ok := b.Store()[ckey]
			if !ok {
				// fmt.Printf("WARNING: object not found in store: %#v\n", ckey)
				continue
			}
			if previousVersion != nil {
				delta, err := snapshot.DiffObjects(previousVersion, currentVersion)
				if err != nil {
					fmt.Println("Error diffing objects:", err)
				}
				fmt.Printf("ReconcileID: %s\n \tPrevKey: %s\n\tCurrKey: %s\n", e.ReconcileID, prevKey.Short(), ckey.Short())
				fmt.Println(delta)
			}
			previousVersion = currentVersion
			prevKey = ckey
		}
		// fmt.Println(e)
	}
}

func (b *Builder) fromTrace(traceData []byte) error {
	if err := b.hydrateObjectValues(traceData); err != nil {
		return errors.Wrap(err, "hydrating object values")
	}

	// track all reconciler IDs in the trace
	b.ReconcilerIDs = make(util.Set[string])

	lines := strings.Split(string(traceData), "\n")
	events, err := ParseEventsFromLines(lines)
	if err != nil {
		return err
	}
	fmt.Println("total events", len(events))

	// filter events to only include those that are reads
	readEvents := lo.Filter(events, func(e event.Event, _ int) bool {
		return e.OpType == "GET" || e.OpType == "LIST"
	})

	// for each read event, sanity check that the object is in the store
	// if not, return an error
	for _, e := range readEvents {
		key := e.CausalKey()
		if _, ok := b.store[key]; !ok {
			fmt.Printf("WARNING: object not found in store: %#v\n", key)
			continue
		}
		b.ReconcilerIDs.Add(e.ControllerID)
	}

	b.events = events
	for controllerID := range b.ReconcilerIDs {
		fmt.Println("Found controllerID in trace", controllerID)
	}

	return nil
}

func (b *Builder) hydrateObjectValues(traceData []byte) error {
	// hydrate object values
	rs := newReplayStore()
	if err := rs.HydrateFromTrace(traceData); err != nil {
		return err
	}
	b.replayStore = rs
	return nil
}

func (b *Builder) BuildHarness(controllerID string) (*Harness, error) {
	if _, ok := b.ReconcilerIDs[controllerID]; !ok {
		return nil, fmt.Errorf("controllerID not found in trace: %s", controllerID)
	}

	controllerEvents := lo.Filter(b.events, func(e event.Event, _ int) bool {
		return e.ControllerID == controllerID
	})
	byReconcileID := lo.GroupBy(controllerEvents, func(e event.Event) string {
		return e.ReconcileID
	})

	FrameData := make(map[string]FrameData)
	frames := make([]Frame, 0)
	effects := make(map[string]DataEffect)

	for reconcileID, events := range byReconcileID {

		reads, writes := event.FilterReadsWrites(events)
		effects[reconcileID] = DataEffect{Reads: reads, Writes: writes}
		req, err := b.inferReconcileRequestFromReadset(controllerID, reads)
		if err != nil {
			return nil, err
		}
		cacheFrame, err := b.generateCacheFrame(reads)
		if err != nil {
			return nil, err
		}
		FrameData[reconcileID] = cacheFrame

		rootEventID := getRootIDFromEvents(events)

		// TODO revisit this
		earliestTs := events[0].Timestamp

		frames = append(frames, Frame{Type: FrameTypeTraced, ID: reconcileID, Req: req, sequenceID: earliestTs, TraceyRootID: rootEventID})
	}

	// sort the frames by sequenceID
	sort.Slice(frames, func(i, j int) bool {
		return frames[i].sequenceID < frames[j].sequenceID
	})

	harness := newHarness(controllerID, frames, FrameData, effects)
	return harness, nil
}

func (r *Builder) generateCacheFrame(events []event.Event) (FrameData, error) {
	cacheFrame := make(FrameData)
	for _, e := range events {
		key := e.CausalKey()
		if obj, ok := r.store[key]; ok {
			if _, ok := cacheFrame[e.Kind]; !ok {
				cacheFrame[e.Kind] = make(map[types.NamespacedName]*unstructured.Unstructured)
			}
			cacheFrame[e.Kind][types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}] = obj
		} else {
			return nil, fmt.Errorf("object not found in store: %#v", key)
		}
	}
	return cacheFrame, nil
}

func (r *Builder) inferReconcileRequestFromReadset(controllerID string, readset []event.Event) (reconcile.Request, error) {
	for _, e := range readset {
		// Assumption: reconcile routines are invoked upon a Resource that shares the same name (Kind)
		// as the controller that is managing it.
		if e.Kind == controllerID {
			if obj, ok := r.store[e.CausalKey()]; ok {
				name := obj.GetName()
				namespace := obj.GetNamespace()
				req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}}
				return req, nil
			}
		}
	}
	return reconcile.Request{}, fmt.Errorf("could not infer reconcile.Request from readset")
}

func getRootIDFromEvents(readset []event.Event) string {
	readEventCounts := make(map[string]int)
	for _, e := range readset {
		if e.RootEventID != "" {
			readEventCounts[e.RootEventID]++
		}
	}
	// return the read event with the highest count
	var maxCount int
	var rootID string
	for id, count := range readEventCounts {
		if count > maxCount {
			maxCount = count
			rootID = id
		}
	}
	if len(readEventCounts) > 1 {
		fmt.Println("Multiple root event IDs found in readset:", readEventCounts)
	}
	return rootID
}
