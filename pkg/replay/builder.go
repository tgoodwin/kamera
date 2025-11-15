package replay

import (
	"fmt"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/util"
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

	// this tracks which top-level object a reconciler is triggered with
	reconcilerToKind map[string]string
}

func (b *Builder) AssignReconcilerToKind(reconcilerID, kind string) {
	if b.reconcilerToKind == nil {
		b.reconcilerToKind = make(map[string]string)
	}
	gk := util.ParseGroupKind(kind)
	b.reconcilerToKind[reconcilerID] = util.CanonicalGroupKind(gk.Group, gk.Kind)
}

func (b *Builder) Store() Store {
	return b.replayStore.store
}

func (b *Builder) Events() []event.Event {
	return b.events
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
		if event.IsWriteOp(event.OperationType(e.OpType)) {
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
	}
}

type ReconcileEvent struct {
	ReconcileID  string
	ControllerID string
	Frame        Frame
}

func (b *Builder) OrderedReconcileEvents() []ReconcileEvent {
	traceEvents := b.Events()

	byReconcile := lo.GroupBy(traceEvents, func(e event.Event) string {
		return e.ReconcileID
	})
	out := make([]ReconcileEvent, 0)
	for reconcileID, events := range byReconcile {
		// assert that all events in a reconcileID group have the same controllerID
		controllerID := events[0].ControllerID
		for _, e := range events {
			if e.ControllerID != controllerID {
				fmt.Printf("WARNING: reconcileID %s has multiple controllerIDs: %s, %s\n", reconcileID, controllerID, e.ControllerID)
			}
		}
		// sort events by timestamp
		sort.Slice(events, func(i, j int) bool {
			return events[i].Timestamp < events[j].Timestamp
		})

		reads, _ := event.FilterReadsWrites(events)
		req, err := b.inferReconcileRequestFromReadset(controllerID, reads)
		if err != nil {
			fmt.Println("Error inferring reconcile request:", err)
		}
		fmt.Printf("ReconcileID: %s, ControllerID: %s, Req: %v\n", reconcileID, controllerID, req)
		// cacheFrame, err := b.generateCacheFrame(reads)
		// if err != nil {
		// 	fmt.Println("Error generating cache frame:", err)
		// }
		rootEventID := getRootIDFromEvents(events)
		earliestTs := events[0].Timestamp

		frame := Frame{Type: FrameTypeTraced, ID: reconcileID, Req: req, sequenceID: earliestTs, TraceyRootID: rootEventID}
		out = append(out, ReconcileEvent{ReconcileID: reconcileID, ControllerID: controllerID, Frame: frame})

	}
	// // sort by timestamp
	// sort.Slice(traceEvents, func(i, j int) bool {
	// 	return traceEvents[i].Timestamp < traceEvents[j].Timestamp
	// })
	// // map to ReconcileEvent
	// reconcileEvents := lo.Map(traceEvents, func(e event.Event, _ int) ReconcileEvent {
	// 	return ReconcileEvent{ReconcileID: e.ReconcileID, ControllerID: e.ControllerID}
	// })

	uniq := lo.Uniq(out)
	if len(uniq) != len(out) {
		fmt.Println("WARNING: duplicate reconcile events found")
	}
	return uniq
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

	FrameData := make(map[string]CacheFrame)
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

func (r *Builder) generateCacheFrame(events []event.Event) (CacheFrame, error) {
	cacheFrame := make(CacheFrame)
	for _, e := range events {
		key := e.CausalKey()
		if obj, ok := r.store[key]; ok {
			eventKindKey := e.CanonicalGroupKind()
			if _, ok := cacheFrame[eventKindKey]; !ok {
				cacheFrame[eventKindKey] = make(map[types.NamespacedName]*unstructured.Unstructured)
			}
			cacheFrame[eventKindKey][types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}] = obj
		} else {
			return nil, fmt.Errorf("object not found in store: %#v", key)
		}
	}
	return cacheFrame, nil
}

// inferReconcileRequestFromReadset produces the top-level reconcile.Request that a traced reconcile invocation was
// called with. This data isn't part of our tracing instrumentation so we use a heuristic to infer it.
func (r *Builder) inferReconcileRequestFromReadset(controllerID string, readset []event.Event) (reconcile.Request, error) {
	for _, e := range readset {
		// Assumption: reconcile routines are invoked upon a Resource that shares the same name (Kind)
		// as the controller that is managing it.
		kindForController := r.reconcilerToKind[controllerID]
		eventKindKey := e.CanonicalGroupKind()
		if eventKindKey == kindForController || e.Kind == controllerID {
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
