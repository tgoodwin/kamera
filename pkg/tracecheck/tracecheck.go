package tracecheck

import (
	"fmt"
	"time"

	"github.com/samber/lo"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/replay"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/tracegen"
	"github.com/tgoodwin/kamera/pkg/util"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var MaxDepth int

type Client client.Client
type Reconciler reconcile.Reconciler

type ReconcilerConstructor func(client Client) Reconciler

type testEmitter interface {
	event.Emitter
	Dump(frameID string) []string
}

type TraceChecker struct {
	reconcilerFactory map[string]ReconcilerConstructor
	ResourceDeps      ResourceDeps
	manager           *manager
	scheme            *runtime.Scheme

	// this just determines which top-level object a reconciler is triggered with
	reconcilerToKind map[string]string

	knowledgeManager *KnowledgeManager

	// TODO move this elsewhere
	builder *replay.Builder
	mode    string

	emitter testEmitter
}

func NewTraceChecker(scheme *runtime.Scheme) *TraceChecker {
	readDeps := make(ResourceDeps)
	snapStore := snapshot.NewStore()

	vStore := newVersionStore(snapStore)
	KnowledgeManager := NewKnowledgeManager(snapStore)

	mgr := &manager{
		versionStore: vStore,
		effects:      make(map[string]reconcileEffects),
		scheme:       scheme,
	}

	return &TraceChecker{
		reconcilerFactory: make(map[string]ReconcilerConstructor),
		ResourceDeps:      readDeps,
		manager:           mgr,
		scheme:            scheme,

		knowledgeManager: KnowledgeManager,

		emitter: event.NewInMemoryEmitter(),

		mode: "standalone",

		// TODO refactor
		reconcilerToKind: make(map[string]string),
	}
}

func FromBuilder(b *replay.Builder) *TraceChecker {
	readDeps := make(ResourceDeps)

	//snapshot store
	snapshotStore := snapshot.NewStore()
	vStore := newVersionStore(snapshotStore)

	store := b.Store()
	// eventsByReconcile := lo.GroupBy(b.Events(), func(e event.Event) string {
	// 	return e.ReconcileID
	// })
	fmt.Println("---Store Contents---")
	for k := range store {
		fmt.Printf("Key: %s\n", k)
	}
	fmt.Println("---End Store Contents---")
	things := make([]joinRecord, 0)
	for _, e := range b.Events() {
		ckey := e.CausalKey()
		unstructuredObj, ok := store[ckey]
		if !ok {
			fmt.Println("Could not find object for causal key: ", ckey)
			continue
		}
		if err := snapshotStore.StoreObject(unstructuredObj); err != nil {
			panic(fmt.Sprintf("storing object: %s", err))
		}

		vHash := vStore.Publish(unstructuredObj)
		ikey := snapshot.IdentityKey{Group: ckey.Group, Kind: ckey.Kind, ObjectID: ckey.ObjectID}
		nsName := types.NamespacedName{Namespace: unstructuredObj.GetNamespace(), Name: unstructuredObj.GetName()}

		// this is logically representing a "join" between the sleeve event model
		// and the tracecheck model which includes the versionHash

		// we want to be able to produce an objectVersion for a given traced reconcile
		// so, in order to do so, the TraceChecker needs to have some knowledge about the
		// reconcileIDs that are present in the trace and an ability to map from the reconcileID
		// "frame" to a set of objectVersions that were either read within that frame or produced by that frame.
		// this is the purpose of the "thing" struct
		thing := joinRecord{
			event:       e,
			ikey:        ikey,
			versionHash: vHash,
			nsName:      nsName,
		}
		things = append(things, thing)

		// now build the read deps
		if event.IsReadOp(event.OperationType(e.OpType)) {
			if _, ok := readDeps[e.Kind]; !ok {
				readDeps[e.Kind] = util.NewSet[string]()
			}
			readDeps[e.Kind].Add(e.ControllerID)
		}
	}

	converter := newConverter(things)

	knowledgeManager := NewKnowledgeManager(snapshotStore)

	mgr := &manager{
		versionStore:  vStore,
		effects:       make(map[string]reconcileEffects),
		converterImpl: converter,
		// TODO handle scheme properly
		scheme: nil,
	}

	return &TraceChecker{
		reconcilerFactory: make(map[string]ReconcilerConstructor),
		ResourceDeps:      readDeps,
		manager:           mgr,
		mode:              "traced",

		knowledgeManager: knowledgeManager,

		builder:          b,
		reconcilerToKind: make(map[string]string),
	}
}

func (tc *TraceChecker) GetStartStateFromObject(obj client.Object, dependentControllers ...string) StateNode {
	r, err := snapshot.AsRecord(obj, "start")
	if err != nil {
		panic("converting to unstructured: " + err.Error())
	}
	u, err := r.ToUnstructured()
	if err != nil {
		panic("converting to unstructured: " + err.Error())
	}
	vHash := tc.manager.versionStore.Publish(u)
	sleeveObjectID := tag.GetSleeveObjectID(obj)
	gvk := util.GetGroupVersionKind(obj)
	ikey := snapshot.IdentityKey{Group: gvk.Group, Kind: gvk.Kind, ObjectID: sleeveObjectID}

	// HACK TODO REFACTOR
	if tc.builder == nil {
		tc.builder = &replay.Builder{ReconcilerIDs: util.NewSet(dependentControllers...)}
	}

	dependent := lo.Map(dependentControllers, func(s string, _ int) PendingReconcile {
		return PendingReconcile{
			ReconcilerID: s,
			Request: reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: obj.GetNamespace(),
					Name:      obj.GetName(),
				},
			},
		}
	})

	key := snapshot.NewCompositeKeyWithGroup(gvk.Group, ikey.Kind, obj.GetNamespace(), obj.GetName(), sleeveObjectID)

	return StateNode{
		Contents: StateSnapshot{
			contents: ObjectVersions{key: vHash},
			KindSequences: KindSequences{
				util.CanonicalGroupKind(ikey.Group, ikey.Kind): 1,
			},
			stateEvents: []StateEvent{
				{
					ReconcileID: "TOP",
					Timestamp:   event.FormatTimeStr(time.Now()),
					Sequence:    1,
					Effect:      newEffect(key, vHash, event.CREATE),
				},
			},
		},
		PendingReconciles: dependent,
	}
}

func (tc *TraceChecker) AddReconciler(reconcilerID string, constructor ReconcilerConstructor) {
	tc.reconcilerFactory[reconcilerID] = constructor
}

func (tc *TraceChecker) AssignReconcilerToKind(reconcilerID, kind string) {
	gk := util.ParseGroupKind(kind)
	tc.reconcilerToKind[reconcilerID] = util.CanonicalGroupKind(gk.Group, gk.Kind)
}

func (tc *TraceChecker) AddEmitter(emitter testEmitter) {
	tc.emitter = emitter
}

func (tc *TraceChecker) instantiateReconcilers() map[string]*ReconcilerContainer {
	if tc.emitter == nil {
		panic("Must set emitter on TraceChecker before instantiating reconcilers")
	}
	out := make(map[string]*ReconcilerContainer)
	for reconcilerID, constructor := range tc.reconcilerFactory {
		h, err := tc.builder.BuildHarness(reconcilerID)
		if err != nil {
			panic(fmt.Sprintf("building harness: %s", err))
		}

		// initialize the client's frame manager with traced frames
		frameManager := replay.NewFrameManager(h.FrameData())
		replayClient := replay.NewClient(
			reconcilerID,
			tc.scheme,
			frameManager,
			tc.manager, // this is what calls RecordEffect
		)

		wrappedClient := tracegen.New(
			replayClient,
			reconcilerID,
			tc.emitter,
			tracegen.NewContextTracker(
				reconcilerID,
				tc.emitter,
				replay.FrameIDFromContext,
			),
		)
		r := constructor(wrappedClient)

		container := &ReconcilerContainer{
			Name:           reconcilerID,
			Strategy:       NewControllerRuntimeStrategy(r, frameManager, tc.manager, reconcilerID),
			versionManager: tc.manager,
			effectReader:   tc.manager,
		}
		out[reconcilerID] = container

	}
	return out
}

func (tc *TraceChecker) ShowDeps() {
	for k, v := range tc.ResourceDeps {
		fmt.Printf("Kind: %s, Reconcilers: %v\n", k, v)
	}
}

func (tc *TraceChecker) GetStartState() StateNode {
	return tc.manager.getStart()
}

func (tc *TraceChecker) PrintState(s StateNode) {
	ov := s.Objects()
	for k, v := range ov {
		fmt.Printf("ObjectKey: %s, VersionHash: %s\n", k, v)
	}
	fmt.Println("Pending Reconciles: ", s.PendingReconciles)
}

// Deprecated: NewExplorer is deprecated. Use ExplorerBuilder instead.
func (tc *TraceChecker) NewExplorer(maxDepth int) *Explorer {
	if len(tc.ResourceDeps) == 0 {
		panic("Warning: No resource dependencies found")
	}

	reconcilers := tc.instantiateReconcilers()
	if len(reconcilers) != len(tc.builder.ReconcilerIDs) {
		panic("building explorer: not all traced reconcilers were instantiated. forget to add them?")
	}

	// if constructing an explorer from trace data, load a knowledge manager.
	// otherwise we need to skip this step. TODO refactor
	var knowledgeManager *EventKnowledge
	// TODO should be able to just check if the builder is nil or not
	if tc.mode == "traced" {
		knowledgeManager = NewEventKnowledge(tc.builder.Store())
		knowledgeManager.Load(tc.builder.Events())
	}

	return &Explorer{
		reconcilers:  reconcilers,
		dependencies: tc.ResourceDeps,
		config: &ExploreConfig{
			MaxDepth: maxDepth,
		},

		triggerManager: NewTriggerManager(
			tc.ResourceDeps,
			tc.reconcilerToKind,
			tc.manager.snapStore,
		),

		knowledgeManager: knowledgeManager,
		versionManager:   tc.manager.versionStore,
	}
}

func (tc *TraceChecker) SummarizeResults(result *Result) {
	for i, sn := range result.ConvergedStates {
		fmt.Println("Result #", i+1)
		fmt.Println("result converged state: ", sn.State.Objects())
		uniquePaths := GetUniquePaths(sn.Paths)
		fmt.Println("unique paths to this state: ", len(uniquePaths))
		for i, path := range uniquePaths {
			fmt.Println("Path #", i+1)
			path.Summarize()
		}
	}
}

func (tc *TraceChecker) DiffStates(a, b StateNode) []string {
	diffs := make([]string, 0)
	for key, vHash := range a.Objects() {
		currKind := key.CanonicalGroupKind()
		for otherKey, otherHash := range b.Objects() {
			if otherKey.CanonicalGroupKind() == currKind {
				// disregarding ID, let's identify the difference between teh two objects of kind
				if diff := tc.manager.Diff(&vHash, &otherHash); diff != "" {
					diffs = append(diffs, diff)
				}
			}
		}
	}
	return diffs
}

func (tc *TraceChecker) Unique(results []StateNode) {
	unique := util.NewAnySet(func(a, b StateNode) bool {
		diffs := tc.DiffStates(a, b)
		return len(diffs) == 0
	})
	for _, res := range results {
		unique.Add(res)
	}
	fmt.Println("# unique: ", len(unique.Items()))
}
