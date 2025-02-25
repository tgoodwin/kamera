package tracecheck

import (
	"fmt"

	sleeveclient "github.com/tgoodwin/sleeve/pkg/client"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/replay"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"github.com/tgoodwin/sleeve/pkg/tag"
	"github.com/tgoodwin/sleeve/pkg/util"
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
	reconcilers  map[string]ReconcilerConstructor
	ResourceDeps ResourceDeps
	manager      *manager
	scheme       *runtime.Scheme

	// this just determines which top-level object a reconciler is triggered with
	reconcilerToKind map[string]string

	// TODO move this elsewhere
	builder *replay.Builder
	mode    string

	emitter testEmitter
}

func NewTraceChecker(scheme *runtime.Scheme) *TraceChecker {
	vStore := newVersionStore()
	readDeps := make(ResourceDeps)
	lc := snapshot.NewLifecycleContainer()

	mgr := &manager{
		versionStore:       vStore,
		LifecycleContainer: lc,
		effects:            make(map[string]reconcileEffects),
	}

	return &TraceChecker{
		reconcilers:  make(map[string]ReconcilerConstructor),
		ResourceDeps: readDeps,
		manager:      mgr,
		scheme:       scheme,

		emitter: event.NewInMemoryEmitter(),

		mode: "standalone",

		builder: nil,

		// TODO refactor
		reconcilerToKind: make(map[string]string),
	}
}

func FromBuilder(b *replay.Builder) *TraceChecker {
	vStore := newVersionStore()
	readDeps := make(ResourceDeps)
	lc := snapshot.NewLifecycleContainer()

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
		versionValue, ok := store[ckey]
		if !ok {
			fmt.Println("Could not find object for causal key: ", ckey)
			continue
		}
		vHash := vStore.Publish(versionValue)
		ikey := snapshot.IdentityKey{Kind: ckey.Kind, ObjectID: ckey.ObjectID}
		lc.InsertSynthesizedVersion(ikey, vHash, e.ReconcileID)

		nsName := types.NamespacedName{Namespace: versionValue.GetNamespace(), Name: versionValue.GetName()}

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
		if event.IsReadOp(e) {
			if _, ok := readDeps[e.Kind]; !ok {
				readDeps[e.Kind] = util.NewSet[string]()
			}
			readDeps[e.Kind].Add(e.ControllerID)
		}
	}

	converter := newConverter(things)

	mgr := &manager{
		versionStore:       vStore,
		LifecycleContainer: lc,
		effects:            make(map[string]reconcileEffects),
		converterImpl:      converter,
	}

	return &TraceChecker{
		reconcilers:  make(map[string]ReconcilerConstructor),
		ResourceDeps: readDeps,
		manager:      mgr,
		mode:         "traced",

		builder:          b,
		reconcilerToKind: make(map[string]string),
	}
}

func (tc *TraceChecker) GetStartStateFromObject(obj client.Object, dependentControllers ...string) StateNode {
	r := snapshot.AsRecord(obj, "start").ToUnstructured()
	vHash := tc.manager.versionStore.Publish(r)
	sleeveObjectID := tag.GetSleeveObjectID(obj)
	ikey := snapshot.IdentityKey{Kind: util.GetKind(obj), ObjectID: sleeveObjectID}
	tc.manager.InsertSynthesizedVersion(ikey, vHash, "start")

	// HACK TODO REFACTOR
	tc.builder = &replay.Builder{
		ReconcilerIDs: util.NewSet(dependentControllers...),
	}

	return StateNode{
		objects:           ObjectVersions{ikey: vHash},
		PendingReconciles: dependentControllers,
	}
}

func (tc *TraceChecker) AddReconciler(reconcilerID string, constructor ReconcilerConstructor) {
	tc.reconcilers[reconcilerID] = constructor
}

func (tc *TraceChecker) AssignReconcilerToKind(reconcilerID, kind string) {
	tc.reconcilerToKind[reconcilerID] = kind
}

func (tc *TraceChecker) AddEmitter(emitter testEmitter) {
	tc.emitter = emitter
}

func (tc *TraceChecker) instantiateReconcilers() map[string]ReconcilerContainer {
	if tc.emitter == nil {
		panic("Must set emitter on TraceChecker before instantiating reconcilers")
	}
	out := make(map[string]ReconcilerContainer)
	for reconcilerID, constructor := range tc.reconcilers {
		h, err := tc.builder.BuildHarness(reconcilerID)
		if err != nil {
			panic(fmt.Sprintf("building harness: %s", err))
		}

		// initialize the client's frame manager with traced frames
		frameManager := replay.NewFrameManager(h.FrameData())
		replayClient := replay.NewClient(
			reconcilerID,
			tc.scheme,
			frameManager.Frames,
			tc.manager,
		)

		wrappedClient := sleeveclient.New(
			replayClient,
			reconcilerID,
			tc.emitter,
			sleeveclient.NewContextTracker(
				reconcilerID,
				tc.emitter,
				replay.FrameIDFromContext,
			),
		)
		r := constructor(wrappedClient)

		kindforReconciler, ok := tc.reconcilerToKind[reconcilerID]
		if !ok {
			panic(fmt.Sprintf("No kind assigned to reconciler: %s", reconcilerID))
		}

		rImpl := reconcileImpl{
			Name:           reconcilerID,
			For:            kindforReconciler,
			Reconciler:     r,
			versionManager: tc.manager,
			effectReader:   tc.manager,
			frameInserter:  frameManager,
		}
		container := ReconcilerContainer{
			reconciler: &rImpl,
			harness:    h,
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
	var knowledgeManager *GlobalKnowledge
	if tc.mode == "traced" {
		knowledgeManager = NewGlobalKnowledge(tc.builder.Store())
		knowledgeManager.Load(tc.builder.Events())
	}

	return &Explorer{
		reconcilers:  reconcilers,
		dependencies: tc.ResourceDeps,
		maxDepth:     maxDepth,

		knowledgeManager: knowledgeManager,
	}
}

func (tc *TraceChecker) EvalPredicate(sn StateNode, p replay.Predicate) bool {
	ov := sn.Objects()
	for k, v := range ov {
		// get the full object value
		fullObj := tc.manager.Resolve(v)
		if passed := p(fullObj); passed {
			fmt.Println("Predicate satisfied for object: ", k)
			return true
		}
	}
	return false
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
		currKind := key.Kind
		for otherKey, otherHash := range b.Objects() {
			if otherKey.Kind == currKind {
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
