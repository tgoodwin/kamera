package tracecheck

import (
	"fmt"

	sleeveclient "github.com/tgoodwin/sleeve/pkg/client"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/replay"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
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

type TraceChecker struct {
	reconcilers  map[string]ReconcilerConstructor
	resourceDeps resourceDeps
	manager      *manager
	scheme       *runtime.Scheme

	// TODO move this elsewhere
	builder *replay.Builder

	emitter event.Emitter
}

func FromBuilder(b *replay.Builder) *TraceChecker {
	vStore := newVersionStore()
	readDeps := make(resourceDeps)
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
		resourceDeps: readDeps,
		manager:      mgr,

		builder: b,
	}
}

func (tc *TraceChecker) AddReconciler(reconcilerID string, constructor ReconcilerConstructor) {
	tc.reconcilers[reconcilerID] = constructor
}

func (tc *TraceChecker) AddEmitter(emitter event.Emitter) {
	tc.emitter = emitter
}

func (tc *TraceChecker) instantiateReconcilers() map[string]reconciler {
	if tc.emitter == nil {
		panic("Must set emitter on TraceChecker before instantiating reconcilers")
	}
	out := make(map[string]reconciler)
	harnesses := make(map[string]*replay.Harness)
	for reconcilerID, constructor := range tc.reconcilers {
		frameManager := replay.NewFrameManager()
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

		// TODO configure file emitter here
		rImpl := reconcileImpl{
			Name:           reconcilerID,
			Reconciler:     r,
			versionManager: tc.manager,
			effectReader:   tc.manager,
			frameInserter:  frameManager,
		}
		out[reconcilerID] = &rImpl

		// build the harness
		h, err := tc.builder.BuildHarness(reconcilerID)
		if err != nil {
			fmt.Println("Error building harness: ", err)
		}
		harnesses[reconcilerID] = h
	}
	return out
}

func (tc *TraceChecker) ShowDeps() {
	for k, v := range tc.resourceDeps {
		fmt.Printf("Kind: %s, Reconcilers: %v\n", k, v)
	}
}

func (tc *TraceChecker) GetStartState() StateNode {
	return tc.manager.getStart()
}

func (tc *TraceChecker) PrintState(s StateNode) {
	ov := s.ObjectVersions
	for k, v := range ov {
		fmt.Printf("ObjectKey: %s, VersionHash: %s\n", k, v)
	}
	fmt.Println("Pending Reconciles: ", s.PendingReconciles)
}

func (tc *TraceChecker) NewExplorer(maxDepth int) *Explorer {
	return &Explorer{
		reconcilers:  tc.instantiateReconcilers(),
		dependencies: tc.resourceDeps,
		maxDepth:     maxDepth,
	}
}

func (tc *TraceChecker) showDeltas(prevState, currState *StateNode) {
	changes := currState.action.Changes
	for k, v := range changes {
		if prevVersion, ok := prevState.ObjectVersions[k]; ok {
			fmt.Printf("Delta for object %s-%s:\n", k.Kind, util.Shorter(k.ObjectID))
			delta := tc.manager.Diff(&prevVersion, &v)
			if currState.action.ControllerID == "Deployment" && k.Kind == "Deployment" {
				panic("ahhh")
			}
			fmt.Println(delta)
		} else {
			// creation event
			fmt.Println("New Object:")
			fmt.Println(tc.manager.Diff(nil, &v))
		}
	}
}

func (tc *TraceChecker) SummarizeFromRoot(sn *StateNode) {
	if sn.parent != nil {
		tc.SummarizeFromRoot(sn.parent)
		sn.Summarize()
		fmt.Println("Deltas:")
		tc.showDeltas(sn.parent, sn)
		// effects := tc.manager.effects[sn.action.FrameID]
		// for _, eff := range effects.reads {
		// 	fmt.Println("Read: ", eff)
		// }
		// for _, eff := range effects.writes {
		// 	fmt.Println("Write: ", eff)
		// }
	} else {
		fmt.Println("Root StateNode")
		sn.Summarize()
	}
}

func (tc *TraceChecker) EvalPredicate(sn StateNode, p replay.Predicate) bool {
	ov := sn.ObjectVersions
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
