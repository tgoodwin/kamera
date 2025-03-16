package tracecheck

import (
	"fmt"
	"time"

	"github.com/samber/lo"
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

type ExplorerBuilder struct {
	reconcilers      map[string]ReconcilerConstructor
	resourceDeps     ResourceDeps
	scheme           *runtime.Scheme
	maxDepth         int
	stalenessDepth   int
	emitter          testEmitter
	snapStore        *snapshot.Store
	reconcilerToKind map[string]string

	// for replay mode
	builder *replay.Builder
}

func NewExplorerBuilder(scheme *runtime.Scheme) *ExplorerBuilder {
	return &ExplorerBuilder{
		reconcilers:      make(map[string]ReconcilerConstructor),
		resourceDeps:     make(ResourceDeps),
		scheme:           scheme,
		snapStore:        snapshot.NewStore(),
		maxDepth:         10, // Default value
		stalenessDepth:   0,  // Default value
		reconcilerToKind: make(map[string]string),
	}
}

func (b *ExplorerBuilder) WithReconciler(id string, constructor ReconcilerConstructor) *ExplorerBuilder {
	b.reconcilers[id] = constructor
	return b
}

func (b *ExplorerBuilder) WithResourceDep(kind string, reconcilerIDs ...string) *ExplorerBuilder {
	if _, ok := b.resourceDeps[kind]; !ok {
		b.resourceDeps[kind] = util.NewSet[string]()
	}
	for _, id := range reconcilerIDs {
		b.resourceDeps[kind].Add(id)
	}
	return b
}

func (b *ExplorerBuilder) WithMaxDepth(depth int) *ExplorerBuilder {
	b.maxDepth = depth
	return b
}

func (b *ExplorerBuilder) WithStalenessDepth(depth int) *ExplorerBuilder {
	b.stalenessDepth = depth
	return b
}

func (b *ExplorerBuilder) WithEmitter(emitter testEmitter) *ExplorerBuilder {
	b.emitter = emitter
	return b
}

func (b *ExplorerBuilder) WithReplayBuilder(builder *replay.Builder) *ExplorerBuilder {
	b.builder = builder
	return b
}

func (b *ExplorerBuilder) AssignReconcilerToKind(reconcilerID, kind string) *ExplorerBuilder {
	b.reconcilerToKind[reconcilerID] = kind
	return b
}

func (b *ExplorerBuilder) instantiateReconcilers(mgr *manager) map[string]ReconcilerContainer {
	containers := make(map[string]ReconcilerContainer)

	for reconcilerID, constructor := range b.reconcilers {
		var frameManager *replay.FrameManager
		if b.builder != nil {
			// Build harness from the replay builder
			h, err := b.builder.BuildHarness(reconcilerID)
			if err != nil {
				// Handle error
				panic("building harness: " + err.Error())
			}

			// Create frame manager
			frameManager = replay.NewFrameManager(h.FrameData())
		} else {
			// standalone mode
			// this just constructs a map of reconcileID -> CacheFrame
			frameManager = replay.NewFrameManager(nil)
		}

		// Create replay client
		replayClient := replay.NewClient(
			reconcilerID,
			b.scheme,
			frameManager,
			mgr,
		)

		// Create wrapped client
		wrappedClient := sleeveclient.New(
			replayClient,
			reconcilerID,
			b.emitter,
			sleeveclient.NewContextTracker(
				reconcilerID,
				b.emitter,
				replay.FrameIDFromContext,
			),
		)

		// Create reconciler
		r := constructor(wrappedClient)

		// Get kind for reconciler
		kindForReconciler, ok := b.reconcilerToKind[reconcilerID]
		if !ok {
			// Skip or handle error
			panic(fmt.Sprintf("No kind assigned to reconciler: %s", reconcilerID))
		}

		// Create reconciler implementation
		rImpl := &reconcileImpl{
			Name:           reconcilerID,
			For:            kindForReconciler,
			Reconciler:     r,
			versionManager: mgr,
			effectReader:   mgr,
			frameInserter:  frameManager,
		}

		// Create container
		container := ReconcilerContainer{
			reconcileImpl: rImpl,
		}

		containers[reconcilerID] = container
	}

	return containers
}

func (b *ExplorerBuilder) NewStateEventBuilder() *StateEventBuilder {
	return NewStateEventBuilder(b.snapStore)
}

func (b *ExplorerBuilder) NewStateClassifier() *StateClassifier {
	return NewStateClassifier(
		newVersionStore(b.snapStore),
	)
}

func (b *ExplorerBuilder) GetStartStateFromObject(obj client.Object, dependentControllers ...string) StateNode {
	r, err := snapshot.AsRecord(obj, "start")
	if err != nil {
		panic("converting to unstructured: " + err.Error())
	}
	u := r.ToUnstructured()
	vHash := b.snapStore.PublishWithStrategy(u, snapshot.AnonymizedHash)
	sleeveObjectID := tag.GetSleeveObjectID(obj)
	ikey := snapshot.IdentityKey{Kind: util.GetKind(obj), ObjectID: sleeveObjectID}

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

	key := snapshot.NewCompositeKey(ikey.Kind, obj.GetNamespace(), obj.GetName(), sleeveObjectID)

	return StateNode{
		Contents: NewStateSnapshot(
			ObjectVersions{key: vHash},
			map[string]int64{
				ikey.Kind: 1,
			},
			[]StateEvent{
				{
					ReconcileID: "TOP",
					Timestamp:   event.FormatTimeStr(time.Now()),
					Sequence:    1,
					effect: newEffect(
						key,
						vHash,
						event.CREATE,
					),
				},
			},
		),
		PendingReconciles: dependent,
	}
}

func (b *ExplorerBuilder) Build(mode string) (*Explorer, error) {
	// Validate configuration
	if len(b.resourceDeps) == 0 {
		return nil, fmt.Errorf("no resource dependencies defined")
	}

	if b.emitter == nil {
		return nil, fmt.Errorf("no emitter specified")
	}

	// Create version store and knowledge manager
	vStore := newVersionStore(b.snapStore)

	// Create manager
	mgr := &manager{
		versionStore: vStore,
		effects:      make(map[string]reconcileEffects),

		snapStore: b.snapStore,

		// effectContext tracks the state of the world at the time of reconcile
		// and this is separate from snapshot store because we want this context
		// to not be shared across branches of the exploration tree.
		effectRKeys: make(map[string]util.Set[snapshot.ResourceKey]),

		// effectIKeys tracks the identity keys that were read or written
		// during a reconcile operation.
		effectIKeys: make(map[string]util.Set[snapshot.IdentityKey]),

		// resourceValdiator mimics the behavior of the API
		// server in terms of rejecting operations that conflict
		// with the current state of the world.
		// It needs to be hydrated with the current state of the world
		// before it can be used and uses the snapshot store as the source of truth.
		// resourceValidator: replay.NewResourceConflictManager(b.snapStore.ResourceKeys()),
	}

	// Initialize reconcilers with appropriate clients
	reconcilers := b.instantiateReconcilers(mgr)

	// Create knowledge manager if using replay builder
	var knowledgeManager *EventKnowledge
	if mode == "traced" && b.builder != nil {
		knowledgeManager = NewEventKnowledge(b.builder.Store())
		if err := knowledgeManager.Load(b.builder.Events()); err != nil {
			return nil, fmt.Errorf("loading events: %w", err)
		}
	}

	// Create trigger manager
	triggerManager := NewTriggerManager(
		b.resourceDeps,
		b.reconcilerToKind,
		mgr.snapStore,
	)

	// Construct the Explorer
	explorer := &Explorer{
		reconcilers:      reconcilers,
		dependencies:     b.resourceDeps,
		maxDepth:         b.maxDepth,
		stalenessDepth:   b.stalenessDepth,
		triggerManager:   triggerManager,
		knowledgeManager: knowledgeManager,

		effectContextManager: mgr,
	}

	return explorer, nil
}
