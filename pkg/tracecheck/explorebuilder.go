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
	"github.com/tgoodwin/kamera/sleevectrl/pkg/controller"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var CleanupReconcilerID = "CleanupReconciler"

const (
	deploymentControllerID = "DeploymentController"
)

type ExplorerBuilder struct {
	reconcilers                map[string]ReconcilerConstructor
	recorderInjectedStrategies map[string]func(recorder replay.EffectRecorder) Strategy
	resourceDeps               ResourceDeps
	scheme                     *runtime.Scheme
	emitter                    testEmitter
	snapStore                  *snapshot.Store
	reconcilerToKind           map[string]string

	priorityBuilder *PriorityStrategyBuilder

	config *ExploreConfig

	// for replay mode
	builder *replay.Builder
}

func NewExplorerBuilder(scheme *runtime.Scheme) *ExplorerBuilder {
	utilruntime.Must(appsv1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))

	builder := &ExplorerBuilder{
		reconcilers:                make(map[string]ReconcilerConstructor),
		recorderInjectedStrategies: make(map[string]func(recorder replay.EffectRecorder) Strategy),
		resourceDeps:               make(ResourceDeps),
		scheme:                     scheme,
		snapStore:                  snapshot.NewStore(),
		reconcilerToKind:           make(map[string]string),

		config: &ExploreConfig{
			MaxDepth:                10,
			KindBoundsPerReconciler: make(map[string]ReconcilerConfig),
		},
	}

	builder.registerCoreControllers()

	return builder
}

func (b *ExplorerBuilder) WithReconciler(id string, constructor ReconcilerConstructor) *ExplorerBuilder {
	b.reconcilers[id] = constructor
	return b
}

func (b *ExplorerBuilder) WithCustomStrategy(id string, strategyFunc func(recorder replay.EffectRecorder) Strategy) *ExplorerBuilder {
	{
		b.recorderInjectedStrategies[id] = strategyFunc
		return b
	}
}

func (b *ExplorerBuilder) WithDebug() {
	b.config.debug = true
}

func (b *ExplorerBuilder) BreakEarly() {
	b.config.breakEarly = true
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

func (b *ExplorerBuilder) WithPriorityStrategy(p *PriorityStrategyBuilder) *ExplorerBuilder {
	b.priorityBuilder = p
	return b
}

func (b *ExplorerBuilder) WithMaxDepth(depth int) *ExplorerBuilder {
	b.config.MaxDepth = depth
	return b
}

func (b *ExplorerBuilder) ExploreStaleStates() *ExplorerBuilder {
	b.config.useStaleness = 1
	return b
}

func (b *ExplorerBuilder) WithKindBounds(reconcilerID string, rc ReconcilerConfig) *ExplorerBuilder {
	b.config.KindBoundsPerReconciler[reconcilerID] = rc
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

// TODO make how we handle kinds more type safe
func (b *ExplorerBuilder) AssignReconcilerToKind(reconcilerID, kind string) *ExplorerBuilder {
	b.reconcilerToKind[reconcilerID] = kind
	return b
}

func (b *ExplorerBuilder) registerCoreControllers() {
	// Deployment Controller
	b.WithReconciler("DeploymentController", func(c Client) Reconciler {
		return &controller.DeploymentReconciler{
			Client: c,
			Scheme: b.scheme,
		}
	})
	b.AssignReconcilerToKind(deploymentControllerID, "Deployment")
	b.WithResourceDep("Deployment", deploymentControllerID)
	b.WithResourceDep("ReplicaSet", deploymentControllerID)

	// ReplicaSet Controller
	b.WithReconciler("ReplicaSetController", func(c Client) Reconciler {
		return &controller.ReplicaSetReconciler{
			Client: c,
			Scheme: b.scheme,
		}
	})
	b.AssignReconcilerToKind("ReplicaSetController", "ReplicaSet")
	b.WithResourceDep("ReplicaSet", "ReplicaSetController")
	b.WithResourceDep("Pod", "ReplicaSetController")
	b.WithResourceDep("Deployment", "ReplicaSetController")

	// Pod Lifecycle Controller, e.g. "fake kubelet"
	b.WithReconciler("PodLifecycleController", func(c Client) Reconciler {
		return controller.NewPodLifecycleReconciler(
			c,
			b.scheme,
			controller.NewDefaultPodLifecycleFactory(),
			0,
		)
	})
	b.AssignReconcilerToKind("PodLifecycleController", "Pod")
	b.WithResourceDep("Pod", "PodLifecycleController")
	b.WithResourceDep("PodTemplate", "PodLifecycleController")
	b.WithResourceDep("ReplicaSet", "PodLifecycleController")
	b.WithResourceDep("Deployment", "PodLifecycleController")
	b.WithResourceDep("StatefulSet", "PodLifecycleController")
	b.WithResourceDep("DaemonSet", "PodLifecycleController")
	b.WithResourceDep("Job", "PodLifecycleController")
	b.WithResourceDep("CronJob", "PodLifecycleController")

	b.WithReconciler("ServiceController", func(c Client) Reconciler {
		return &controller.ServiceReconciler{
			Client: c,
			Scheme: b.scheme,
		}
	})
	b.AssignReconcilerToKind("ServiceController", "Service")
	b.WithResourceDep("Service", "ServiceController")
	b.WithResourceDep("Endpoints", "ServiceController")

	// endpoints controller
	b.WithReconciler("EndpointsController", func(c Client) Reconciler {
		return &controller.EndpointsReconciler{
			Client: c,
			Scheme: b.scheme,
		}
	})
	b.AssignReconcilerToKind("EndpointsController", "Endpoints")
	b.WithResourceDep("Endpoints", "EndpointsController")
	b.WithResourceDep("Service", "EndpointsController")
}

func (b *ExplorerBuilder) instantiateReconcilers(mgr *manager) map[string]*ReconcilerContainer {
	containers := make(map[string]*ReconcilerContainer)

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
		// wrappedClient := tracegen.New(
		// 	replayClient,
		// 	reconcilerID,
		// 	b.emitter,
		// 	tracegen.NewContextTracker(
		// 		reconcilerID,
		// 		b.emitter,
		// 		replay.FrameIDFromContext,
		// 	),
		// )

		// Create reconciler
		r := constructor(replayClient)

		// Create reconciler implementation
		rImpl := Wrap(reconcilerID, r, mgr, frameManager, mgr)

		containers[reconcilerID] = rImpl
	}

	// for strategies where we need to inject the recorder directly (e.g. Knative)
	for name, constructor := range b.recorderInjectedStrategies {
		strategy := constructor(mgr)
		container := &ReconcilerContainer{
			Name:           name,
			Strategy:       strategy,
			effectReader:   mgr,
			versionManager: mgr,
		}
		containers[container.Name] = container
	}

	return containers
}

// instantiateCleanupReconciler adds a reconciler to the system that handles
// actual deletion of resources after they have been "marked" for deletion. In reality,
// the APIServer would handle this, but we need to simulate this behavior in our system.
func (b *ExplorerBuilder) instantiateCleanupReconciler(mgr *manager) *ReconcilerContainer {
	fm := replay.NewFrameManager(nil)
	replayClient := replay.NewClient(
		CleanupReconcilerID,
		b.scheme,
		fm,
		mgr,
	)
	wrappedClient := tracegen.New(
		replayClient,
		CleanupReconcilerID,
		b.emitter,
		tracegen.NewContextTracker(
			CleanupReconcilerID,
			b.emitter,
			replay.FrameIDFromContext,
		),
	)
	r := &controller.FinalizerReconciler{
		Client:   wrappedClient,
		Recorder: mgr,
	}
	container := &ReconcilerContainer{
		Name:           CleanupReconcilerID,
		Strategy:       &ControllerRuntimeStrategy{Reconciler: r, frameInserter: fm, reconcilerName: CleanupReconcilerID, effectReader: mgr},
		effectReader:   mgr,
		versionManager: mgr,
	}
	return container
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
	u, err := r.ToUnstructured()
	if err != nil {
		panic("converting to unstructured: " + err.Error())
	}
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
			KindSequences{
				ikey.Kind: 1,
			},
			[]StateEvent{
				{
					ReconcileID: "TOP",
					Timestamp:   event.FormatTimeStr(time.Now()),
					Sequence:    1,
					Effect: newEffect(
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
		b.emitter = event.NewInMemoryEmitter()
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
	cleanupReconciler := b.instantiateCleanupReconciler(mgr)
	reconcilers[CleanupReconcilerID] = cleanupReconciler

	// Create knowledge manager if using replay builder
	var knowledgeManager *EventKnowledge
	if mode == "traced" && b.builder != nil {
		knowledgeManager = NewEventKnowledge(b.builder.Store())
		if err := knowledgeManager.Load(b.builder.Events()); err != nil {
			return nil, fmt.Errorf("loading events: %w", err)
		}
	}

	if b.priorityBuilder == nil {
		b.priorityBuilder = NewPriorityStrategyBuilder()
	}

	// Create trigger manager
	triggerManager := NewTriggerManager(
		b.resourceDeps,
		b.reconcilerToKind,
		mgr.snapStore,
	)

	// Construct the Explorer
	explorer := &Explorer{
		reconcilers:          reconcilers,
		dependencies:         b.resourceDeps,
		triggerManager:       triggerManager,
		knowledgeManager:     knowledgeManager,
		config:               b.config,
		effectContextManager: mgr,
		versionManager:       vStore,

		priorityHandler: b.priorityBuilder.Build(b.snapStore),
	}

	return explorer, nil
}

func (b *ExplorerBuilder) BuildLensManager(traceFilePath string) (*LensManager, error) {
	traces, err := b.ParseJSONLTrace(traceFilePath)
	if err != nil {
		return nil, fmt.Errorf("parsing trace file: %w", err)
	}
	rollup := CausalRollup(traces)
	mgr := &manager{
		versionStore: newVersionStore(b.snapStore),
		effects:      make(map[string]reconcileEffects),
		snapStore:    b.snapStore,
		effectRKeys:  make(map[string]util.Set[snapshot.ResourceKey]),
		effectIKeys:  make(map[string]util.Set[snapshot.IdentityKey]),
	}

	return NewLensManager(
		rollup,
		mgr,
	), nil
}
