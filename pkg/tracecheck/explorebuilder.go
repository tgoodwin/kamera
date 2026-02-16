package tracecheck

import (
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/samber/lo"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/replay"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/tracegen"
	"github.com/tgoodwin/kamera/pkg/util"
	"github.com/tgoodwin/kamera/sleevectrl/pkg/controller"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	cleanupReconcilerID    ReconcilerID = "CleanupReconciler"
	deploymentControllerID ReconcilerID = "DeploymentController"
)

type ExplorerBuilder struct {
	reconcilers                map[ReconcilerID]ReconcilerConstructor
	recorderInjectedStrategies map[ReconcilerID]func(recorder replay.EffectRecorder) Strategy
	resourceDeps               ResourceDeps
	watchers                   WatchRegistrations
	scheme                     *runtime.Scheme
	emitter                    testEmitter
	snapStore                  *snapshot.Store
	reconcilerToKind           map[ReconcilerID]string

	priorityBuilder *PriorityStrategyBuilder

	config *ExploreConfig

	// for replay mode
	builder *replay.Builder

	// podCrashProbabilities configures random crash probabilities for the
	// PodLifecycleController. Maps lifecycle stage to crash probability (0.0 to 1.0).
	podCrashProbabilities map[controller.PodLifecycleStage]float64
}

// ReconcilerBuilder enables chaining reconciler-specific configuration
// (e.g. For, Watches) without repeatedly passing the reconciler ID.
type ReconcilerBuilder struct {
	parent *ExplorerBuilder
	id     ReconcilerID
}

func (rb *ReconcilerBuilder) For(kind string) *ReconcilerBuilder {
	rb.parent.AssignReconcilerToKind(rb.id, kind)
	return rb
}

func (rb *ReconcilerBuilder) ForGK(gk schema.GroupKind) *ReconcilerBuilder {
	rb.parent.AssignReconcilerToKind(rb.id, util.CanonicalGroupKind(gk.Group, gk.Kind))
	return rb
}

func (rb *ReconcilerBuilder) Watches(kind string, mapper WatchMapper) *ReconcilerBuilder {
	rb.parent.WithWatch(kind, mapper, rb.id)
	return rb
}

func (rb *ReconcilerBuilder) WatchesGK(gk schema.GroupKind, mapper WatchMapper) *ReconcilerBuilder {
	rb.parent.WithWatchGK(gk, mapper, rb.id)
	return rb
}

// PermuteOrder marks this reconciler as eligible for order permutation during exploration.
//
// Deprecated: configure permutations via the centralized ExploreConfig:
//
//	cfg := builder.Config()
//	cfg.Perturbations.PermuteOrder[id] = true
//	builder.SetConfig(cfg)
func (rb *ReconcilerBuilder) PermuteOrder() *ReconcilerBuilder {
	cfg := rb.parent.Config()
	cfg.Perturbations.PermuteOrder[rb.id] = true
	rb.parent.SetConfig(cfg)
	return rb
}

// Done returns the parent ExplorerBuilder to continue builder-style chaining.
func (rb *ReconcilerBuilder) Done() *ExplorerBuilder {
	return rb.parent
}

func NewExplorerBuilder(scheme *runtime.Scheme) *ExplorerBuilder {
	utilruntime.Must(appsv1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))

	builder := &ExplorerBuilder{
		reconcilers:                make(map[ReconcilerID]ReconcilerConstructor),
		recorderInjectedStrategies: make(map[ReconcilerID]func(recorder replay.EffectRecorder) Strategy),
		resourceDeps:               make(ResourceDeps),
		watchers:                   make(WatchRegistrations),
		scheme:                     scheme,
		emitter:                    event.NewInMemoryEmitter(),
		snapStore:                  snapshot.NewStore(),
		reconcilerToKind:           make(map[ReconcilerID]string),

		config: &ExploreConfig{
			MaxDepth:        *searchDepth,
			RecordPerfStats: *emitStats,
			Timeout:         *timeout,
			Optimizations: OptimizationConfig{
				OnlyPermuteTriggered: true,
			},
			Perturbations: PerturbationConfig{
				PermuteOrder: make(map[ReconcilerID]bool),
				Staleness:    make(map[ReconcilerID]StalenessConfig),
			},
		},
	}
	if builder.config.MaxDepth == 0 {
		builder.config.MaxDepth = DefaultMaxDepth
	}

	builder.registerCoreControllers()

	return builder
}

// BuildRestartSeed materializes the provided state into a restart seed using the builder's store.
func (b *ExplorerBuilder) BuildRestartSeed(state StateNode) (RestartSeed, error) {
	if b == nil {
		return RestartSeed{}, fmt.Errorf("explorer builder is nil")
	}
	resolver := NewVersionStore(b.snapStore, b.scheme)
	seed, err := BuildRestartSeedFromState(state.Objects(), resolver, state.PendingReconciles)
	if err != nil {
		return RestartSeed{}, err
	}
	seed.Depth = state.depth
	return seed, nil
}

// Fork returns a new builder with shared configuration and fresh mutable state
// (snapshot store, emitter). This is intended for isolated parallel runs.
func (b *ExplorerBuilder) Fork() *ExplorerBuilder {
	if b == nil {
		return nil
	}

	return &ExplorerBuilder{
		reconcilers:                maps.Clone(b.reconcilers),
		recorderInjectedStrategies: maps.Clone(b.recorderInjectedStrategies),
		resourceDeps:               cloneResourceDeps(b.resourceDeps),
		watchers:                   cloneWatchRegistrations(b.watchers),
		scheme:                     b.scheme,
		emitter:                    event.NewInMemoryEmitter(),
		snapStore:                  snapshot.NewStore(),
		reconcilerToKind:           maps.Clone(b.reconcilerToKind),
		priorityBuilder:            b.priorityBuilder,
		config:                     cloneExploreConfig(b.config),
		builder:                    b.builder,
		podCrashProbabilities:      maps.Clone(b.podCrashProbabilities),
	}
}

func cloneExploreConfig(cfg *ExploreConfig) *ExploreConfig {
	if cfg == nil {
		return &ExploreConfig{
			Perturbations: PerturbationConfig{
				PermuteOrder: make(map[ReconcilerID]bool),
				Staleness:    make(map[ReconcilerID]StalenessConfig),
			},
			Optimizations: OptimizationConfig{OnlyPermuteTriggered: true},
		}
	}
	cloned := cfg.Clone()
	return &cloned
}

func cloneResourceDeps(input ResourceDeps) ResourceDeps {
	if input == nil {
		return nil
	}
	out := make(ResourceDeps, len(input))
	for kind, deps := range input {
		if deps == nil {
			out[kind] = nil
			continue
		}
		copied := util.NewSet[ReconcilerID]()
		for id := range deps {
			copied.Add(id)
		}
		out[kind] = copied
	}
	return out
}

func cloneWatchRegistrations(input WatchRegistrations) WatchRegistrations {
	if input == nil {
		return nil
	}
	out := make(WatchRegistrations, len(input))
	for kind, regs := range input {
		out[kind] = slices.Clone(regs)
	}
	return out
}

func (b *ExplorerBuilder) WithReconciler(id ReconcilerID, constructor ReconcilerConstructor) *ReconcilerBuilder {
	b.reconcilers[id] = constructor
	b.ensurePermuteOrderEntry(id)
	return &ReconcilerBuilder{parent: b, id: id}
}

func (b *ExplorerBuilder) WithCustomStrategy(id ReconcilerID, strategyFunc func(recorder replay.EffectRecorder) Strategy) *ReconcilerBuilder {
	{
		b.recorderInjectedStrategies[id] = strategyFunc
		b.ensurePermuteOrderEntry(id)
		return &ReconcilerBuilder{parent: b, id: id}
	}
}

func (b *ExplorerBuilder) WithStrategy(id ReconcilerID, strategyFunc func(recorder replay.EffectRecorder) Strategy) *ReconcilerBuilder {
	return b.WithCustomStrategy(id, strategyFunc)
}

func (b *ExplorerBuilder) WithResourceDep(kind string, reconcilerIDs ...ReconcilerID) *ExplorerBuilder {
	return b.WithResourceDepGK(parseKindString(kind), reconcilerIDs...)
}

func (b *ExplorerBuilder) WithResourceDepGK(gk schema.GroupKind, reconcilerIDs ...ReconcilerID) *ExplorerBuilder {
	key := util.CanonicalGroupKind(gk.Group, gk.Kind)
	if _, ok := b.resourceDeps[key]; !ok {
		b.resourceDeps[key] = util.NewSet[ReconcilerID]()
	}
	for _, id := range reconcilerIDs {
		b.resourceDeps[key].Add(id)
	}
	return b
}

func (b *ExplorerBuilder) WithWatch(kind string, mapper WatchMapper, reconcilerID ReconcilerID) *ExplorerBuilder {
	return b.WithWatchGK(parseKindString(kind), mapper, reconcilerID)
}

func (b *ExplorerBuilder) WithWatchGK(gk schema.GroupKind, mapper WatchMapper, reconcilerID ReconcilerID) *ExplorerBuilder {
	if mapper == nil {
		return b
	}
	if b.watchers == nil {
		b.watchers = make(WatchRegistrations)
	}
	key := util.CanonicalGroupKind(gk.Group, gk.Kind)
	reg := WatchRegistration{
		Mapper:       mapper,
		ReconcilerID: reconcilerID,
	}
	b.watchers[key] = append(b.watchers[key], reg)
	return b
}

func parseKindString(kind string) schema.GroupKind {
	return util.ParseGroupKind(kind)
}

func (b *ExplorerBuilder) ensurePermuteOrderEntry(id ReconcilerID) {
	if b.config == nil {
		b.config = &ExploreConfig{
			Optimizations: OptimizationConfig{
				OnlyPermuteTriggered: true,
			},
			Perturbations: PerturbationConfig{
				PermuteOrder: make(map[ReconcilerID]bool),
				Staleness:    make(map[ReconcilerID]StalenessConfig),
			},
		}
	}
	if b.config.Perturbations.PermuteOrder == nil {
		b.config.Perturbations.PermuteOrder = make(map[ReconcilerID]bool)
	}
	if _, ok := b.config.Perturbations.PermuteOrder[id]; !ok {
		b.config.Perturbations.PermuteOrder[id] = false
	}
}

func (b *ExplorerBuilder) WithPriorityStrategy(p *PriorityStrategyBuilder) *ExplorerBuilder {
	b.priorityBuilder = p
	return b
}

func (b *ExplorerBuilder) WithMaxDepth(depth int) *ExplorerBuilder {
	b.config.MaxDepth = depth
	return b
}

func (b *ExplorerBuilder) WithTimeout(d time.Duration) *ExplorerBuilder {
	b.config.Timeout = d
	return b
}

func (b *ExplorerBuilder) WithPerfStats() *ExplorerBuilder {
	b.config.RecordPerfStats = true
	return b
}

// WithPodCrashProbability sets the probability that a Pod will crash after
// reaching the specified lifecycle stage. This introduces intentional
// nondeterminism for testing multiple converged states.
//
// Available stages:
//   - controller.StageScheduled: After scheduled (Pod is Pending + PodScheduled=True)
//   - controller.StageRunning: After running but not ready (Pod is Running + Ready=False)
//   - controller.StageReady: After fully ready (Pod is Running + Ready=True + IPs assigned)
//
// Example:
//
//	builder.WithPodCrashProbability(controller.StageReady, 0.5) // 50% chance to crash after becoming ready
func (b *ExplorerBuilder) WithPodCrashProbability(stage controller.PodLifecycleStage, probability float64) *ExplorerBuilder {
	if b.podCrashProbabilities == nil {
		b.podCrashProbabilities = make(map[controller.PodLifecycleStage]float64)
	}
	b.podCrashProbabilities[stage] = probability
	return b
}

// WithPerturbations sets stale-read perturbation config for a single reconciler.
//
// Deprecated: configure perturbations via the centralized ExploreConfig:
//
//	cfg := b.Config()
//	cfg.Perturbations.Staleness[reconcilerID] = rc
//	b.SetConfig(cfg)
func (b *ExplorerBuilder) WithPerturbations(reconcilerID ReconcilerID, rc StalenessConfig) *ExplorerBuilder {
	cfg := b.Config()
	if cfg.Perturbations.Staleness == nil {
		cfg.Perturbations.Staleness = make(map[ReconcilerID]StalenessConfig)
	}
	cfg.Perturbations.Staleness[reconcilerID] = rc
	return b.SetConfig(cfg)
}

// WithPermuteOrder sets permutation eligibility for a single reconciler.
//
// Deprecated: configure permutations via the centralized ExploreConfig:
//
//	cfg := b.Config()
//	cfg.Perturbations.PermuteOrder[id] = enabled
//	b.SetConfig(cfg)
func (b *ExplorerBuilder) WithPermuteOrder(id ReconcilerID, enabled bool) *ExplorerBuilder {
	cfg := b.Config()
	if cfg.Perturbations.PermuteOrder == nil {
		cfg.Perturbations.PermuteOrder = make(map[ReconcilerID]bool)
	}
	cfg.Perturbations.PermuteOrder[id] = enabled
	return b.SetConfig(cfg)
}

// WithPermuteOrders sets the per-reconciler permute-order configuration.
// Entries missing for known reconcilers are defaulted to false so the UI can display them.
//
// Deprecated: configure permutations via the centralized ExploreConfig:
//
//	cfg := b.Config()
//	cfg.Perturbations.PermuteOrder = map[ReconcilerID]bool{...}
//	b.SetConfig(cfg)
func (b *ExplorerBuilder) WithPermuteOrders(perms map[ReconcilerID]bool) *ExplorerBuilder {
	if b.config == nil {
		b.config = &ExploreConfig{Optimizations: OptimizationConfig{OnlyPermuteTriggered: true}}
	}
	target := perms
	if target == nil && b.config.Perturbations.PermuteOrder != nil {
		target = b.config.Perturbations.PermuteOrder
	}
	b.config.Perturbations.PermuteOrder = make(map[ReconcilerID]bool, len(target))
	for id, enabled := range target {
		b.config.Perturbations.PermuteOrder[id] = enabled
	}
	// ensure all reconcilers are represented even if not provided in perms
	for id := range b.reconcilers {
		if _, ok := b.config.Perturbations.PermuteOrder[id]; !ok {
			b.config.Perturbations.PermuteOrder[id] = false
		}
	}
	for id := range b.recorderInjectedStrategies {
		if _, ok := b.config.Perturbations.PermuteOrder[id]; !ok {
			b.config.Perturbations.PermuteOrder[id] = false
		}
	}
	return b
}

// SetConfig replaces the builder's config wholesale.
// Maps are cloned and missing permute entries are defaulted for known reconcilers.
func (b *ExplorerBuilder) SetConfig(cfg ExploreConfig) *ExplorerBuilder {
	cloned := cfg.Clone()
	if cloned.Perturbations.PermuteOrder == nil {
		cloned.Perturbations.PermuteOrder = make(map[ReconcilerID]bool)
	}
	if cloned.Perturbations.Staleness == nil {
		cloned.Perturbations.Staleness = make(map[ReconcilerID]StalenessConfig)
	}
	b.config = &cloned
	for id := range b.reconcilers {
		b.ensurePermuteOrderEntry(id)
	}
	for id := range b.recorderInjectedStrategies {
		b.ensurePermuteOrderEntry(id)
	}
	return b
}

// WithDivergenceCircuitBreaker enables the divergence circuit breaker.
// If paths from a divergence point converge to the same state more than `threshold` times,
// further exploration from that subtree is skipped. This is a performance optimization
// to limit combinatorial explosion. Currently, divergence points are created for
// stale-read perturbations.
func (b *ExplorerBuilder) WithDivergenceCircuitBreaker(threshold int) *ExplorerBuilder {
	b.config.divergenceCircuitBreakerThreshold = threshold
	return b
}

// WithOptimizations configures which exploration optimizations to enable.
// By default, all optimizations are disabled; opt in per-heuristic for ablation studies.
func (b *ExplorerBuilder) WithOptimizations(opt OptimizationConfig) *ExplorerBuilder {
	if b.config == nil {
		b.config = &ExploreConfig{Optimizations: OptimizationConfig{OnlyPermuteTriggered: true}}
	}
	b.config.Optimizations = opt
	return b
}

// WithoutOptimizations disables all exploration optimizations.
// Useful for tests that need deterministic, exhaustive exploration.
func (b *ExplorerBuilder) WithoutOptimizations() *ExplorerBuilder {
	if b.config == nil {
		b.config = &ExploreConfig{Optimizations: OptimizationConfig{OnlyPermuteTriggered: true}}
	}
	b.config.Optimizations = OptimizationConfig{}
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

// Config returns a copy of the current builder configuration.
func (b *ExplorerBuilder) Config() ExploreConfig {
	if b.config == nil {
		return ExploreConfig{
			Perturbations: PerturbationConfig{
				PermuteOrder: make(map[ReconcilerID]bool),
				Staleness:    make(map[ReconcilerID]StalenessConfig),
			},
			Optimizations: OptimizationConfig{OnlyPermuteTriggered: true},
		}
	}
	return b.config.Clone()
}

// AssignReconcilerToKind configures which resource a reconciler "owns"
// TODO make how we handle kinds more type safe
func (b *ExplorerBuilder) AssignReconcilerToKind(reconcilerID ReconcilerID, kind string) *ExplorerBuilder {
	gk := parseKindString(kind)
	b.reconcilerToKind[reconcilerID] = util.CanonicalGroupKind(gk.Group, gk.Kind)
	return b
}

func (b *ExplorerBuilder) registerCoreControllers() {
	// Deployment Controller
	b.WithReconciler("DeploymentController", func(c client.Client) Reconciler {
		return &controller.DeploymentReconciler{
			Client: c,
			Scheme: b.scheme,
		}
	}).For("apps/Deployment")

	b.WithResourceDepGK(schema.GroupKind{Group: "apps", Kind: "Deployment"}, deploymentControllerID)
	b.WithResourceDepGK(schema.GroupKind{Group: "apps", Kind: "ReplicaSet"}, deploymentControllerID)

	// ReplicaSet Controller
	b.WithReconciler("ReplicaSetController", func(c client.Client) Reconciler {
		return &controller.ReplicaSetReconciler{
			Client: c,
			Scheme: b.scheme,
		}
	}).For("apps/ReplicaSet")

	b.WithResourceDepGK(schema.GroupKind{Group: "apps", Kind: "ReplicaSet"}, "ReplicaSetController")
	b.WithResourceDepGK(schema.GroupKind{Group: "", Kind: "Pod"}, "ReplicaSetController")
	b.WithResourceDepGK(schema.GroupKind{Group: "apps", Kind: "Deployment"}, "ReplicaSetController")

	// Pod Lifecycle Controller, e.g. "fake kubelet"
	// Note: The factory is retrieved at reconciler instantiation time (during Build()),
	// so crash probabilities configured via WithPodCrashProbability() will be used.
	b.WithReconciler("PodLifecycleController", func(c client.Client) Reconciler {
		return controller.NewPodLifecycleReconciler(
			c,
			b.scheme,
			b.getPodLifecycleFactory(),
			0,
		)
	}).For("Pod")

	b.WithResourceDepGK(schema.GroupKind{Group: "", Kind: "Pod"}, "PodLifecycleController")
	b.WithResourceDepGK(schema.GroupKind{Group: "", Kind: "PodTemplate"}, "PodLifecycleController")
	b.WithResourceDepGK(schema.GroupKind{Group: "apps", Kind: "ReplicaSet"}, "PodLifecycleController")
	b.WithResourceDepGK(schema.GroupKind{Group: "apps", Kind: "Deployment"}, "PodLifecycleController")
	b.WithResourceDepGK(schema.GroupKind{Group: "apps", Kind: "StatefulSet"}, "PodLifecycleController")
	b.WithResourceDepGK(schema.GroupKind{Group: "apps", Kind: "DaemonSet"}, "PodLifecycleController")
	b.WithResourceDepGK(schema.GroupKind{Group: "batch", Kind: "Job"}, "PodLifecycleController")
	b.WithResourceDepGK(schema.GroupKind{Group: "batch", Kind: "CronJob"}, "PodLifecycleController")

	b.WithReconciler("ServiceController", func(c client.Client) Reconciler {
		return &controller.ServiceReconciler{
			Client: c,
			Scheme: b.scheme,
		}
	}).For("Service")

	b.WithResourceDepGK(schema.GroupKind{Group: "", Kind: "Service"}, "ServiceController")
	b.WithResourceDepGK(schema.GroupKind{Group: "", Kind: "Endpoints"}, "ServiceController")

	// endpoints controller
	b.WithReconciler("EndpointsController", func(c client.Client) Reconciler {
		return &controller.EndpointsReconciler{
			Client: c,
			Scheme: b.scheme,
		}
	}).For("Service")

	b.WithResourceDepGK(schema.GroupKind{Group: "", Kind: "Endpoints"}, "EndpointsController")
	b.WithResourceDepGK(schema.GroupKind{Group: "", Kind: "Service"}, "EndpointsController")
	b.WithResourceDepGK(schema.GroupKind{Group: "", Kind: "Pod"}, "EndpointsController")
}

// getPodLifecycleFactory returns the PodStateMachineFactory to use for the
// PodLifecycleController, configured with any crash probabilities.
func (b *ExplorerBuilder) getPodLifecycleFactory() controller.PodStateMachineFactory {
	if len(b.podCrashProbabilities) == 0 {
		return controller.NewDefaultPodLifecycleFactory()
	}
	return &controller.DeterministicPodStateMachineFactory{
		Steps:              controller.DefaultPodLifecycle(),
		CrashProbabilities: b.podCrashProbabilities,
	}
}

func (b *ExplorerBuilder) instantiateReconcilers(mgr *manager) map[ReconcilerID]*ReconcilerContainer {
	containers := make(map[ReconcilerID]*ReconcilerContainer)

	for reconcilerID, constructor := range b.reconcilers {
		var frameManager *replay.FrameManager
		if b.builder != nil {
			// Build harness from the replay builder
			h, err := b.builder.BuildHarness(string(reconcilerID))
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
			string(reconcilerID),
			b.scheme,
			frameManager,
			mgr,
		)

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
		string(cleanupReconcilerID),
		b.scheme,
		fm,
		mgr,
	)
	wrappedClient := tracegen.New(
		replayClient,
		string(cleanupReconcilerID),
		b.emitter,
		tracegen.NewContextTracker(
			string(cleanupReconcilerID),
			b.emitter,
			replay.FrameIDFromContext,
		),
	)
	r := &controller.FinalizerReconciler{
		Client:   wrappedClient,
		Recorder: mgr,
	}
	container := &ReconcilerContainer{
		Name:           cleanupReconcilerID,
		Strategy:       &ControllerRuntimeStrategy{Reconciler: r, frameInserter: fm, name: cleanupReconcilerID, effectReader: mgr},
		effectReader:   mgr,
		versionManager: mgr,
	}
	return container
}

func (b *ExplorerBuilder) NewStateEventBuilder() *StateEventBuilder {
	return NewStateEventBuilder(b.snapStore, b.scheme)
}

func (b *ExplorerBuilder) NewStateClassifier() *StateClassifier {
	return NewStateClassifier(NewVersionStore(b.snapStore, b.scheme))
}

// BuildStartStateFromObjects constructs a starting StateNode from concrete objects and an initial pending list.
func (b *ExplorerBuilder) BuildStartStateFromObjects(objs []client.Object, pending []PendingReconcile) (StateNode, error) {
	return buildStartStateFromObjects(b.snapStore, b.scheme, objs, pending)
}

func (b *ExplorerBuilder) GetStartStateFromObject(obj client.Object, dependentControllers ...ReconcilerID) StateNode {
	dependent := lo.Map(dependentControllers, func(s ReconcilerID, _ int) PendingReconcile {
		return PendingReconcile{
			ReconcilerID: s,
			Request: reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: obj.GetNamespace(),
					Name:      obj.GetName(),
				},
			},
			Source: SourceStateChange,
		}
	})

	state, err := b.BuildStartStateFromObjects([]client.Object{obj}, dependent)
	if err != nil {
		panic("building start state: " + err.Error())
	}
	return state
}

func (b *ExplorerBuilder) Build(modes ...string) (*Explorer, error) {
	// TODO just pull out a dedicated 'BuildFromTraceFile' type of thing
	// to keep that concept separate.
	mode := "standalone"
	if len(modes) > 0 && modes[0] != "" {
		mode = modes[0]
	}
	if b.config == nil {
		b.config = &ExploreConfig{Optimizations: OptimizationConfig{OnlyPermuteTriggered: true}}
	}
	if b.config.MaxDepth == 0 {
		b.config.MaxDepth = DefaultMaxDepth
	}
	// Validate configuration
	if len(b.resourceDeps) == 0 {
		return nil, fmt.Errorf("no resource dependencies defined")
	}

	if b.emitter == nil {
		b.emitter = event.NewInMemoryEmitter()
	}

	// Create version store and knowledge manager
	vStore := NewVersionStore(b.snapStore, b.scheme)

	// Create manager
	mgr := &manager{
		versionStore: vStore,
		effects:      make(map[string]reconcileEffects),

		scheme: b.scheme,

		// effectContext tracks the state of the world at the time of reconcile
		// and this is separate from snapshot store because we want this context
		// to not be shared across branches of the exploration tree.
		effectRKeys: make(map[string]util.Set[string]),

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
	reconcilers[cleanupReconcilerID] = cleanupReconciler

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
		b.watchers,
		mgr.versionStore,
	)

	// Construct the Explorer
	explorer := &Explorer{
		reconcilers:          reconcilers,
		dependencies:         b.resourceDeps,
		triggerManager:       triggerManager,
		knowledgeManager:     knowledgeManager,
		Config:               b.config,
		effectContextManager: mgr,
		versionManager:       vStore,

		// for prioritizing 'interesting' (potentially bug-causing) states to explore
		priorityHandler: b.priorityBuilder.Build(b.snapStore),
	}

	return explorer, nil
}

// BuildLensManager builds a LensManager which can be used to explore and interact with the contents
// of traces produced by the tracing instrumentation portion of this project.
func (b *ExplorerBuilder) BuildLensManager(traceFilePath string) (*LensManager, error) {
	traces, err := b.ParseJSONLTrace(traceFilePath)
	if err != nil {
		return nil, fmt.Errorf("parsing trace file: %w", err)
	}
	rollup := CausalRollup(traces)
	mgr := &manager{
		versionStore: NewVersionStore(b.snapStore, b.scheme),
		effects:      make(map[string]reconcileEffects),
		scheme:       b.scheme,
		effectRKeys:  make(map[string]util.Set[string]),
		effectIKeys:  make(map[string]util.Set[snapshot.IdentityKey]),
	}

	return NewLensManager(
		rollup,
		mgr,
	), nil
}
