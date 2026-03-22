package tracecheck

import (
	"fmt"
	"sort"

	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// ReconcilerID is a type alias for reconciler identifiers, providing type safety
// and clarity when working with reconciler references throughout the codebase.
type ReconcilerID string

// ResourceDeps is a map of canonical resource identifiers (group/kind) to the reconcilers that depend on them.
type ResourceDeps map[string]util.Set[ReconcilerID]

func (rd ResourceDeps) ForReconciler(reconcilerID ReconcilerID) ([]string, error) {
	out := make([]string, 0)
	found := false
	for kind, reconcilers := range rd {
		if reconcilers.Contains(reconcilerID) {
			found = true
			out = append(out, kind)
		}
	}
	if !found {
		return nil, errors.New(fmt.Sprintf("reconciler %s not found in ResourceDeps", reconcilerID))
	}
	return out, nil
}

// WatchMapper mirrors controller-runtime's enqueue mapping behavior for Watches().
// Given a changed object, it returns the reconcile requests that should be enqueued.
type WatchMapper func(obj *unstructured.Unstructured) []reconcile.Request

// StatefulWatchMapper is like WatchMapper but receives a StateReader for cross-referencing
// other objects in the current state. This is needed for mappers like the EndpointsController's
// Pod→Service mapping, which must list Services to find those whose selectors match a pod.
type StatefulWatchMapper func(obj *unstructured.Unstructured, reader StateReader) []reconcile.Request

// StateReader provides read-only access to the current exploration state for stateful watch mappers.
type StateReader interface {
	// ListByKind returns all objects of the given canonical group/kind in the current state.
	ListByKind(canonicalGroupKind string) []*unstructured.Unstructured
}

func EnqueueRequestForObject() WatchMapper {
	return func(obj *unstructured.Unstructured) []reconcile.Request {
		if obj == nil {
			return nil
		}
		nsName := types.NamespacedName{
			Namespace: obj.GetNamespace(),
			Name:      obj.GetName(),
		}
		if nsName.Namespace == "" || nsName.Name == "" {
			return nil
		}
		return []reconcile.Request{
			{NamespacedName: nsName},
		}
	}
}

type WatchRegistration struct {
	Mapper         WatchMapper
	StatefulMapper StatefulWatchMapper
	ReconcilerID   ReconcilerID
}

// WatchRegistrations maps canonical group/kind strings to watch registrations.
type WatchRegistrations map[string][]WatchRegistration

// PrimariesByKind tracks the owner of a resource by kind, where owner is the reconciler that
// has ControllerManagedBy.For called with the kind of the resource
type PrimariesByKind map[string]util.Set[ReconcilerID]

// PendingReconcileSource indicates how a reconcile request was triggered
type PendingReconcileSource string

const (
	// SourceStateChange indicates the reconcile was triggered by a resource state change
	SourceStateChange PendingReconcileSource = "State Change"
	// SourceAsyncEnqueue indicates the reconcile was triggered by an async enqueue (e.g., ticker)
	SourceAsyncEnqueue PendingReconcileSource = "Async Enqueue"
	// SourceRequeueAfter indicates the reconcile was triggered by reconcile.Result.RequeueAfter.
	// This is actionable delayed work and should block convergence.
	SourceRequeueAfter PendingReconcileSource = "Requeue After"
	// SourceStableRequeueAfter indicates a RequeueAfter that has repeatedly
	// re-fired on the same object state without producing writes. It is treated as
	// ignorable polling for convergence purposes.
	SourceStableRequeueAfter PendingReconcileSource = "Stable Requeue After"

	SourceRequeue PendingReconcileSource = "Requeue"
)

type PendingReconcile struct {
	ReconcilerID ReconcilerID
	Request      reconcile.Request
	Source       PendingReconcileSource
	// ReadyAtDepth is an exploration-only scheduling escape hatch layered on top of
	// the otherwise reactive PendingReconcile model.
	//
	// PendingReconcile normally represents work that became runnable because some
	// prior event enqueued it. ReadyAtDepth exists primarily to support user action
	// interleaving, where the explorer needs to inject non-reactive work at an
	// arbitrary point in a branch. RequeueAfter reuses the same mechanism so the
	// explorer has a single depth-based readiness gate for all deferred work.
	//
	// Zero means "ready immediately". Positive values mean "deferred until this
	// exploration depth".
	ReadyAtDepth int
}

func (pr PendingReconcile) String() string {
	return fmt.Sprintf("%s:%s/%s", pr.ReconcilerID, pr.Request.Namespace, pr.Request.Name)
}

func (pr PendingReconcile) IsDeferredAtDepth(depth int) bool {
	return pr.ReadyAtDepth > depth
}

func (pr PendingReconcile) IsReadyAtDepth(depth int) bool {
	return !pr.IsDeferredAtDepth(depth)
}

func isIgnorableForConvergence(pr PendingReconcile) bool {
	return pr.Source == SourceAsyncEnqueue ||
		pr.Source == SourceRequeue ||
		pr.Source == SourceStableRequeueAfter
}

// allPendingIgnorableForConvergence returns true if all pending reconciles are from
// sources that don't indicate actionable progress (async enqueues from tickers, or requeues
// from controllers that always re-enqueue). This is used to determine convergence:
// if the only remaining work is time-based re-enqueues or poll-based requeues,
// the controller logic has effectively converged since no state is changing.
//
// IMPORTANT: Returns false if ANY pending has SourceStateChange, which means
// the state should NOT be considered converged.
func allPendingIgnorableForConvergence(pending []PendingReconcile) bool {
	if len(pending) == 0 {
		return false // empty list should not be considered "all ignorable"
	}
	for _, pr := range pending {
		if !isIgnorableForConvergence(pr) {
			return false
		}
	}
	return true
}

// countIgnorableForConvergence counts pending reconciles that are ignorable for convergence.
// RequeueAfter is intentionally excluded since it represents actionable follow-up work.
func countIgnorableForConvergence(pending []PendingReconcile) int {
	count := 0
	for _, pr := range pending {
		if isIgnorableForConvergence(pr) {
			count++
		}
	}
	return count
}

// pendingSourcesSummary returns a summary of pending reconcile sources for debugging
func pendingSourcesSummary(pending []PendingReconcile) map[PendingReconcileSource]int {
	counts := make(map[PendingReconcileSource]int)
	for _, pr := range pending {
		counts[pr.Source]++
	}
	return counts
}

type Resolver interface {
	Resolve(hash snapshot.VersionHash) *unstructured.Unstructured
}

// TriggerManager handles the dependency graph between resources and reconcilers
// and models the event-driven mechanism that triggers reconcilers upon changes to resources
type TriggerManager struct {
	deps        ResourceDeps
	owners      PrimariesByKind
	watchers    WatchRegistrations
	resolver    Resolver
	stateReader StateReader
}

// if an object has already been marked for deletion and undergoes a further update,
// the cleanupReconciler needs to hear about that update.
func shouldQueueCleanupReconcile(op event.OperationType, obj *unstructured.Unstructured) bool {
	deletionTS := obj.GetDeletionTimestamp()
	if deletionTS == nil || deletionTS.IsZero() {
		return false
	}
	switch op {
	case event.MARK_FOR_DELETION, event.UPDATE, event.PATCH, event.APPLY:
		return true
	default:
		return false
	}
}

func canonicalKindKeyFromGroupKind(gk schema.GroupKind) string {
	return util.CanonicalGroupKind(gk.Group, gk.Kind)
}

func canonicalKindKey(group, kind string) string {
	return util.CanonicalGroupKind(group, kind)
}

func identityWatchMapper(obj *unstructured.Unstructured) []reconcile.Request {
	if obj == nil {
		return nil
	}
	nsName := types.NamespacedName{
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}
	if nsName.Namespace == "" || nsName.Name == "" {
		return nil
	}
	return []reconcile.Request{
		{NamespacedName: nsName},
	}
}

// NewTriggerManager creates a new instance of TriggerManager
func NewTriggerManager(
	subscribingReconcilersByKind ResourceDeps,
	reconcilerToPrimaryKind map[ReconcilerID]string,
	watchers WatchRegistrations,
	resolver Resolver,
) *TriggerManager {

	primariesByKind := make(PrimariesByKind)
	for reconcilerID, kindSpec := range reconcilerToPrimaryKind {
		gk := util.ParseGroupKind(kindSpec)
		canonical := canonicalKindKeyFromGroupKind(gk)
		if _, exists := primariesByKind[canonical]; !exists {
			primariesByKind[canonical] = make(util.Set[ReconcilerID])
		}
		primariesByKind[canonical].Add(reconcilerID)
	}
	if watchers == nil {
		watchers = make(WatchRegistrations)
	}

	return &TriggerManager{
		deps:     subscribingReconcilersByKind,
		owners:   primariesByKind,
		watchers: watchers,
		resolver: resolver,
	}
}

func (tm *TriggerManager) KindDepsForReconciler(reconcilerID ReconcilerID) ([]string, error) {
	return tm.deps.ForReconciler(reconcilerID)
}

// SetStateReader updates the state reader used by stateful watch mappers.
// Called before each getTriggered invocation with the current state.
func (tm *TriggerManager) SetStateReader(reader StateReader) {
	tm.stateReader = reader
}

// getTriggered returns a list of PendingReconcile items based on the provided changes
// Returns an error if any object hash cannot be resolved
func (tm *TriggerManager) getTriggered(changes Changes) ([]PendingReconcile, error) {
	// Use a map for deduplication
	uniqueReconciles := make(map[string]PendingReconcile)

	for _, effect := range changes.Effects {
		objKey, vHash := effect.Key.IdentityKey, effect.Version
		objectVal := tm.resolver.Resolve(vHash)
		if objectVal == nil {
			return nil, fmt.Errorf("unable to resolve object for key %s with hash %s", objKey, vHash.Value)
		}

		nsName := types.NamespacedName{
			Namespace: objectVal.GetNamespace(),
			Name:      objectVal.GetName(),
		}
		// ensure the object has a name (namespace may be empty for cluster-scoped objects)
		if nsName.Name == "" {
			return nil, fmt.Errorf("resolved object %s has no namespaced name", objKey)
		}

		if effect.OpType == event.MARK_FOR_DELETION {
			deletionTS := objectVal.GetDeletionTimestamp()
			if deletionTS == nil || deletionTS.IsZero() {
				panic("found object marked for deletion but with no deletion timestamp")
			}
		}

		// Deletion-scope mutations need to wake the CleanupReconciler so it can
		// react when finalizer state changes allow actual removal.
		if shouldQueueCleanupReconcile(effect.OpType, objectVal) {
			// queue up the CleanupReconciler to handle the actual removal
			reconcileKey := fmt.Sprintf("%s:%s:%s", cleanupReconcilerID, nsName.Namespace, nsName.Name)
			uniqueReconciles[reconcileKey] = PendingReconcile{
				ReconcilerID: cleanupReconcilerID,
				Request: reconcile.Request{
					NamespacedName: nsName,
				},
				Source: SourceStateChange,
			}
		}

		// When an object is REMOVED, queue the GarbageCollectorController to
		// find and delete any dependents whose ownerReferences point to the
		// now-absent parent. This models K8s background cascade deletion.
		if effect.OpType == event.REMOVE {
			reconcileKey := fmt.Sprintf("%s:%s:%s", garbageCollectorReconcilerID, nsName.Namespace, nsName.Name)
			uniqueReconciles[reconcileKey] = PendingReconcile{
				ReconcilerID: garbageCollectorReconcilerID,
				Request: reconcile.Request{
					NamespacedName: nsName,
				},
				Source: SourceStateChange,
			}
		}

		// Add primary reconcilers if available
		ownerKey := canonicalKindKey(objKey.Group, objKey.Kind)
		if primaries, exists := tm.owners[ownerKey]; exists {
			for primaryReconcilerID := range primaries {
				reconcileKey := fmt.Sprintf("%s:%s:%s", primaryReconcilerID, nsName.Namespace, nsName.Name)
				uniqueReconciles[reconcileKey] = PendingReconcile{
					ReconcilerID: primaryReconcilerID,
					Request: reconcile.Request{
						NamespacedName: nsName,
					},
					Source: SourceStateChange,
				}
			}
		}

		// Add reconcilers registered via watches (controller-runtime Watches semantics).
		// Mapping from changed object -> reconcile requests is delegated to the mapper.
		if watchRegs, exists := tm.watchers[ownerKey]; exists {
			for _, reg := range watchRegs {
				var requests []reconcile.Request
				if reg.StatefulMapper != nil && tm.stateReader != nil {
					requests = reg.StatefulMapper(objectVal, tm.stateReader)
				} else if reg.Mapper != nil {
					requests = reg.Mapper(objectVal)
				} else {
					continue
				}
				for _, req := range requests {
					nsName := req.NamespacedName
					if nsName.Name == "" {
						continue
					}
					reconcileKey := fmt.Sprintf("%s:%s:%s", reg.ReconcilerID, nsName.Namespace, nsName.Name)
					if _, alreadyAdded := uniqueReconciles[reconcileKey]; alreadyAdded {
						continue
					}
					uniqueReconciles[reconcileKey] = PendingReconcile{
						ReconcilerID: reg.ReconcilerID,
						Request: reconcile.Request{
							NamespacedName: nsName,
						},
						Source: SourceStateChange,
					}
				}
			}
		}

		// Process owner references if available
		if objectVal.GetOwnerReferences() != nil {
			for _, ownerRef := range objectVal.GetOwnerReferences() {
				// Only process if we have a registered reconciler for this owner kind
				ownerGV := schema.FromAPIVersionAndKind(ownerRef.APIVersion, ownerRef.Kind)
				ownerKey := canonicalKindKey(ownerGV.Group, ownerGV.Kind)
				if primaries, exists := tm.owners[ownerKey]; exists {
					for ownerReconcilerID := range primaries {
						ownerNSName := types.NamespacedName{
							// Making a reasonable assumption here that the owner
							// is in the same namespace as the owned object
							Namespace: objectVal.GetNamespace(),
							Name:      ownerRef.Name,
						}

						reconcileKey := fmt.Sprintf("%s:%s:%s", ownerReconcilerID, ownerNSName.Namespace, ownerNSName.Name)
						uniqueReconciles[reconcileKey] = PendingReconcile{
							ReconcilerID: ownerReconcilerID,
							Request: reconcile.Request{
								NamespacedName: ownerNSName,
							},
							Source: SourceStateChange,
						}
					}
				}
			}
		}
	}

	// Convert map to slice
	result := make([]PendingReconcile, 0, len(uniqueReconciles))
	for _, reconcile := range uniqueReconciles {
		result = append(result, reconcile)
	}

	// Sort for deterministic output
	sort.Slice(result, func(i, j int) bool {
		if result[i].ReconcilerID != result[j].ReconcilerID {
			return result[i].ReconcilerID < result[j].ReconcilerID
		}
		if result[i].Request.Namespace != result[j].Request.Namespace {
			return result[i].Request.Namespace < result[j].Request.Namespace
		}
		return result[i].Request.Name < result[j].Request.Name
	})

	return result, nil
}

// Convenience method that delegates to getTriggered but panics on errors
// This maintains backwards compatibility with existing code
func (tm *TriggerManager) GetTriggered(changes Changes) ([]PendingReconcile, error) {
	result, err := tm.getTriggered(changes)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// objectVersionsStateReader implements StateReader by resolving objects from an
// ObjectVersions map using a Resolver.
type objectVersionsStateReader struct {
	objects  ObjectVersions
	resolver Resolver
}

func NewStateReaderFromObjectVersions(objects ObjectVersions, resolver Resolver) StateReader {
	return &objectVersionsStateReader{objects: objects, resolver: resolver}
}

func (r *objectVersionsStateReader) ListByKind(canonicalGroupKind string) []*unstructured.Unstructured {
	var result []*unstructured.Unstructured
	for key, hash := range r.objects {
		objCanonical := util.CanonicalGroupKind(key.ResourceKey.Group, key.ResourceKey.Kind)
		if objCanonical == canonicalGroupKind {
			obj := r.resolver.Resolve(hash)
			if obj != nil {
				result = append(result, obj)
			}
		}
	}
	return result
}

func NewPendingReconciles(nsName types.NamespacedName, dependentControllers ...ReconcilerID) []PendingReconcile {
	return lo.Map(dependentControllers, func(controllerID ReconcilerID, _ int) PendingReconcile {
		return PendingReconcile{
			ReconcilerID: controllerID,
			Request: reconcile.Request{
				NamespacedName: nsName,
			},
			Source: SourceStateChange,
		}
	})
}
