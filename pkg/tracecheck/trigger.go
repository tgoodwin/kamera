package tracecheck

import (
	"fmt"
	"sort"

	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"github.com/tgoodwin/sleeve/pkg/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// ResourceDeps is a map of resource kinds to the reconcilers that depend on them
type ResourceDeps map[string]util.Set[string]

func (rd ResourceDeps) ForReconciler(reconcilerID string) ([]string, error) {
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

// PrimariesByKind tracks the owner of a resource by kind, where owner is the reconciler that
// has ControllerManagedBy.For called with the kind of the resource
type PrimariesByKind map[string]util.Set[string]

type PendingReconcile struct {
	ReconcilerID string
	Request      reconcile.Request
}

type hashResolver interface {
	GetByHash(hash snapshot.VersionHash, strategy snapshot.HashStrategy) (*unstructured.Unstructured, bool)
}

// TriggerManager handles the dependency graph between resources and reconcilers
// and models the event-driven mechanism that triggers reconcilers upon changes to resources
type TriggerManager struct {
	deps     ResourceDeps
	owners   PrimariesByKind
	resolver hashResolver
}

// NewTriggerManager creates a new instance of TriggerManager
func NewTriggerManager(deps ResourceDeps, reconcilerToPrimaryKind map[string]string, resolver hashResolver) *TriggerManager {

	primariesByKind := make(PrimariesByKind)
	for reconcilerID, kind := range reconcilerToPrimaryKind {
		if _, exists := primariesByKind[kind]; !exists {
			primariesByKind[kind] = make(util.Set[string])
		}
		primariesByKind[kind].Add(reconcilerID)
	}
	return &TriggerManager{
		deps:     deps,
		owners:   primariesByKind,
		resolver: resolver,
	}
}

// getTriggered returns a list of PendingReconcile items based on the provided changes
// Returns an error if any object hash cannot be resolved
func (tm *TriggerManager) getTriggered(changes ObjectVersions) ([]PendingReconcile, error) {
	// Use a map for deduplication
	uniqueReconciles := make(map[string]PendingReconcile)

	for objKey, vHash := range changes {
		// Get the full object from the hash
		objectVal, ok := tm.resolver.GetByHash(vHash, vHash.Strategy)
		if !ok {
			return nil, fmt.Errorf("object not found for hash %s", vHash)
		}

		nsName := types.NamespacedName{
			Namespace: objectVal.GetNamespace(),
			Name:      objectVal.GetName(),
		}
		// check to ensure the object has a namespaced name
		if nsName.Name == "" || nsName.Namespace == "" {
			return nil, fmt.Errorf("resolved object %s has no namespaced name", objKey)
		}

		// Add primary reconcilers if available
		if primaries, exists := tm.owners[objKey.Kind]; exists {
			for primaryReconcilerID := range primaries {
				reconcileKey := fmt.Sprintf("%s:%s:%s", primaryReconcilerID, nsName.Namespace, nsName.Name)
				uniqueReconciles[reconcileKey] = PendingReconcile{
					ReconcilerID: primaryReconcilerID,
					Request: reconcile.Request{
						NamespacedName: nsName,
					},
				}
			}
		}

		// Process owner references if available
		if objectVal.GetOwnerReferences() != nil {
			for _, ownerRef := range objectVal.GetOwnerReferences() {
				// Only process if we have a registered reconciler for this owner kind
				if primaries, exists := tm.owners[ownerRef.Kind]; exists {
					for ownerReconcilerID := range primaries {
						ownerNSName := types.NamespacedName{
							// TODO this is an assumption that the owner is in the same namespace
							// as the owned object
							Namespace: objectVal.GetNamespace(),
							Name:      ownerRef.Name,
						}

						reconcileKey := fmt.Sprintf("%s:%s:%s", ownerReconcilerID, ownerNSName.Namespace, ownerNSName.Name)
						uniqueReconciles[reconcileKey] = PendingReconcile{
							ReconcilerID: ownerReconcilerID,
							Request: reconcile.Request{
								NamespacedName: ownerNSName,
							},
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

	// Debug print the pending reconciles
	// for _, reconcile := range result {
	// 	fmt.Printf("PendingReconcile: ReconcilerID=%s, nsName=%s/%s\n",
	// 		reconcile.ReconcilerID, reconcile.Request.Namespace, reconcile.Request.Name)
	// }

	return result, nil
}

// Convenience method that delegates to getTriggered but panics on errors
// This maintains backwards compatibility with existing code
func (tm *TriggerManager) MustGetTriggered(changes ObjectVersions) []PendingReconcile {
	result, err := tm.getTriggered(changes)
	if err != nil {
		panic(err.Error())
	}
	return result
}

func NewPendingReconciles(nsName types.NamespacedName, dependentControllers ...string) []PendingReconcile {
	return lo.Map(dependentControllers, func(controllerID string, _ int) PendingReconcile {
		return PendingReconcile{
			ReconcilerID: controllerID,
			Request: reconcile.Request{
				NamespacedName: nsName,
			},
		}
	})
}
