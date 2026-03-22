/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// GarbageCollectorReconciler simulates the Kubernetes GarbageCollectorController.
// When triggered after an object is REMOVED, it lists all objects in the same
// namespace and deletes any whose ownerReferences point to a non-existent owner.
// This models background cascade deletion: parent removed first, then dependents
// are asynchronously marked for deletion.
type GarbageCollectorReconciler struct {
	Client client.Client
	Scheme *runtime.Scheme
}

func (r *GarbageCollectorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.V(1).Info("GarbageCollector reconciling", "trigger", req.NamespacedName)

	// List all objects in the namespace. We use an unstructured list to find
	// objects of any kind that may have ownerReferences pointing to a now-absent parent.
	// Since we don't know what kinds exist, we iterate over common resource types
	// that are typically owned via ownerReferences.
	kindsToCheck := []struct {
		apiVersion string
		kind       string
		listKind   string
	}{
		{"apps/v1", "StatefulSet", "StatefulSetList"},
		{"apps/v1", "Deployment", "DeploymentList"},
		{"apps/v1", "ReplicaSet", "ReplicaSetList"},
		{"v1", "Pod", "PodList"},
		{"v1", "Service", "ServiceList"},
		{"v1", "ConfigMap", "ConfigMapList"},
		{"v1", "PersistentVolumeClaim", "PersistentVolumeClaimList"},
		{"policy/v1", "PodDisruptionBudget", "PodDisruptionBudgetList"},
		{"v1", "ServiceAccount", "ServiceAccountList"},
	}

	for _, kindInfo := range kindsToCheck {
		list := &unstructured.UnstructuredList{}
		list.SetAPIVersion(kindInfo.apiVersion)
		list.SetKind(kindInfo.listKind)

		if err := r.Client.List(ctx, list, client.InNamespace(req.Namespace)); err != nil {
			// Some kinds may not be available; skip them.
			logger.V(2).Info("skipping kind during GC scan", "kind", kindInfo.kind, "error", err)
			continue
		}

		for i := range list.Items {
			obj := &list.Items[i]
			ownerRefs := obj.GetOwnerReferences()
			if len(ownerRefs) == 0 {
				continue
			}
			// Skip objects already marked for deletion.
			if obj.GetDeletionTimestamp() != nil && !obj.GetDeletionTimestamp().IsZero() {
				continue
			}

			for _, ref := range ownerRefs {
				// Check if the owner still exists by trying to Get it.
				owner := &unstructured.Unstructured{}
				owner.SetAPIVersion(ref.APIVersion)
				owner.SetKind(ref.Kind)
				ownerKey := types.NamespacedName{
					Namespace: obj.GetNamespace(),
					Name:      ref.Name,
				}
				err := r.Client.Get(ctx, ownerKey, owner)
				if err == nil {
					// Owner exists. But check if it's being deleted and has a
					// different UID (recreated with same name).
					if string(owner.GetUID()) != string(ref.UID) {
						// Owner was recreated — this dependent is orphaned.
						logger.V(1).Info("GC: owner recreated with different UID, deleting dependent",
							"dependent", fmt.Sprintf("%s/%s", obj.GetKind(), obj.GetName()),
							"owner", fmt.Sprintf("%s/%s", ref.Kind, ref.Name),
							"expectedUID", ref.UID,
							"actualUID", owner.GetUID())
						if err := r.Client.Delete(ctx, obj); err != nil {
							logger.Error(err, "GC: failed to delete orphaned dependent",
								"kind", obj.GetKind(), "name", obj.GetName())
						}
						break
					}
					continue // Owner exists with correct UID, not orphaned.
				}
				if client.IgnoreNotFound(err) != nil {
					logger.Error(err, "GC: error checking owner existence",
						"ownerKind", ref.Kind, "ownerName", ref.Name)
					continue
				}

				// Owner not found — this dependent is orphaned. Delete it.
				logger.V(1).Info("GC: owner not found, deleting dependent",
					"dependent", fmt.Sprintf("%s/%s", obj.GetKind(), obj.GetName()),
					"missingOwner", fmt.Sprintf("%s/%s", ref.Kind, ref.Name))
				if err := r.Client.Delete(ctx, obj); err != nil {
					logger.Error(err, "GC: failed to delete orphaned dependent",
						"kind", obj.GetKind(), "name", obj.GetName())
				}
				break // Only need one missing owner to trigger deletion.
			}
		}
	}

	return ctrl.Result{}, nil
}
