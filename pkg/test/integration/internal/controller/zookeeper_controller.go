package controller

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	schema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// ZookeeperCluster is a simplified version of the real CRD
type ZookeeperCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ZookeeperClusterSpec   `json:"spec,omitempty"`
	Status            ZookeeperClusterStatus `json:"status,omitempty"`
}

type ZookeeperClusterSpec struct {
	Size int32 `json:"size,omitempty"`
}

type ZookeeperClusterStatus struct {
	Nodes []string `json:"nodes,omitempty"`
}

// PVC represents a simplified PersistentVolumeClaim
type PVC struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              PVCSpec   `json:"spec,omitempty"`
	Status            PVCStatus `json:"status,omitempty"`
}

type PVCSpec struct {
	StorageClassName string `json:"storageClassName,omitempty"`
	Size             string `json:"size,omitempty"`
}

type PVCStatus struct {
	Phase string `json:"phase,omitempty"`
}

// ZookeeperReconciler mimics the buggy reconciler
type ZookeeperReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// Reconcile implements the buggy behavior described in the issue
func (r *ZookeeperReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Reconciling ZookeeperCluster", "namespacedName", req.NamespacedName)

	// Get the ZookeeperCluster resource
	zk := &unstructured.Unstructured{}
	zk.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "zookeeper.pravega.io",
		Version: "v1beta1",
		Kind:    "ZookeeperCluster",
	})

	if err := r.Get(ctx, req.NamespacedName, zk); err != nil {
		logger.Error(err, "Failed to get ZookeeperCluster")
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}

	// Check if the ZookeeperCluster is being deleted
	// THIS IS THE CRITICAL CHECK that can be based on stale data!
	if !zk.GetDeletionTimestamp().IsZero() {
		logger.Info("Detected ZookeeperCluster is being deleted", "name", zk.GetName())

		// Get all PVCs linked to this ZookeeperCluster by name only
		// This is the bug - it's only matching by name, not UID
		return r.handleDeletion(ctx, zk)
	}

	// If the ZookeeperCluster is not being deleted, create/update PVCs if needed
	err := r.handleCreation(ctx, zk)
	if err != nil {
		return reconcile.Result{}, err
	}

	return reconcile.Result{}, nil
}

func (r *ZookeeperReconciler) handleDeletion(ctx context.Context, zk *unstructured.Unstructured) (reconcile.Result, error) {
	logger := log.FromContext(ctx)

	// List all PVCs with label app=zk.Name (the bug is here - not checking UID)
	pvcList := &unstructured.UnstructuredList{}
	pvcList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "",
		Version: "v1",
		Kind:    "PersistentVolumeClaimList",
	})

	matchLabels := client.MatchingLabels{"app": zk.GetName()}
	if err := r.List(ctx, pvcList, client.InNamespace(zk.GetNamespace()), matchLabels); err != nil {
		logger.Error(err, "Failed to list PVCs")
		return reconcile.Result{}, err
	}

	// Delete all PVCs found
	for _, pvc := range pvcList.Items {
		fmt.Println("Deleting PVC", pvc.GetName())
		logger.Info("Deleting PVC", "pvc", pvc.GetName())
		if err := r.Delete(ctx, &pvc); err != nil {
			logger.Error(err, "Failed to delete PVC", "pvc", pvc.GetName())
			return reconcile.Result{}, err
		}
	}

	return reconcile.Result{}, nil
}

func (r *ZookeeperReconciler) handleCreation(ctx context.Context, zk *unstructured.Unstructured) error {
	logger := log.FromContext(ctx)

	// Create PVCs for each node in the ZookeeperCluster
	size, _, _ := unstructured.NestedInt64(zk.Object, "spec", "size")

	for i := 0; i < int(size); i++ {
		pvcName := fmt.Sprintf("%s-pvc-%d", zk.GetName(), i)

		// Check if PVC already exists
		pvc := &unstructured.Unstructured{}
		pvc.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "",
			Version: "v1",
			Kind:    "PersistentVolumeClaim",
		})

		err := r.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: zk.GetNamespace()}, pvc)
		if client.IgnoreNotFound(err) != nil {
			logger.Error(err, "Error checking PVC existence")
			return err
		}

		// Create PVC if it doesn't exist
		if err != nil { // Not found, create it
			pvc = &unstructured.Unstructured{}
			pvc.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "PersistentVolumeClaim",
			})
			pvc.SetName(pvcName)
			pvc.SetNamespace(zk.GetNamespace())

			// Set owner reference to the ZookeeperCluster
			pvc.SetLabels(map[string]string{
				"app": zk.GetName(), // This is the critical label that's used to match by name
			})

			// Set PVC spec
			_ = unstructured.SetNestedField(pvc.Object, "10Gi", "spec", "resources", "requests", "storage")
			_ = unstructured.SetNestedField(pvc.Object, "standard", "spec", "storageClassName")

			if err := r.Create(ctx, pvc); err != nil {
				logger.Error(err, "Failed to create PVC", "pvc", pvcName)
				return err
			}

			logger.Info("Created PVC", "pvc", pvcName)
		}
	}

	return nil
}
