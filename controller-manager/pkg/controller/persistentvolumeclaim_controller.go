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
	"sort"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get;list;watch;create;update;patch;delete

// PersistentVolumeClaimReconciler reconciles a PersistentVolumeClaim object
type PersistentVolumeClaimReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *PersistentVolumeClaimReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// Fetch the PVC instance
	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, req.NamespacedName, pvc); err != nil {
		if client.IgnoreNotFound(err) != nil {
			log.Error(err, "unable to fetch PVC")
			return ctrl.Result{}, err
		}
		// PVC not found, likely deleted, return
		return ctrl.Result{}, nil
	}

	// Check if the PVC is being deleted
	if !pvc.DeletionTimestamp.IsZero() {
		log.Info("PVC is being deleted", "name", pvc.Name)
		return ctrl.Result{}, nil
	}

	// If the PVC is already bound, nothing to do
	if pvc.Status.Phase == corev1.ClaimBound {
		log.Info("PVC already bound", "name", pvc.Name, "volume", pvc.Spec.VolumeName)
		return ctrl.Result{}, nil
	}

	// Find a matching PV if this is a static binding
	if pvc.Spec.VolumeName != "" {
		pv := &corev1.PersistentVolume{}
		if err := r.Get(ctx, types.NamespacedName{Name: pvc.Spec.VolumeName}, pv); err != nil {
			if errors.IsNotFound(err) {
				log.Info("PV specified in PVC does not exist", "pv", pvc.Spec.VolumeName)
				// Update the PVC phase to Pending
				pvc.Status.Phase = corev1.ClaimPending
				if err := r.Status().Update(ctx, pvc); err != nil {
					log.Error(err, "failed to update PVC status to Pending")
					return ctrl.Result{}, err
				}
				return ctrl.Result{Requeue: true}, nil
			}
			log.Error(err, "failed to get PV", "pv", pvc.Spec.VolumeName)
			return ctrl.Result{}, err
		}

		// Bind PVC to PV
		return r.bindPVCToPV(ctx, pvc, pv)
	}

	// Dynamic provisioning or waiting for a suitable PV
	// List available PVs that match the PVC selector
	pvList := &corev1.PersistentVolumeList{}
	if err := r.List(ctx, pvList); err != nil {
		log.Error(err, "failed to list PVs")
		return ctrl.Result{}, err
	}

	// Filter PVs that match the PVC
	var matchingPVs []*corev1.PersistentVolume
	for i := range pvList.Items {
		pv := &pvList.Items[i]
		if r.pvMatchesPVC(pv, pvc) {
			matchingPVs = append(matchingPVs, pv)
		}
	}

	// Sort matching PVs by creation timestamp (oldest first)
	sort.Slice(matchingPVs, func(i, j int) bool {
		return matchingPVs[i].CreationTimestamp.Before(&matchingPVs[j].CreationTimestamp)
	})

	// Bind to the first matching PV if any
	if len(matchingPVs) > 0 {
		pv := matchingPVs[0]
		return r.bindPVCToPV(ctx, pvc, pv)
	}

	// No matching PV found, simulate dynamic provisioning for demo purposes
	if pvc.Status.Phase != corev1.ClaimPending {
		pvc.Status.Phase = corev1.ClaimPending
		if err := r.Status().Update(ctx, pvc); err != nil {
			log.Error(err, "failed to update PVC status to Pending")
			return ctrl.Result{}, err
		}
	}

	// Create a PV for this PVC
	pv := r.createPVForPVC(pvc)
	if err := r.Create(ctx, pv); err != nil {
		log.Error(err, "failed to create PV for PVC")
		return ctrl.Result{}, err
	}
	log.Info("Created PV for PVC", "pvc", pvc.Name, "pv", pv.Name)

	// Requeue to bind the PVC to the newly created PV
	return ctrl.Result{Requeue: true}, nil
}

// pvMatchesPVC checks if a PV matches the requirements of a PVC
func (r *PersistentVolumeClaimReconciler) pvMatchesPVC(pv *corev1.PersistentVolume, pvc *corev1.PersistentVolumeClaim) bool {
	// Check if PV is available
	if pv.Status.Phase != corev1.VolumeAvailable {
		return false
	}

	// Check storage class
	if pvc.Spec.StorageClassName != nil && pv.Spec.StorageClassName != "" &&
		*pvc.Spec.StorageClassName != pv.Spec.StorageClassName {
		return false
	}

	// Check access modes
	if !containsAllAccessModes(pvc.Spec.AccessModes, pv.Spec.AccessModes) {
		return false
	}

	// Check storage capacity
	pvcSize := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	pvSize := pv.Spec.Capacity[corev1.ResourceStorage]
	return pvSize.Cmp(pvcSize) >= 0
}

// containsAllAccessModes checks if all PVC access modes are supported by the PV
func containsAllAccessModes(pvcModes, pvModes []corev1.PersistentVolumeAccessMode) bool {
	for _, pvcMode := range pvcModes {
		found := false
		for _, pvMode := range pvModes {
			if pvMode == pvcMode {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// bindPVCToPV binds a PVC to a PV
func (r *PersistentVolumeClaimReconciler) bindPVCToPV(
	ctx context.Context,
	pvc *corev1.PersistentVolumeClaim,
	pv *corev1.PersistentVolume) (ctrl.Result, error) {

	log := log.FromContext(ctx)

	// Update PV
	pv.Spec.ClaimRef = &corev1.ObjectReference{
		Kind:       "PersistentVolumeClaim",
		Namespace:  pvc.Namespace,
		Name:       pvc.Name,
		UID:        pvc.UID,
		APIVersion: "v1",
	}
	pv.Status.Phase = corev1.VolumeBound

	if err := r.Update(ctx, pv); err != nil {
		log.Error(err, "failed to update PV", "pv", pv.Name)
		return ctrl.Result{}, err
	}

	// Update PVC
	pvc.Spec.VolumeName = pv.Name
	if err := r.Update(ctx, pvc); err != nil {
		log.Error(err, "failed to update PVC with volume name", "pvc", pvc.Name)
		return ctrl.Result{}, err
	}

	// Update PVC status
	pvc.Status.Phase = corev1.ClaimBound
	pvc.Status.AccessModes = pv.Spec.AccessModes
	pvc.Status.Capacity = pv.Spec.Capacity

	if err := r.Status().Update(ctx, pvc); err != nil {
		log.Error(err, "failed to update PVC status", "pvc", pvc.Name)
		return ctrl.Result{}, err
	}

	log.Info("Successfully bound PVC to PV", "pvc", pvc.Name, "pv", pv.Name)
	return ctrl.Result{}, nil
}

// createPVForPVC creates a new PV matching the PVC requirements
func (r *PersistentVolumeClaimReconciler) createPVForPVC(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolume {
	pvName := fmt.Sprintf("pv-%s", uuid.New().String()[:8])

	storageClass := ""
	if pvc.Spec.StorageClassName != nil {
		storageClass = *pvc.Spec.StorageClassName
	}

	// Create a new PV
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: pvName,
			// sleeve code assumes every object has a namespace/name. This is not true for PVs
			// in reality, but we need to set it here to avoid panics in the tracecheck code.
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeSpec{
			StorageClassName: storageClass,
			AccessModes:      pvc.Spec.AccessModes,
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: pvc.Spec.Resources.Requests[corev1.ResourceStorage],
			},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: fmt.Sprintf("/tmp/sleeve-pv/%s", pvName),
				},
			},
		},
		Status: corev1.PersistentVolumeStatus{
			Phase: corev1.VolumeAvailable,
		},
	}

	return pv
}

// SetupWithManager sets up the controller with the Manager.
func (r *PersistentVolumeClaimReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.PersistentVolumeClaim{}).
		Complete(r)
}
