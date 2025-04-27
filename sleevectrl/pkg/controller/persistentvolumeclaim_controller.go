package controller

import (
	"context"
	"fmt"

	// Removed math/rand, os, path/filepath, strconv, time
	"reflect"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1" // Import storagev1
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	// Removed: "github.com/discreteevents/sleeve/pkg/event"
)

const (
	pvFinalizer = "kubernetes.io/pv-protection"
	// Annotation for the selected node used with WaitForFirstConsumer binding mode
	selectedNodeAnnotation = "volume.kubernetes.io/selected-node"
)

// PersistentVolumeClaimReconciler reconciles a PersistentVolumeClaim object
type PersistentVolumeClaimReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

//+kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=persistentvolumeclaims/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=core,resources=persistentvolumeclaims/finalizers,verbs=update
//+kubebuilder:rbac:groups=core,resources=persistentvolumes,verbs=get;list;watch;update;patch;delete // Removed 'create' as we rely on external provisioner
//+kubebuilder:rbac:groups=core,resources=persistentvolumes/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=core,resources=persistentvolumes/finalizers,verbs=update
//+kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get;list;watch // Added storageclasses RBAC

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *PersistentVolumeClaimReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the PersistentVolumeClaim instance
	pvc := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, req.NamespacedName, pvc)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("PersistentVolumeClaim resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get PersistentVolumeClaim")
		return ctrl.Result{}, err
	}

	// Handle deletion: Check if the PVC is marked for deletion
	if !pvc.ObjectMeta.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, pvc)
	}

	// Handle finalizer addition if necessary
	if !controllerutil.ContainsFinalizer(pvc, pvFinalizer) {
		logger.Info("Adding finalizer to PVC", "pvc", pvc.Name)
		controllerutil.AddFinalizer(pvc, pvFinalizer)
		if err := r.Update(ctx, pvc); err != nil {
			logger.Error(err, "Failed to add finalizer to PVC", "pvc", pvc.Name)
			return ctrl.Result{}, err
		}
		// Requeue after adding finalizer to process the object again
		return ctrl.Result{Requeue: true}, nil
	}

	// --- Main Reconciliation Logic ---

	// If PVC is already Bound or Lost, nothing specific to do for binding
	if pvc.Status.Phase == corev1.ClaimBound || pvc.Status.Phase == corev1.ClaimLost {
		logger.Info("PVC is already bound or lost, skipping binding logic", "pvc", pvc.Name, "phase", pvc.Status.Phase)
		return ctrl.Result{}, nil
	}

	// If PVC is Pending, try to find or provision a PV
	if pvc.Status.Phase == corev1.ClaimPending {
		logger.Info("PVC is Pending, attempting to find/bind PV", "pvc", pvc.Name)

		// --- START: WaitForFirstConsumer Check ---
		var storageClass *storagev1.StorageClass
		if pvc.Spec.StorageClassName != nil && *pvc.Spec.StorageClassName != "" {
			scName := *pvc.Spec.StorageClassName
			storageClass = &storagev1.StorageClass{}
			err := r.Get(ctx, types.NamespacedName{Name: scName}, storageClass)
			if err != nil {
				if apierrors.IsNotFound(err) {
					logger.Error(err, "StorageClass specified in PVC not found", "storageClass", scName)
					r.Recorder.Eventf(pvc, corev1.EventTypeWarning, "ProvisioningFailed", "StorageClass %q not found", scName)
					return ctrl.Result{}, nil // Don't requeue immediately if SC is missing
				}
				logger.Error(err, "Failed to get StorageClass", "storageClass", scName)
				return ctrl.Result{}, err // Requeue on other errors
			}

			// Check VolumeBindingMode
			if storageClass.VolumeBindingMode != nil && *storageClass.VolumeBindingMode == storagev1.VolumeBindingWaitForFirstConsumer {
				// Check if the scheduler has selected a node
				if _, ok := pvc.Annotations[selectedNodeAnnotation]; !ok {
					logger.Info("PVC is waiting for first consumer and no node selected yet", "pvc", pvc.Name)
					// Do not attempt binding yet, wait for the scheduler
					return ctrl.Result{}, nil
				}
				logger.Info("PVC is waiting for first consumer and node is selected, proceeding with binding", "pvc", pvc.Name, "node", pvc.Annotations[selectedNodeAnnotation])
				// Node selected, proceed with binding logic below
			}
		}
		// --- END: WaitForFirstConsumer Check ---

		// 1. Check if a specific PV is requested
		if pvc.Spec.VolumeName != "" {
			return r.bindToSpecifiedPV(ctx, pvc)
		}

		// 2. Try to find a suitable existing PV (Static Provisioning / Already Provisioned Dynamically)
		// This now covers both statically created PVs and PVs dynamically created by the external provisioner.
		bound, pv, err := r.findAndBindSuitablePV(ctx, pvc)
		if err != nil {
			logger.Error(err, "Error trying to find and bind a suitable PV", "pvc", pvc.Name)
			return ctrl.Result{}, err // Requeue on error
		}
		if bound {
			logger.Info("Successfully bound PVC to existing/provisioned PV", "pvc", pvc.Name, "pv", pv.Name)
			r.Recorder.Eventf(pvc, corev1.EventTypeNormal, "ProvisioningSucceeded", "Successfully bound to PV %q", pv.Name)
			return ctrl.Result{}, nil // Done
		}

		// 3. If no suitable PV found AND a StorageClass exists, wait for external provisioner.
		if storageClass != nil {
			// We didn't find a suitable PV yet, and dynamic provisioning is expected via the SC.
			// The external provisioner (e.g., rancher.io/local-path) should handle PV creation.
			// We just need to wait for a suitable PV to appear.
			logger.Info("No suitable PV found. Waiting for external provisioner associated with StorageClass.", "pvc", pvc.Name, "storageClass", storageClass.Name, "provisioner", storageClass.Provisioner)
			// No error, just requeue after a delay or rely on watch events for new/updated PVs.
			// Returning empty result relies on watches. Add RequeueAfter if needed.
			// r.Recorder.Eventf(pvc, corev1.EventTypeNormal, "ExternalProvisioning", "Waiting for external provisioner %q", storageClass.Provisioner)
			return ctrl.Result{}, nil // Wait for PV creation by external provisioner
		} else {
			// No suitable PV found and no StorageClass specified. Static binding failed.
			logger.Info("No suitable PV found and no StorageClass specified for dynamic provisioning", "pvc", pvc.Name)
			r.Recorder.Eventf(pvc, corev1.EventTypeWarning, "ProvisioningFailed", "No existing PV found and no StorageClass specified for dynamic provisioning")
			return ctrl.Result{}, nil // Nothing more this controller can do
		}
	}

	logger.Info("PVC is not Pending, Bound, or Lost - current phase unhandled", "pvc", pvc.Name, "phase", pvc.Status.Phase)
	return ctrl.Result{}, nil
}

// bindToSpecifiedPV handles the case where pvc.Spec.VolumeName is set.
func (r *PersistentVolumeClaimReconciler) bindToSpecifiedPV(ctx context.Context, pvc *corev1.PersistentVolumeClaim) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	pvName := pvc.Spec.VolumeName
	logger.Info("PVC requested specific PV", "pvc", pvc.Name, "pv", pvName)

	pv := &corev1.PersistentVolume{}
	err := r.Get(ctx, types.NamespacedName{Name: pvName}, pv)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.Error(err, "Requested PV not found", "pvc", pvc.Name, "pv", pvName)
			r.Recorder.Eventf(pvc, corev1.EventTypeWarning, "BindingFailed", "PV %q not found", pvName)
			return ctrl.Result{}, nil // Don't requeue if PV is permanently gone
		}
		logger.Error(err, "Failed to get requested PV", "pvc", pvc.Name, "pv", pvName)
		return ctrl.Result{}, err // Requeue on other errors
	}

	// Check if the PV is available and matches the PVC requirements
	// Important: For WaitForFirstConsumer, the external provisioner might create the PV
	// but leave it 'Available'. This controller still needs to perform the binding update.
	if !isPVAvailableForClaim(pv, pvc) {
		logger.Info("Requested PV is not available or does not match PVC requirements", "pvc", pvc.Name, "pv", pv.Name)
		r.Recorder.Eventf(pvc, corev1.EventTypeWarning, "BindingFailed", "PV %q is not available or does not meet requirements", pvName)
		return ctrl.Result{}, nil // Don't requeue if PV is unsuitable
	}

	// Bind the PV to the PVC
	err = r.bindPVToPVC(ctx, pv, pvc)
	if err != nil {
		logger.Error(err, "Failed to bind specified PV to PVC", "pvc", pvc.Name, "pv", pv.Name)
		return ctrl.Result{}, err // Requeue on binding error
	}

	logger.Info("Successfully bound PVC to specified PV", "pvc", pvc.Name, "pv", pv.Name)
	r.Recorder.Eventf(pvc, corev1.EventTypeNormal, "ProvisioningSucceeded", "Successfully bound to PV %q", pv.Name)
	return ctrl.Result{}, nil
}

// findAndBindSuitablePV attempts to find an existing PV that matches the PVC's requirements.
// This PV could be statically provisioned or dynamically provisioned by an external controller.
func (r *PersistentVolumeClaimReconciler) findAndBindSuitablePV(ctx context.Context, pvc *corev1.PersistentVolumeClaim) (bool, *corev1.PersistentVolume, error) {
	logger := log.FromContext(ctx)
	logger.Info("Attempting to find suitable existing/provisioned PV for PVC", "pvc", pvc.Name)

	pvList := &corev1.PersistentVolumeList{}
	// TODO: Add label selector matching if pvc.Spec.Selector is set
	err := r.List(ctx, pvList) // List all PVs, could be optimized with selectors based on SC, access modes etc.
	if err != nil {
		logger.Error(err, "Failed to list PersistentVolumes")
		return false, nil, err
	}

	var bestMatch *corev1.PersistentVolume = nil

	for i := range pvList.Items {
		pv := &pvList.Items[i]
		// Check general availability and requirements match
		if isPVAvailableForClaim(pv, pvc) {
			// Check capacity
			pvQuantity := pv.Spec.Capacity[corev1.ResourceStorage]
			pvcQuantity := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
			if pvQuantity.Cmp(pvcQuantity) >= 0 {

				// Additional check for WaitForFirstConsumer: PV must have matching node affinity if annotation is set
				if nodeName, ok := pvc.Annotations[selectedNodeAnnotation]; ok && nodeName != "" {
					if !checkNodeAffinity(pv, nodeName) {
						logger.V(1).Info("PV NodeAffinity does not match selected node", "pv", pv.Name, "pvc", pvc.Name, "selectedNode", nodeName)
						continue // Skip this PV, doesn't match the required node
					}
				}
				// Found a potential match
				// TODO: Implement better matching logic (e.g., find smallest sufficient PV)
				bestMatch = pv
				break // Take the first suitable one for now
			}
		}
	}

	if bestMatch != nil {
		logger.Info("Found suitable existing/provisioned PV", "pvc", pvc.Name, "pv", bestMatch.Name)
		err = r.bindPVToPVC(ctx, bestMatch, pvc)
		if err != nil {
			logger.Error(err, "Failed to bind found PV to PVC", "pvc", pvc.Name, "pv", bestMatch.Name)
			return false, nil, err // Return error to trigger requeue
		}
		return true, bestMatch, nil // Bound successfully
	}

	logger.Info("No suitable existing/provisioned PV found yet", "pvc", pvc.Name)
	return false, nil, nil // No suitable PV found, no error
}

// Removed simulateDynamicProvisioning function

// bindPVToPVC performs the bi-directional binding between a PV and a PVC.
// This is called for both static binding and after an external provisioner creates a PV.
func (r *PersistentVolumeClaimReconciler) bindPVToPVC(ctx context.Context, pv *corev1.PersistentVolume, pvc *corev1.PersistentVolumeClaim) error {
	logger := log.FromContext(ctx)

	// --- 1. Update PV: Set ClaimRef and Phase ---
	// It's crucial this controller ensures the ClaimRef is set correctly,
	// even if an external provisioner created the PV.
	pvClaimRefChanged := false
	if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID != pvc.UID {
		// Ensure we don't steal a PV already bound to another claim
		if pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.UID != pvc.UID {
			logger.Error(fmt.Errorf("PV %s is already bound to a different PVC %s/%s", pv.Name, pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name), "Binding check failed", "pvc", pvc.Name)
			return fmt.Errorf("PV %s is already bound to a different PVC", pv.Name)
		}
		pv.Spec.ClaimRef = &corev1.ObjectReference{
			Kind:            pvc.Kind,
			APIVersion:      pvc.APIVersion,
			Name:            pvc.Name,
			Namespace:       pvc.Namespace,
			UID:             pvc.UID,
			ResourceVersion: pvc.ResourceVersion,
		}
		pvClaimRefChanged = true
		logger.Info("Setting ClaimRef on PV", "pv", pv.Name, "pvc", pvc.Name)
	}

	pvPhaseChanged := false
	// Also ensure the PV phase is Bound. The provisioner might leave it Available.
	if pv.Status.Phase != corev1.VolumeBound {
		pv.Status.Phase = corev1.VolumeBound
		pvPhaseChanged = true
		logger.Info("Setting PV Phase to Bound", "pv", pv.Name)
	}

	if pvClaimRefChanged { // If spec changed, update spec
		logger.Info("Updating PV Spec with ClaimRef", "pv", pv.Name)
		err := r.Update(ctx, pv)
		if err != nil {
			logger.Error(err, "Failed to update PV spec with ClaimRef", "pv", pv.Name)
			return err
		}
	}
	if pvPhaseChanged { // If status changed, update status
		// Fetch latest PV before status update if spec was updated
		if pvClaimRefChanged {
			latestPV := &corev1.PersistentVolume{}
			if getErr := r.Get(ctx, types.NamespacedName{Name: pv.Name}, latestPV); getErr != nil {
				logger.Error(getErr, "Failed to get latest PV before status update", "pv", pv.Name)
				return getErr
			}
			pv = latestPV                        // Use the latest version for status update
			pv.Status.Phase = corev1.VolumeBound // Ensure phase is still set
		}

		logger.Info("Updating PV Status with Phase", "pv", pv.Name)
		err := r.Status().Update(ctx, pv) // Update status subresource
		if err != nil {
			// Attempt to fetch the PV again to see if the phase was updated concurrently
			checkPV := &corev1.PersistentVolume{}
			if getErr := r.Get(ctx, types.NamespacedName{Name: pv.Name}, checkPV); getErr == nil {
				if checkPV.Status.Phase == corev1.VolumeBound {
					logger.Info("PV Status already updated to Bound concurrently", "pv", pv.Name) // Ignore conflict if status is already correct
				} else {
					logger.Error(err, "Failed to update PV status with Phase", "pv", pv.Name)
					return err // Real error
				}
			} else {
				logger.Error(err, "Failed to update PV status with Phase and failed to re-fetch PV", "pv", pv.Name)
				return err // Real error
			}
		}
	}

	// --- 2. Update PVC: Set VolumeName and Phase ---
	pvcSpecNeedsUpdate := false
	if pvc.Spec.VolumeName != pv.Name {
		pvc.Spec.VolumeName = pv.Name
		pvcSpecNeedsUpdate = true
		logger.Info("Setting VolumeName on PVC", "pvc", pvc.Name, "pv", pv.Name)
	}

	pvcStatusNeedsUpdate := false
	if pvc.Status.Phase != corev1.ClaimBound {
		pvc.Status.Phase = corev1.ClaimBound
		// Set actual capacity from the PV
		pvc.Status.Capacity = pv.Spec.Capacity
		pvcStatusNeedsUpdate = true
		logger.Info("Setting PVC Phase to Bound and copying Capacity", "pvc", pvc.Name)
	}

	if pvcSpecNeedsUpdate {
		logger.Info("Updating PVC Spec with VolumeName", "pvc", pvc.Name)
		err := r.Update(ctx, pvc) // Update the whole PVC object (spec)
		if err != nil {
			logger.Error(err, "Failed to update PVC spec with VolumeName", "pvc", pvc.Name)
			return err
		}
	}

	if pvcStatusNeedsUpdate {
		// Fetch the latest version before status update
		latestPVC := &corev1.PersistentVolumeClaim{}
		if getErr := r.Get(ctx, types.NamespacedName{Namespace: pvc.Namespace, Name: pvc.Name}, latestPVC); getErr != nil {
			logger.Error(getErr, "Failed to get latest PVC before status update", "pvc", pvc.Name)
			return getErr
		}
		latestPVC.Status.Phase = corev1.ClaimBound
		latestPVC.Status.Capacity = pv.Spec.Capacity

		logger.Info("Updating PVC Status to Bound", "pvc", pvc.Name)
		err := r.Status().Update(ctx, latestPVC) // Update the status subresource
		if err != nil {
			// Check if status was updated concurrently
			checkPVC := &corev1.PersistentVolumeClaim{}
			if getErr := r.Get(ctx, types.NamespacedName{Namespace: pvc.Namespace, Name: pvc.Name}, checkPVC); getErr == nil {
				if checkPVC.Status.Phase == corev1.ClaimBound && checkPVC.Spec.VolumeName == pv.Name {
					logger.Info("PVC Status already updated to Bound concurrently", "pvc", pvc.Name) // Ignore conflict if status is already correct
				} else {
					logger.Error(err, "Failed to update PVC status to Bound", "pvc", pvc.Name)
					return err // Real error
				}
			} else {
				logger.Error(err, "Failed to update PVC status to Bound and failed to re-fetch PVC", "pvc", pvc.Name)
				return err // Real error
			}
		}
	}

	logger.Info("Successfully completed bi-directional binding", "pvc", pvc.Name, "pv", pv.Name)
	return nil
}

// reconcileDelete handles cleanup when a PVC is deleted.
func (r *PersistentVolumeClaimReconciler) reconcileDelete(ctx context.Context, pvc *corev1.PersistentVolumeClaim) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Reconciling PVC deletion", "pvc", pvc.Name)

	// Check if the PVC is bound to a PV
	if pvc.Spec.VolumeName == "" {
		logger.Info("PVC is not bound to any PV, removing finalizer directly", "pvc", pvc.Name)
		// If not bound, just remove the finalizer
		if controllerutil.ContainsFinalizer(pvc, pvFinalizer) {
			controllerutil.RemoveFinalizer(pvc, pvFinalizer)
			if err := r.Update(ctx, pvc); err != nil {
				logger.Error(err, "Failed to remove finalizer from unbound PVC", "pvc", pvc.Name)
				return ctrl.Result{}, err
			}
			logger.Info("Removed finalizer from unbound PVC", "pvc", pvc.Name)
		}
		return ctrl.Result{}, nil // Deletion handled
	}

	// PVC is bound, fetch the PV
	pv := &corev1.PersistentVolume{}
	err := r.Get(ctx, types.NamespacedName{Name: pvc.Spec.VolumeName}, pv)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("Bound PV not found, assuming already deleted. Removing finalizer.", "pvc", pvc.Name, "pv", pvc.Spec.VolumeName)
			// If PV is gone, we can remove the finalizer
			if controllerutil.ContainsFinalizer(pvc, pvFinalizer) {
				controllerutil.RemoveFinalizer(pvc, pvFinalizer)
				if err := r.Update(ctx, pvc); err != nil {
					logger.Error(err, "Failed to remove finalizer after bound PV not found", "pvc", pvc.Name)
					return ctrl.Result{}, err
				}
				logger.Info("Removed finalizer after bound PV not found", "pvc", pvc.Name)
			}
			return ctrl.Result{}, nil // Deletion handled
		}
		logger.Error(err, "Failed to get PV associated with deleting PVC", "pvc", pvc.Name, "pv", pvc.Spec.VolumeName)
		return ctrl.Result{}, err // Requeue on error
	}

	// Check the PV's reclaim policy
	reclaimPolicy := pv.Spec.PersistentVolumeReclaimPolicy
	logger.Info("Checking reclaim policy for PV", "pv", pv.Name, "policy", reclaimPolicy)

	// Ensure the PV's ClaimRef points to the deleting PVC before modifying it
	if pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.UID == pvc.UID {
		pvNeedsUpdate := false
		// If the policy is Retain or Delete, we release the PV by clearing the ClaimRef.
		// The actual deletion for the 'Delete' policy is handled by the external provisioner
		// or the default PV controller when it sees the released PV.
		if reclaimPolicy == corev1.PersistentVolumeReclaimRetain || reclaimPolicy == corev1.PersistentVolumeReclaimDelete {
			logger.Info("PV reclaim policy requires release. Releasing PV by clearing ClaimRef.", "pv", pv.Name, "policy", reclaimPolicy)
			pv.Spec.ClaimRef = nil
			pvNeedsUpdate = true
		} else {
			// Handle Recycle or other policies if necessary
			logger.Info("PV reclaim policy is not Retain or Delete. Releasing PV by clearing ClaimRef.", "pv", pv.Name, "policy", reclaimPolicy)
			pv.Spec.ClaimRef = nil
			pvNeedsUpdate = true
		}

		if pvNeedsUpdate {
			logger.Info("Updating PV to clear ClaimRef", "pv", pv.Name, "policy", reclaimPolicy)
			err = r.Update(ctx, pv) // Update PV spec
			if err != nil {
				logger.Error(err, "Failed to update PV to clear ClaimRef", "pv", pv.Name)
				return ctrl.Result{}, err // Requeue
			}
			logger.Info("Cleared ClaimRef on PV", "pv", pv.Name, "policy", reclaimPolicy)
		}

	} else {
		logger.Info("PV ClaimRef does not point to this PVC or is nil. Assuming PV is already released or handled.", "pv", pv.Name)
	}

	// Remove the finalizer from the PVC now that the PV has been handled (released)
	if controllerutil.ContainsFinalizer(pvc, pvFinalizer) {
		logger.Info("Removing finalizer from PVC", "pvc", pvc.Name)
		controllerutil.RemoveFinalizer(pvc, pvFinalizer)
		if err := r.Update(ctx, pvc); err != nil {
			logger.Error(err, "Failed to remove finalizer from PVC", "pvc", pvc.Name)
			return ctrl.Result{}, err
		}
		logger.Info("Successfully removed finalizer from PVC", "pvc", pvc.Name)
	}

	return ctrl.Result{}, nil // Deletion process complete for this controller's responsibility
}

// isPVAvailableForClaim checks if a PV is suitable for binding to a PVC.
func isPVAvailableForClaim(pv *corev1.PersistentVolume, pvc *corev1.PersistentVolumeClaim) bool {
	// PV must be Available or Bound (only if ClaimRef is nil or points to *this* PVC)
	if pv.Status.Phase != corev1.VolumeAvailable && pv.Status.Phase != corev1.VolumeBound {
		return false
	}
	// Crucial check: If PV is Bound, it MUST be bound to this PVC already, or ClaimRef must be nil (e.g., Released)
	// We should not bind a PV that is actively bound to another PVC.
	if pv.Status.Phase == corev1.VolumeBound && pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.UID != pvc.UID {
		return false // Bound to another PVC
	}

	// Check if PV is being deleted
	if pv.ObjectMeta.DeletionTimestamp != nil {
		return false
	}

	// StorageClassName must match
	if !storageClassNamesEqual(pvc.Spec.StorageClassName, pv.Spec.StorageClassName) {
		return false
	}

	// AccessModes must be compatible (PV must support all modes requested by PVC)
	if !containsAccessModes(pv.Spec.AccessModes, pvc.Spec.AccessModes) {
		return false
	}

	// VolumeMode must match
	if !volumeModesEqual(pvc.Spec.VolumeMode, pv.Spec.VolumeMode) {
		return false
	}

	// Capacity check (done later in findAndBindSuitablePV)

	// TODO: Check label selectors (pvc.Spec.Selector) against PV labels

	return true // PV is potentially suitable
}

// containsAccessModes checks if pvModes contains all pvcModes.
func containsAccessModes(pvModes []corev1.PersistentVolumeAccessMode, pvcModes []corev1.PersistentVolumeAccessMode) bool {
	if len(pvcModes) == 0 {
		return true // If PVC asks for nothing specific, any PV modes are fine? Or should match default? Let's assume true.
	}
	if len(pvModes) == 0 {
		return false // PV supports no modes, but PVC requested some.
	}

	for _, pvcMode := range pvcModes {
		found := false
		for _, pvMode := range pvModes {
			if pvcMode == pvMode {
				found = true
				break
			}
		}
		if !found {
			return false // A mode requested by PVC is not supported by PV
		}
	}
	return true
}

// checkNodeAffinity verifies if the PV's node affinity matches the selected node.
// Returns true if affinity matches or if PV has no affinity.
func checkNodeAffinity(pv *corev1.PersistentVolume, nodeName string) bool {
	if pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil {
		return true // PV has no required node affinity, so it's compatible with any node.
	}

	// Check if the required node selector terms match the nodeName
	// This is a simplified check assuming the standard kubernetes.io/hostname label.
	// A more robust implementation would evaluate all terms and expressions.
	nodeLabels := labels.Set{corev1.LabelHostname: nodeName} // Simulate node labels

	for _, term := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
		selector, err := nodeSelectorRequirementsAsSelector(term.MatchExpressions)
		if err != nil {
			// Log error? Treat as non-match?
			log.Log.Error(err, "Failed to parse PV NodeAffinity MatchExpressions", "pv", pv.Name)
			return false
		}
		if selector.Matches(nodeLabels) {
			return true // Found a term that matches the node
		}
	}

	return false // No required term matched the node
}

// nodeSelectorRequirementsAsSelector converts NodeSelectorRequirements to a labels.Selector
// (Helper function adapted from Kubernetes source)
func nodeSelectorRequirementsAsSelector(nsm []corev1.NodeSelectorRequirement) (labels.Selector, error) {
	if len(nsm) == 0 {
		return labels.Nothing(), nil
	}
	selector := labels.NewSelector()
	for _, expr := range nsm {
		var op selection.Operator
		switch expr.Operator {
		case corev1.NodeSelectorOpIn:
			op = selection.In
		case corev1.NodeSelectorOpNotIn:
			op = selection.NotIn
		case corev1.NodeSelectorOpExists:
			op = selection.Exists
		case corev1.NodeSelectorOpDoesNotExist:
			op = selection.DoesNotExist
		case corev1.NodeSelectorOpGt:
			op = selection.GreaterThan
		case corev1.NodeSelectorOpLt:
			op = selection.LessThan
		default:
			return nil, fmt.Errorf("%q is not a valid node selector operator", expr.Operator)
		}
		r, err := labels.NewRequirement(expr.Key, op, expr.Values)
		if err != nil {
			return nil, err
		}
		selector = selector.Add(*r)
	}
	return selector, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *PersistentVolumeClaimReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Removed FieldIndexer setup for hostPath

	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.PersistentVolumeClaim{}).
		// Watch PVs: Events on PVs (like creation by external provisioner, or status changes)
		// will trigger reconciliation for the associated PVC (if ClaimRef is set) or potentially all PVCs.
		Watches(&corev1.PersistentVolume{}, &pvEventHandler{client: mgr.GetClient()}). // Custom handler might be needed for complex scenarios
		// Owns(&corev1.PersistentVolume{}). // Using Owns might be too broad if PVs are managed externally. Watches is often better.
		Complete(r)
}

// --- Helper functions (resourceListsEqual, accessModesEqual, etc.) remain the same ---
// ... (Keep the existing helper functions below)

// Helper function to check if two ResourceLists are equal.
func resourceListsEqual(list1, list2 corev1.ResourceList) bool {
	if len(list1) != len(list2) {
		return false
	}
	for key, val1 := range list1 {
		val2, ok := list2[key]
		if !ok || !val1.Equal(val2) {
			return false
		}
	}
	return true
}

// Helper function to check if two slices of AccessModes are equal (ignoring order).
func accessModesEqual(modes1, modes2 []corev1.PersistentVolumeAccessMode) bool {
	if len(modes1) != len(modes2) {
		return false
	}
	map1 := make(map[corev1.PersistentVolumeAccessMode]struct{})
	for _, mode := range modes1 {
		map1[mode] = struct{}{}
	}
	for _, mode := range modes2 {
		if _, ok := map1[mode]; !ok {
			return false
		}
	}
	return true
}

// Helper function to check if two selectors are equal.
func selectorsEqual(s1, s2 *metav1.LabelSelector) bool {
	return reflect.DeepEqual(s1, s2)
}

// Helper function to build requirements from a LabelSelector.
func buildRequirements(selector *metav1.LabelSelector) ([]labels.Requirement, error) {
	if selector == nil {
		return nil, nil
	}

	requirements := make([]labels.Requirement, 0, len(selector.MatchLabels)+len(selector.MatchExpressions))

	for key, value := range selector.MatchLabels {
		r, err := labels.NewRequirement(key, selection.Equals, []string{value})
		if err != nil {
			return nil, fmt.Errorf("failed to create requirement for label %s=%s: %w", key, value, err)
		}
		requirements = append(requirements, *r)
	}

	for _, expr := range selector.MatchExpressions {
		var op selection.Operator
		switch expr.Operator {
		case metav1.LabelSelectorOpIn:
			op = selection.In
		case metav1.LabelSelectorOpNotIn:
			op = selection.NotIn
		case metav1.LabelSelectorOpExists:
			op = selection.Exists
		case metav1.LabelSelectorOpDoesNotExist:
			op = selection.DoesNotExist
		default:
			return nil, fmt.Errorf("unsupported operator: %s", expr.Operator)
		}
		r, err := labels.NewRequirement(expr.Key, op, expr.Values)
		if err != nil {
			return nil, fmt.Errorf("failed to create requirement for expression %s %s %v: %w", expr.Key, expr.Operator, expr.Values, err)
		}
		requirements = append(requirements, *r)
	}

	return requirements, nil
}

// Helper function to check if PV labels match PVC selector requirements.
func matchesSelector(pvLabels map[string]string, requirements []labels.Requirement) bool {
	if len(requirements) == 0 {
		return true // No selector means match all
	}
	labelSet := labels.Set(pvLabels)
	selector := labels.NewSelector().Add(requirements...)
	return selector.Matches(labelSet)
}

// Helper function to compare VolumeModes, handling nil pointers.
func volumeModesEqual(mode1, mode2 *corev1.PersistentVolumeMode) bool {
	pvMode := corev1.PersistentVolumeFilesystem  // Default if nil
	pvcMode := corev1.PersistentVolumeFilesystem // Default if nil
	if mode1 != nil {
		pvMode = *mode1
	}
	if mode2 != nil {
		pvcMode = *mode2
	}
	return pvMode == pvcMode
}

// Helper function to compare StorageClassNames, handling nil/empty strings.
func storageClassNamesEqual(pvcScName *string, pvScName string) bool {
	pvcName := ""
	if pvcScName != nil {
		pvcName = *pvcScName
	}
	// Both PVC and PV must specify the same SC, or both must be empty.
	return pvcName == pvScName
}

// Helper function to check if a string exists in a slice of strings.
func containsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

// Helper function to remove a string from a slice of strings.
func removeString(slice []string, s string) []string {
	result := []string{}
	for _, item := range slice {
		if item != s {
			result = append(result, item)
		}
	}
	return result
}

// Removed generatePVName helper function

// pvEventHandler triggers reconciles for PVCs when related PVs change.
// This basic version triggers reconcile for the bound PVC if ClaimRef is set.
// More sophisticated logic might be needed depending on how unbound PVs should trigger checks.
type pvEventHandler struct {
	client client.Client
}

func (e *pvEventHandler) Create(ctx context.Context, evt event.CreateEvent, q workqueue.RateLimitingInterface) {
	pv, ok := evt.Object.(*corev1.PersistentVolume)
	if !ok {
		log.Log.Error(nil, "PV CreateEvent has unexpected type", "object", evt.Object)
		return
	}
	// If a new PV appears and is Available, potentially trigger reconciles for Pending PVCs?
	// This could be broad. Let's start simple: only trigger if ClaimRef is already set (unlikely on create).
	logger := log.FromContext(ctx)
	logger.V(1).Info("PV Create event", "pv", pv.Name)
	if pv.Spec.ClaimRef != nil {
		q.Add(ctrl.Request{NamespacedName: types.NamespacedName{
			Namespace: pv.Spec.ClaimRef.Namespace,
			Name:      pv.Spec.ClaimRef.Name,
		}})
	}
}

func (e *pvEventHandler) Update(ctx context.Context, evt event.UpdateEvent, q workqueue.RateLimitingInterface) {
	oldPv, okOld := evt.ObjectOld.(*corev1.PersistentVolume)
	newPv, okNew := evt.ObjectNew.(*corev1.PersistentVolume)
	if !okOld || !okNew {
		log.Log.Error(nil, "PV UpdateEvent has unexpected type", "oldObject", evt.ObjectOld, "newObject", evt.ObjectNew)
		return
	}
	logger := log.FromContext(ctx)
	logger.V(1).Info("PV Update event", "pv", newPv.Name, "oldPhase", oldPv.Status.Phase, "newPhase", newPv.Status.Phase)

	// If ClaimRef changes or Phase changes (e.g., Available -> Bound), reconcile the referenced PVC.
	if (newPv.Spec.ClaimRef != nil && (oldPv.Spec.ClaimRef == nil || oldPv.Spec.ClaimRef.UID != newPv.Spec.ClaimRef.UID)) ||
		(newPv.Status.Phase != oldPv.Status.Phase && newPv.Spec.ClaimRef != nil) {
		logger.V(1).Info("PV update triggering PVC reconcile", "pv", newPv.Name, "pvcNamespace", newPv.Spec.ClaimRef.Namespace, "pvcName", newPv.Spec.ClaimRef.Name)
		q.Add(ctrl.Request{NamespacedName: types.NamespacedName{
			Namespace: newPv.Spec.ClaimRef.Namespace,
			Name:      newPv.Spec.ClaimRef.Name,
		}})
	} else if newPv.Spec.ClaimRef != nil && oldPv.Spec.ClaimRef != nil && newPv.Spec.ClaimRef.UID == oldPv.Spec.ClaimRef.UID {
		// Also reconcile if the PV is already bound but something else changed (e.g. capacity update after resize)
		logger.V(1).Info("PV update triggering bound PVC reconcile", "pv", newPv.Name, "pvcNamespace", newPv.Spec.ClaimRef.Namespace, "pvcName", newPv.Spec.ClaimRef.Name)
		q.Add(ctrl.Request{NamespacedName: types.NamespacedName{
			Namespace: newPv.Spec.ClaimRef.Namespace,
			Name:      newPv.Spec.ClaimRef.Name,
		}})
	}

	// If a PV becomes Available, we might want to trigger reconciles for *all* Pending PVCs
	// that could potentially bind to it. This can be expensive.
	// A more targeted approach would be better, perhaps using predicates.
	// Example (potentially broad):
	// if oldPv.Status.Phase != corev1.VolumeAvailable && newPv.Status.Phase == corev1.VolumeAvailable {
	//    logger.Info("PV became available, potentially triggering checks for Pending PVCs", "pv", newPv.Name)
	//    // Need logic here to find relevant Pending PVCs and enqueue them.
	// }
}

func (e *pvEventHandler) Delete(ctx context.Context, evt event.DeleteEvent, q workqueue.RateLimitingInterface) {
	pv, ok := evt.Object.(*corev1.PersistentVolume)
	if !ok {
		log.Log.Error(nil, "PV DeleteEvent has unexpected type", "object", evt.Object)
		return
	}
	logger := log.FromContext(ctx)
	logger.V(1).Info("PV Delete event", "pv", pv.Name)
	// If a bound PV is deleted, reconcile the PVC it was bound to.
	if pv.Spec.ClaimRef != nil {
		q.Add(ctrl.Request{NamespacedName: types.NamespacedName{
			Namespace: pv.Spec.ClaimRef.Namespace,
			Name:      pv.Spec.ClaimRef.Name,
		}})
	}
}

func (e *pvEventHandler) Generic(ctx context.Context, evt event.GenericEvent, q workqueue.RateLimitingInterface) {
	// Handle generic events if needed, e.g., periodic checks
}

// **Key Changes:**

// 1.  **Removed `simulateDynamicProvisioning` function:** This entire function is gone.
// 2.  **Removed Call in `Reconcile`:** The section that previously called `simulateDynamicProvisioning` now logs a message indicating it's waiting for the external provisioner and returns `ctrl.Result{}`.
// 3.  **Removed Unused Imports:** `math/rand`, `os`, `path/filepath`, `strconv`, `time` were removed.
// 4.  **Removed FieldIndexer:** The `SetupWithManager` no longer sets up the indexer for `spec.hostPath.path`.
// 5.  **RBAC Update (Commented):** Removed `create` verb for `persistentvolumes` in the RBAC comment, as this controller no longer creates them.
// 6.  **`findAndBindSuitablePV`:** Added a check (`checkNodeAffinity`) within this function to ensure that if `WaitForFirstConsumer` is active (indicated by the annotation on the PVC), the found PV actually matches the required node affinity.
// 7.  **`isPVAvailableForClaim`:** Added a check to ensure the PV is not being deleted. Also refined the `StorageClassName` check slightly.
// 8.  **`bindPVToPVC`:** Added more robust checks and logging around updating PV/PVC spec and status, including fetching the latest object version before status updates and handling potential conflicts if an update happened concurrently. It also ensures a PV isn't "stolen" if it's already bound to a different PVC.
// 9.  **`SetupWithManager`:** Changed `Owns(&corev1.PersistentVolume{})` to `Watches(&corev1.PersistentVolume{}, &pvEventHandler{client: mgr.GetClient()})`. Using `Watches` with a custom handler gives more control over *which* PVCs get reconciled when a PV changes. `Owns` might trigger reconciles too broadly if PVs aren't directly created by this controller.
// 10. **Added `pvEventHandler`:** A basic event handler for PV events. When a PV is updated (e.g., becomes `Available`, gets a `ClaimRef`, changes phase) or deleted, it enqueues the corresponding PVC (if referenced) for reconciliation. This helps ensure the PVC controller reacts promptly when the external provisioner creates or modifies a relevant PV. *(Note: This handler is basic and might need refinement for optimal performance in large clusters)*.
// 11. **Added `checkNodeAffinity` helper:** A function to parse PV node affinity rules and check if they match a given node name.

// Now, your controller will:
// * Bind pre-existing static PVs.
// * Wait for the scheduler if `WaitForFirstConsumer` is set.
// * Wait for an external provisioner to create a PV if dynamic provisioning is required.
// * Bind the PVC to the PV once the external provisioner creates an `Available` PV that matches the requirements (including node affinity for `WaitForFirstConsumer
