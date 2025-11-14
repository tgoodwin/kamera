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
	"strconv"
	"strings"

	"github.com/tgoodwin/kamera/pkg/tag"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var loggr = logf.Log

// StatefulSetReconciler reconciles a StatefulSet object
type StatefulSetReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	KWOKMode bool
}

// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=statefulsets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the StatefulSet object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.18.4/pkg/reconcile

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *StatefulSetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	loggr := logf.FromContext(ctx).WithName("sleeve:statefulset-controller")

	// Fetch the StatefulSet instance
	statefulSet := &appsv1.StatefulSet{}
	if err := r.Get(ctx, req.NamespacedName, statefulSet); err != nil {
		if client.IgnoreNotFound(err) != nil {
			loggr.Error(err, "unable to fetch StatefulSet")
			return ctrl.Result{}, err
		}
		// StatefulSet not found, likely deleted, return
		return ctrl.Result{}, nil
	}
	statefulSet.SetGroupVersionKind(appsv1.SchemeGroupVersion.WithKind("StatefulSet"))

	intendedReplicas := int(*statefulSet.Spec.Replicas)
	if statefulSet.GetDeletionTimestamp() != nil {
		loggr.Info("StatefulSet has been marked for deletion")

		// get all the pods owned by this StatefulSet
		podList := &corev1.PodList{}
		if err := r.List(ctx, podList, client.InNamespace(req.Namespace),
			client.MatchingLabels(statefulSet.Spec.Selector.MatchLabels)); err != nil {
			loggr.Error(err, "unable to list pods")
			return ctrl.Result{}, err
		}

		// organize pods by ordinal
		stsPods := make([]*corev1.Pod, 0)
		for i := range podList.Items {
			pod := &podList.Items[i]
			if metav1.IsControlledBy(pod, statefulSet) {
				stsPods = append(stsPods, pod)
			}
		}
		// sort pods by ordinal highest to lowest
		sort.Slice(stsPods, func(i, j int) bool {
			ordinalI, _ := getOrdinal(stsPods[i].Name)
			ordinalJ, _ := getOrdinal(stsPods[j].Name)
			return ordinalI > ordinalJ
		})

		// check if any pods still need to be deleted
		podsRemaining := false
		for _, pod := range stsPods {
			if pod.GetDeletionTimestamp() == nil {
				// this pod is not yet deleted
				loggr.V(1).Info("Deleting pod", "pod", pod.Name)
				if err := r.Delete(ctx, pod); err != nil {
					loggr.Error(err, "Failed to delete pod")
					return ctrl.Result{}, err
				}
				podsRemaining = true
				break
			}
		}
		if podsRemaining || len(stsPods) > 0 {
			// still pods remaining, return and wait for next reconcile
			loggr.V(1).Info("Pods remaining, requeueing")
			return ctrl.Result{Requeue: true}, nil
		}

		// all pods are deleted, we can remove the finalizer
		loggr.V(1).Info("StatefulSet pods are all deleted, removing finalizer")
		if controllerutil.RemoveFinalizer(statefulSet, "test.k8ssandra.io/sts-finalizer") {
			if err := r.Update(ctx, statefulSet); err != nil {
				loggr.Error(err, "Failed to remove finalizer from StatefulSet")
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Get the current set of pods controlled by this StatefulSet
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList, client.InNamespace(req.Namespace), client.MatchingLabels(statefulSet.Spec.Selector.MatchLabels)); err != nil {
		loggr.Error(err, "unable to list pods")
		return ctrl.Result{}, err
	}

	stsPods := []*corev1.Pod{}
	for _, pod := range podList.Items {
		if pod.OwnerReferences[0].Name == statefulSet.Name {
			stsPods = append(stsPods, &pod)
		}
	}

	if len(stsPods) > intendedReplicas {
		// We need to delete the pods..
		for i := len(stsPods) - 1; i >= intendedReplicas; i-- {
			pod := stsPods[i]
			if pod.GetDeletionTimestamp() == nil {
				loggr.V(1).Info("Deleting pod", "pod", pod.Name, "Spec.Replicas", intendedReplicas, "currPods", len(stsPods))
				if err := r.Client.Delete(ctx, pod); err != nil {
					loggr.Error(err, "Failed to delete extra pod from this StS")
					return ctrl.Result{}, err
				}
			}
		}
		return ctrl.Result{}, nil
	}

	// Organize pods by their ordinal index
	podMap := make(map[int]*corev1.Pod)
	for i := range podList.Items {
		pod := &podList.Items[i]
		if !metav1.IsControlledBy(pod, statefulSet) {
			continue
		}

		// Extract the ordinal from the pod name
		ordinal, err := getOrdinal(pod.Name)
		if err != nil {
			loggr.Error(err, "Failed to get ordinal from pod name", "pod", pod.Name)
			continue
		}
		podMap[ordinal] = pod
	}

	loggr.WithValues(
		"currPods", len(podMap),
		"spec.Replicas", int(*statefulSet.Spec.Replicas),
	).Info("Reconciling StatefulSet")

	// Create/update pods as needed
	currentReplicas := int(*statefulSet.Spec.Replicas)
	for i := 0; i < currentReplicas; i++ {
		if pod, exists := podMap[i]; exists {
			// Pod exists, check if it needs update
			if podNeedsUpdate(pod, statefulSet) && false {
				loggr.Info("Pod needs update", "pod", pod.Name)
				// In a real controller, you would handle pod updates here
				// pod spec is immutable, so you would need to create a new pod
			}
		} else {
			var pod *corev1.Pod
			var err error
			podName := fmt.Sprintf("%s-%d", statefulSet.Name, i)
			if r.KWOKMode {
				pod, err = createDummyPod(podName, statefulSet.Namespace, statefulSet.Spec.Template.DeepCopy())
				if err != nil {
					loggr.Error(err, "Failed to create dummy pod", "pod", podName)
					return ctrl.Result{}, err
				}
			} else {
				pod, err = createPodForStatefulSet(statefulSet, i)
				if err != nil {
					loggr.Error(err, "Failed to create pod for StatefulSet", "pod", podName)
					return ctrl.Result{}, err
				}
				mountName := "server-data"
				if err := createPVCIfNotExists(ctx, r.Client, mountName, pod); err != nil {
					loggr.Error(err, "Failed to create PVC for pod", "pod", pod.Name)
					return ctrl.Result{}, err
				}
			}

			if err := controllerutil.SetControllerReference(statefulSet, pod, r.Scheme); err != nil {
				loggr.Error(err, "Failed to set controller reference for pod", "pod", pod.Name)
				return ctrl.Result{}, err
			}

			loggr.Info("Creating new Pod", "pod", pod.Name, "labels", tag.GetSleeveLabels(pod))
			if err := r.Create(ctx, pod); err != nil {
				loggr.Error(err, "Failed to create Pod", "pod", pod.Name)
				return ctrl.Result{}, err
			}
		}
	}

	// Delete excess pods if scaling down
	for i := currentReplicas; i < len(podMap); i++ {
		if pod, exists := podMap[i]; exists {
			loggr.Info("Deleting excess Pod", "pod", pod.Name)
			if err := r.Delete(ctx, pod); err != nil {
				loggr.Error(err, "Failed to delete Pod", "pod", pod.Name)
				return ctrl.Result{}, err
			}
		}
	}

	// Update StatefulSet status
	status := calculateStatus(statefulSet, podMap)
	if !equality.Semantic.DeepEqual(statefulSet.Status, status) {
		statefulSet.Status = status
		if err := r.Status().Update(ctx, statefulSet); err != nil {
			loggr.Error(err, "Failed to update StatefulSet status")
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

// Helper functions

// getOrdinal extracts the ordinal index from the StatefulSet pod name
func getOrdinal(podName string) (int, error) {
	// Pod name format: <statefulset-name>-<ordinal>
	lastDash := strings.LastIndex(podName, "-")
	if lastDash == -1 {
		return 0, fmt.Errorf("pod name %s does not match StatefulSet pod name format", podName)
	}

	ordinalStr := podName[lastDash+1:]
	ordinal, err := strconv.Atoi(ordinalStr)
	if err != nil {
		return 0, err
	}
	return ordinal, nil
}

// podNeedsUpdate checks if a pod needs to be updated
func podNeedsUpdate(pod *corev1.Pod, sts *appsv1.StatefulSet) bool {
	// Compare the pod spec with the StatefulSet template spec
	// This is a simplified version that only compares the container specs
	if len(pod.Spec.Containers) != len(sts.Spec.Template.Spec.Containers) {
		return true
	}

	for i, container := range pod.Spec.Containers {
		if !equality.Semantic.DeepEqual(container, sts.Spec.Template.Spec.Containers[i]) {
			return true
		}
	}

	// You can add more comparisons here (e.g., volumes, labels, annotations, etc.)
	return false
}

// createDummyPod creates a dummy pod for testing purposes -- it becomes immediately running
func createDummyPod(podName, namespace string, template *corev1.PodTemplateSpec) (*corev1.Pod, error) {
	// Create a new pod based on the StatefulSet's pod template
	// restart := corev1.ContainerRestartPolicyAlways
	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(),
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        podName,
			Namespace:   namespace,
			Labels:      template.Labels,
			Annotations: template.Annotations,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "pause",
					Image: "k8s.gcr.io/pause:3.5",
					// no readiness probe, so it will be assumed ready
				},
			},
		},
	}

	return pod, nil
}

func createPodForStatefulSet(statefulSet *appsv1.StatefulSet, ordinal int) (*corev1.Pod, error) {
	// Start with the pod template from the StatefulSet
	podSpec := statefulSet.Spec.Template.Spec.DeepCopy()

	labels := statefulSet.GetLabels()
	// get non-sleeve labels
	out := make(map[string]string)
	for k, v := range labels {
		if k == tag.TraceyObjectID {
			continue
		}
		out[k] = v
	}

	// For each volumeClaimTemplate, add a corresponding volume to the pod
	for _, template := range statefulSet.Spec.VolumeClaimTemplates {
		volumeName := template.Name
		claimName := fmt.Sprintf("%s-%s-%d", volumeName, statefulSet.Name, ordinal)

		// Create a volume that references the PVC
		volume := corev1.Volume{
			Name: volumeName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: claimName,
				},
			},
		}

		// Add it to the pod's volumes
		podSpec.Volumes = append(podSpec.Volumes, volume)
	}

	// Continue with pod creation...
	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(),
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%d", statefulSet.Name, ordinal),
			Namespace: statefulSet.Namespace,
			Labels:    out,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(statefulSet, appsv1.SchemeGroupVersion.WithKind("StatefulSet")),
			},
		},
		Spec: *podSpec,
	}
	return pod, nil
}

// calculateStatus calculates the StatefulSet status based on the pods
func calculateStatus(sts *appsv1.StatefulSet, pods map[int]*corev1.Pod) appsv1.StatefulSetStatus {
	status := sts.Status.DeepCopy()

	// count all non-deleted pods
	var total int32
	for _, pod := range pods {
		if pod.DeletionTimestamp == nil {
			total++
		}
	}

	// Count ready and current pods
	readyReplicas := 0
	for _, pod := range pods {
		if isPodReady(pod) {
			readyReplicas++
		}
	}

	status.ReadyReplicas = int32(readyReplicas)
	status.Replicas = int32(len(pods))

	// You would calculate other status fields here in a real implementation

	return *status
}

// SetupWithManager sets up the controller with the Manager.
func (r *StatefulSetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1.StatefulSet{}).
		Complete(r)
}

// TODO These should be created to test certain decommission features also
func createPVCIfNotExists(ctx context.Context, cli client.Client, mountName string, pod *corev1.Pod) error {
	volumeMode := new(corev1.PersistentVolumeMode)
	*volumeMode = corev1.PersistentVolumeFilesystem
	storageClassName := "standard"

	pvcName := types.NamespacedName{
		Name:      fmt.Sprintf("%s-%s", mountName, pod.Name),
		Namespace: pod.Namespace,
	}

	// Check if the PVC already exists
	existingPVC := &corev1.PersistentVolumeClaim{}
	if err := cli.Get(ctx, pvcName, existingPVC); err == nil {
		existingPVC.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"))
		// PVC already exists, no need to create
		return nil
	} else if client.IgnoreNotFound(err) != nil {
		// Return error if it's not a "not found" error
		return err
	}

	loggr.Info("Creating PVC", "pvc", pvcName)

	// Create a new PVC
	pvc := &corev1.PersistentVolumeClaim{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(),
			Kind:       "PersistentVolumeClaim",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName.Name,
			Namespace: pvcName.Namespace,
			Labels:    pod.Labels,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					// TODO Hardcoded not real value
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
			StorageClassName: &storageClassName,
			VolumeMode:       volumeMode,
		},
	}

	return cli.Create(ctx, pvc)
}
