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
	"reflect"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// StatefulSetReconciler reconciles a StatefulSet object
type StatefulSetReconciler struct {
	client.Client
	Scheme *runtime.Scheme
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
	log := log.FromContext(ctx)

	// Fetch the StatefulSet instance
	statefulSet := &appsv1.StatefulSet{}
	if err := r.Get(ctx, req.NamespacedName, statefulSet); err != nil {
		if client.IgnoreNotFound(err) != nil {
			log.Error(err, "unable to fetch StatefulSet")
			return ctrl.Result{}, err
		}
		// StatefulSet not found, likely deleted, return
		return ctrl.Result{}, nil
	}

	// Check if the StatefulSet is being deleted
	if !statefulSet.DeletionTimestamp.IsZero() {
		// The object is being deleted
		log.Info("StatefulSet is being deleted", "name", statefulSet.Name)
		return ctrl.Result{}, nil
	}

	// Get the current set of pods controlled by this StatefulSet
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList, client.InNamespace(req.Namespace), client.MatchingLabels(statefulSet.Spec.Selector.MatchLabels)); err != nil {
		log.Error(err, "unable to list pods")
		return ctrl.Result{}, err
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
			log.Error(err, "Failed to get ordinal from pod name", "pod", pod.Name)
			continue
		}
		podMap[ordinal] = pod
	}

	// Create/update pods as needed
	currentReplicas := int(*statefulSet.Spec.Replicas)
	for i := 0; i < currentReplicas; i++ {
		if pod, exists := podMap[i]; exists {
			// Pod exists, check if it needs update
			if podNeedsUpdate(pod, statefulSet) {
				log.Info("Pod needs update", "pod", pod.Name)
				// In a real controller, you would handle pod updates here
			}
		} else {
			// Pod doesn't exist, create it
			newPod, err := newPodForStatefulSet(statefulSet, i)
			if err != nil {
				log.Error(err, "Failed to create new pod", "ordinal", i)
				return ctrl.Result{}, err
			}

			log.Info("Creating new Pod", "pod", newPod.Name)
			if err := r.Create(ctx, newPod); err != nil {
				log.Error(err, "Failed to create Pod", "pod", newPod.Name)
				return ctrl.Result{}, err
			}
		}
	}

	// Delete excess pods if scaling down
	for i := currentReplicas; i < len(podMap); i++ {
		if pod, exists := podMap[i]; exists {
			log.Info("Deleting excess Pod", "pod", pod.Name)
			if err := r.Delete(ctx, pod); err != nil {
				log.Error(err, "Failed to delete Pod", "pod", pod.Name)
				return ctrl.Result{}, err
			}
		}
	}

	// Update StatefulSet status
	status := calculateStatus(statefulSet, podMap)
	if !reflect.DeepEqual(statefulSet.Status, status) {
		statefulSet.Status = status
		if err := r.Status().Update(ctx, statefulSet); err != nil {
			log.Error(err, "Failed to update StatefulSet status")
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
	// In a real implementation, you would compare pod spec vs template
	// This is a simplified version
	return false
}

// newPodForStatefulSet creates a new Pod for a StatefulSet with given index
func newPodForStatefulSet(sts *appsv1.StatefulSet, ordinal int) (*corev1.Pod, error) {
	podName := fmt.Sprintf("%s-%d", sts.Name, ordinal)

	// Create a new pod based on the StatefulSet's pod template
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: sts.Namespace,
			Labels:    sts.Spec.Template.Labels,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(sts, appsv1.SchemeGroupVersion.WithKind("StatefulSet")),
			},
		},
		Spec: sts.Spec.Template.Spec,
	}

	return pod, nil
}

// calculateStatus calculates the StatefulSet status based on the pods
func calculateStatus(sts *appsv1.StatefulSet, pods map[int]*corev1.Pod) appsv1.StatefulSetStatus {
	status := sts.Status.DeepCopy()

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
