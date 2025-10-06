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
	"math/rand"
	"sort"

	"github.com/tgoodwin/kamera/pkg/tag"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// +kubebuilder:rbac:groups=apps,resources=replicasets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=replicasets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=replicasets/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete

// ReplicaSetReconciler reconciles a ReplicaSet object
type ReplicaSetReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	KWOKMode bool
}

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *ReplicaSetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// Fetch the ReplicaSet instance
	rs := &appsv1.ReplicaSet{}
	if err := r.Get(ctx, req.NamespacedName, rs); err != nil {
		if client.IgnoreNotFound(err) != nil {
			log.Error(err, "unable to fetch ReplicaSet")
			return ctrl.Result{}, err
		}
		// ReplicaSet not found, likely deleted, return
		return ctrl.Result{}, nil
	}

	// Check if the ReplicaSet is being deleted
	if !rs.DeletionTimestamp.IsZero() {
		log.Info("ReplicaSet is being deleted", "name", rs.Name)
		return ctrl.Result{}, nil
	}

	// List existing pods for this ReplicaSet
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList,
		client.InNamespace(rs.Namespace),
		client.MatchingLabels(rs.Spec.Selector.MatchLabels)); err != nil {
		log.Error(err, "unable to list Pods")
		return ctrl.Result{}, err
	}

	// Filter out pods that are not controlled by this ReplicaSet
	var filteredPods []corev1.Pod
	for _, pod := range podList.Items {
		if metav1.IsControlledBy(&pod, rs) {
			filteredPods = append(filteredPods, pod)
		}
	}

	// Get the number of active pods (not terminating)
	var activePods []corev1.Pod
	for _, pod := range filteredPods {
		if pod.DeletionTimestamp.IsZero() {
			activePods = append(activePods, pod)
		}
	}

	// Calculate how many pods we need to create or delete
	desiredReplicas := 1
	if rs.Spec.Replicas != nil {
		desiredReplicas = int(*rs.Spec.Replicas)
	}
	diff := desiredReplicas - len(activePods)

	// Create pods if needed
	if diff > 0 {
		log.Info("Creating pods", "count", diff)
		for i := 0; i < diff; i++ {
			pod, err := r.createPodFromTemplate(rs)
			if err != nil {
				log.Error(err, "failed to create pod from template")
				return ctrl.Result{}, err
			}

			if err := r.Create(ctx, pod); err != nil {
				log.Error(err, "failed to create pod")
				return ctrl.Result{}, err
			}
		}
	}

	// Delete pods if needed
	if diff < 0 {
		log.Info("Deleting pods", "count", -diff)
		// Sort pods by creation time, delete newest first
		sort.Slice(activePods, func(i, j int) bool {
			return activePods[i].CreationTimestamp.Before(&activePods[j].CreationTimestamp)
		})

		// Delete the excess pods
		for i := 0; i < -diff; i++ {
			pod := activePods[len(activePods)-1-i]
			if err := r.Delete(ctx, &pod); err != nil {
				log.Error(err, "failed to delete pod", "pod", pod.Name)
				return ctrl.Result{}, err
			}
		}
	}

	// Update ReplicaSet status
	if rs.Status.Replicas != int32(len(filteredPods)) ||
		rs.Status.ReadyReplicas != r.countReadyPods(filteredPods) ||
		rs.Status.AvailableReplicas != r.countAvailablePods(filteredPods, rs) {

		rs.Status.Replicas = int32(len(filteredPods))
		rs.Status.ReadyReplicas = r.countReadyPods(filteredPods)
		rs.Status.AvailableReplicas = r.countAvailablePods(filteredPods, rs)

		if err := r.Status().Update(ctx, rs); err != nil {
			log.Error(err, "failed to update ReplicaSet status")
			return ctrl.Result{}, err
		}
		log.Info("Updated ReplicaSet status", "name", rs.Name)
	}

	return ctrl.Result{}, nil
}

// createPodFromTemplate creates a new Pod based on the ReplicaSet's pod template
func (r *ReplicaSetReconciler) createPodFromTemplate(rs *appsv1.ReplicaSet) (*corev1.Pod, error) {
	// Generate a unique name for the pod
	podName := fmt.Sprintf("%s-%s", rs.Name, generateRandomSuffix(5))

	outLabels := make(map[string]string)
	for k, v := range rs.Spec.Template.Labels {
		if k == tag.TraceyObjectID {
			continue
		}
		outLabels[k] = v
	}

	if r.KWOKMode {
		pod, err := createDummyPod(podName, rs.Namespace, nil)
		if err != nil {
			return nil, err
		}
		return pod, nil
	}

	// Create a new pod based on the template
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: rs.Namespace,
			Labels:    rs.Spec.Template.Labels,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(rs, appsv1.SchemeGroupVersion.WithKind("ReplicaSet")),
			},
		},
		Spec: rs.Spec.Template.Spec,
	}

	return pod, nil
}

// generateRandomSuffix creates a random string of given length for use in pod names
func generateRandomSuffix(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	result := make([]byte, length)
	for i := range result {
		result[i] = charset[rand.Intn(len(charset))]
	}
	return string(result)
}

// countReadyPods counts the number of pods that are in the Ready state
func (r *ReplicaSetReconciler) countReadyPods(pods []corev1.Pod) int32 {
	var count int32
	for _, pod := range pods {
		if isPodReady(&pod) {
			count++
		}
	}
	return count
}

// countAvailablePods counts the number of pods that have been running and ready
// for at least minReadySeconds
func (r *ReplicaSetReconciler) countAvailablePods(pods []corev1.Pod, rs *appsv1.ReplicaSet) int32 {
	// In a real implementation, you would check how long the pod has been ready
	// For simplicity, we'll just return the number of ready pods
	return r.countReadyPods(pods)
}

// SetupWithManager sets up the controller with the Manager.
func (r *ReplicaSetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1.ReplicaSet{}).
		Owns(&corev1.Pod{}).
		Complete(r)
}
