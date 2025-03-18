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
	"encoding/json"
	"fmt"
	"hash/fnv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// DeploymentReconciler reconciles a Deployment object
type DeploymentReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=deployments/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=replicasets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Deployment object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.18.4/pkg/reconcile
func (r *DeploymentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// Fetch the Deployment instance
	deployment := &appsv1.Deployment{}
	if err := r.Get(ctx, req.NamespacedName, deployment); err != nil {
		if client.IgnoreNotFound(err) != nil {
			log.Error(err, "unable to fetch Deployment")
			return ctrl.Result{}, err
		}
		// Deployment not found, likely deleted, return
		return ctrl.Result{}, nil
	}

	// Check if the Deployment is being deleted
	if !deployment.DeletionTimestamp.IsZero() {
		log.Info("Deployment is being deleted", "name", deployment.Name)
		return ctrl.Result{}, nil
	}

	// List existing ReplicaSets for this Deployment
	rsList := &appsv1.ReplicaSetList{}
	if err := r.List(ctx, rsList,
		client.InNamespace(deployment.Namespace),
		client.MatchingLabels(deployment.Spec.Selector.MatchLabels)); err != nil {
		log.Error(err, "unable to list ReplicaSets")
		return ctrl.Result{}, err
	}

	// Find or create ReplicaSet based on Deployment template hash
	newRS, err := r.getOrCreateReplicaSet(ctx, deployment, rsList.Items)
	if err != nil {
		log.Error(err, "failed to get or create ReplicaSet")
		return ctrl.Result{}, err
	}

	// Scale up new ReplicaSet
	if newRS.Spec.Replicas == nil || *newRS.Spec.Replicas != *deployment.Spec.Replicas {
		newRS.Spec.Replicas = deployment.Spec.Replicas
		if err := r.Update(ctx, newRS); err != nil {
			log.Error(err, "failed to scale ReplicaSet", "replicaSet", newRS.Name)
			return ctrl.Result{}, err
		}
		log.Info("Scaled ReplicaSet", "replicaSet", newRS.Name, "replicas", *newRS.Spec.Replicas)
	}

	// Scale down old ReplicaSets
	for _, rs := range rsList.Items {
		// Skip the new ReplicaSet
		if rs.Name == newRS.Name {
			continue
		}

		// Scale down old ReplicaSet
		if rs.Spec.Replicas != nil && *rs.Spec.Replicas > 0 {
			zero := int32(0)
			rs.Spec.Replicas = &zero
			if err := r.Update(ctx, &rs); err != nil {
				log.Error(err, "failed to scale down old ReplicaSet", "replicaSet", rs.Name)
				return ctrl.Result{}, err
			}
			log.Info("Scaled down old ReplicaSet", "replicaSet", rs.Name)
		}
	}

	// Update deployment status
	status, err := r.calculateStatus(deployment, rsList.Items)
	if err != nil {
		log.Error(err, "failed to calculate deployment status")
		return ctrl.Result{}, err
	}

	if !equality.Semantic.DeepEqual(deployment.Status, status) {
		deployment.Status = status
		if err := r.Status().Update(ctx, deployment); err != nil {
			log.Error(err, "failed to update Deployment status")
			return ctrl.Result{}, err
		}
		log.Info("Updated Deployment status", "name", deployment.Name)
	}

	return ctrl.Result{}, nil
}

// getOrCreateReplicaSet returns the appropriate ReplicaSet for the Deployment,
// creating a new one if necessary
func (r *DeploymentReconciler) getOrCreateReplicaSet(
	ctx context.Context,
	deployment *appsv1.Deployment,
	existingRSs []appsv1.ReplicaSet) (*appsv1.ReplicaSet, error) {

	// Generate a hash for the deployment template
	podTemplateHash := computeHash(deployment.Spec.Template.Spec)

	// Check if we already have a ReplicaSet with this hash
	for i := range existingRSs {
		rs := &existingRSs[i]
		if rs.Labels["pod-template-hash"] == podTemplateHash {
			return rs, nil
		}
	}

	// Create a new ReplicaSet
	newRS := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: deployment.Name + "-",
			Namespace:    deployment.Namespace,
			Labels:       deployment.Spec.Selector.MatchLabels,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(deployment, appsv1.SchemeGroupVersion.WithKind("Deployment")),
			},
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: deployment.Spec.Replicas,
			Selector: deployment.Spec.Selector,
			Template: deployment.Spec.Template,
		},
	}

	// Add the pod-template-hash label
	if newRS.Labels == nil {
		newRS.Labels = make(map[string]string)
	}
	newRS.Labels["pod-template-hash"] = podTemplateHash

	// Also add this to the pod template labels
	if newRS.Spec.Template.Labels == nil {
		newRS.Spec.Template.Labels = make(map[string]string)
	}
	newRS.Spec.Template.Labels["pod-template-hash"] = podTemplateHash

	// Create the ReplicaSet
	if err := r.Create(ctx, newRS); err != nil {
		return nil, err
	}

	return newRS, nil
}

// computeHash generates a hash string for a pod spec
func computeHash(spec corev1.PodSpec) string {
	// In a real implementation, this would use a more robust hashing algorithm
	// Here, we're just using a simplified approach for demo purposes
	podSpecJSON, _ := json.Marshal(spec)
	h := fnv.New32a()
	h.Write(podSpecJSON)
	return fmt.Sprintf("%d", h.Sum32())
}

// calculateStatus calculates the deployment status based on associated ReplicaSets
func (r *DeploymentReconciler) calculateStatus(
	deployment *appsv1.Deployment,
	rsList []appsv1.ReplicaSet) (appsv1.DeploymentStatus, error) {

	status := deployment.Status.DeepCopy()

	// Get total replicas, ready replicas, available replicas, etc.
	var replicas, readyReplicas, availableReplicas, updatedReplicas int32

	for _, rs := range rsList {
		replicas += rs.Status.Replicas
		readyReplicas += rs.Status.ReadyReplicas
		availableReplicas += rs.Status.AvailableReplicas

		// Consider a ReplicaSet "updated" if it matches the current pod template hash
		if rs.Labels["pod-template-hash"] == computeHash(deployment.Spec.Template.Spec) {
			updatedReplicas += rs.Status.Replicas
		}
	}

	status.Replicas = replicas
	status.ReadyReplicas = readyReplicas
	status.AvailableReplicas = availableReplicas
	status.UpdatedReplicas = updatedReplicas

	// Determine conditions based on replicas counts
	status.Conditions = []appsv1.DeploymentCondition{}

	// Progressing condition
	progressingCondition := appsv1.DeploymentCondition{
		Type:               appsv1.DeploymentProgressing,
		LastUpdateTime:     metav1.Now(),
		LastTransitionTime: metav1.Now(),
	}

	if updatedReplicas == *deployment.Spec.Replicas &&
		replicas == *deployment.Spec.Replicas &&
		availableReplicas == *deployment.Spec.Replicas {
		progressingCondition.Status = corev1.ConditionTrue
		progressingCondition.Reason = "NewReplicaSetAvailable"
		progressingCondition.Message = "Deployment has minimum availability"
	} else {
		progressingCondition.Status = corev1.ConditionFalse
		progressingCondition.Reason = "ProgressDeadlineExceeded"
		progressingCondition.Message = "Deployment is progressing"
	}
	status.Conditions = append(status.Conditions, progressingCondition)

	// Available condition
	availableCondition := appsv1.DeploymentCondition{
		Type:               appsv1.DeploymentAvailable,
		LastUpdateTime:     metav1.Now(),
		LastTransitionTime: metav1.Now(),
	}

	if availableReplicas >= deployment.Spec.MinReadySeconds {
		availableCondition.Status = corev1.ConditionTrue
		availableCondition.Reason = "MinimumReplicasAvailable"
		availableCondition.Message = "Deployment has minimum availability"
	} else {
		availableCondition.Status = corev1.ConditionFalse
		availableCondition.Reason = "MinimumReplicasUnavailable"
		availableCondition.Message = "Deployment does not have minimum availability"
	}
	status.Conditions = append(status.Conditions, availableCondition)

	return *status, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *DeploymentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1.Deployment{}).
		Owns(&appsv1.ReplicaSet{}).
		Complete(r)
}
