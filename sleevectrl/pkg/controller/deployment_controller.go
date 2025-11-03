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
	"hash/fnv"
	"sort"

	"github.com/davecgh/go-spew/spew"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
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
	logger := log.FromContext(ctx)

	deployment := &appsv1.Deployment{}
	deployment.SetGroupVersionKind(appsv1.SchemeGroupVersion.WithKind("Deployment"))
	if err := r.Get(ctx, req.NamespacedName, deployment); err != nil {
		if client.IgnoreNotFound(err) != nil {
			logger.Error(err, "unable to fetch Deployment")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	if !deployment.DeletionTimestamp.IsZero() {
		logger.Info("Deployment is being deleted", "name", deployment.Name)
		return ctrl.Result{}, nil
	}

	if deployment.Spec.Selector == nil {
		logger.Error(fmt.Errorf("nil selector"), "deployment selector must be specified")
		return ctrl.Result{}, nil
	}

	selector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
	if err != nil {
		logger.Error(err, "unable to convert selector")
		return ctrl.Result{}, err
	}

	rsList := &appsv1.ReplicaSetList{}
	if err := r.List(ctx, rsList,
		client.InNamespace(deployment.Namespace),
		client.MatchingLabelsSelector{Selector: selector}); err != nil {
		logger.Error(err, "unable to list ReplicaSets")
		return ctrl.Result{}, err
	}

	var ownedReplicaSets []*appsv1.ReplicaSet
	for i := range rsList.Items {
		rs := &rsList.Items[i]

		// Attempt to adopt orphaned ReplicaSets that match the selector.
		if !metav1.IsControlledBy(rs, deployment) && rs.DeletionTimestamp.IsZero() {
			if len(rs.OwnerReferences) == 0 {
				if err := controllerutil.SetControllerReference(deployment, rs, r.Scheme); err != nil {
					logger.Error(err, "failed to set controller reference", "replicaSet", rs.Name)
					continue
				}
				if err := r.Update(ctx, rs); err != nil {
					logger.Error(err, "failed to adopt ReplicaSet", "replicaSet", rs.Name)
					continue
				}
			} else {
				// ReplicaSet is controlled by another owner. Skip it.
				continue
			}
		}

		if metav1.IsControlledBy(rs, deployment) {
			ownedReplicaSets = append(ownedReplicaSets, rs)
		}
	}

	newRS, err := r.getOrCreateReplicaSet(ctx, deployment, ownedReplicaSets)
	if err != nil {
		logger.Error(err, "failed to get or create ReplicaSet")
		return ctrl.Result{}, err
	}

	seen := false
	for _, rs := range ownedReplicaSets {
		if rs.UID == newRS.UID {
			seen = true
			break
		}
	}
	if !seen {
		ownedReplicaSets = append(ownedReplicaSets, newRS)
	}

	desiredReplicas := int32(1)
	if deployment.Spec.Replicas != nil {
		desiredReplicas = *deployment.Spec.Replicas
	}

	var (
		oldReplicaSets []*appsv1.ReplicaSet
	)
	for _, rs := range ownedReplicaSets {
		if rs.UID != newRS.UID {
			oldReplicaSets = append(oldReplicaSets, rs)
		}
	}

	strategyType := deployment.Spec.Strategy.Type
	if strategyType == "" {
		strategyType = appsv1.RollingUpdateDeploymentStrategyType
	}

	var maxUnavailable int32

	switch strategyType {
	case appsv1.RecreateDeploymentStrategyType:
		for _, rs := range oldReplicaSets {
			if err := r.scaleReplicaSet(ctx, rs, 0); err != nil {
				logger.Error(err, "failed to scale down old ReplicaSet", "replicaSet", rs.Name)
				return ctrl.Result{}, err
			}
		}
		if err := r.scaleReplicaSet(ctx, newRS, desiredReplicas); err != nil {
			logger.Error(err, "failed to scale new ReplicaSet", "replicaSet", newRS.Name)
			return ctrl.Result{}, err
		}
		maxUnavailable = desiredReplicas
	default:
		maxSurge, resolvedMaxUnavailable, err := resolveRollingUpdateParams(deployment, desiredReplicas)
		if err != nil {
			logger.Error(err, "failed to resolve rolling update parameters")
			return ctrl.Result{}, err
		}
		maxUnavailable = resolvedMaxUnavailable
		if err := r.rolloutReplicaSets(ctx, deployment, newRS, oldReplicaSets, desiredReplicas, maxSurge, resolvedMaxUnavailable); err != nil {
			return ctrl.Result{}, err
		}
	}

	status, err := r.calculateStatus(deployment, ownedReplicaSets, desiredReplicas, maxUnavailable)
	if err != nil {
		logger.Error(err, "failed to calculate deployment status")
		return ctrl.Result{}, err
	}

	if !equality.Semantic.DeepEqual(deployment.Status, status) {
		deployment.Status = status
		if err := r.Status().Update(ctx, deployment); err != nil {
			logger.Error(err, "failed to update Deployment status")
			return ctrl.Result{}, err
		}
		logger.Info("Updated Deployment status", "name", deployment.Name)
	}

	return ctrl.Result{}, nil
}

func (r *DeploymentReconciler) rolloutReplicaSets(
	ctx context.Context,
	deployment *appsv1.Deployment,
	newRS *appsv1.ReplicaSet,
	oldReplicaSets []*appsv1.ReplicaSet,
	desiredReplicas, maxSurge, maxUnavailable int32,
) error {
	logger := log.FromContext(ctx)

	maxTotal := desiredReplicas + maxSurge
	minAvailable := desiredReplicas - maxUnavailable
	if minAvailable < 0 {
		minAvailable = 0
	}

	allSets := append([]*appsv1.ReplicaSet{newRS}, oldReplicaSets...)
	currentTotal := totalSpecReplicas(allSets)
	newSpecReplicas := replicasOrZero(newRS.Spec.Replicas)

	if currentTotal < maxTotal {
		scaleUp := maxTotal - currentTotal
		target := newSpecReplicas + scaleUp
		if target > desiredReplicas {
			target = desiredReplicas
		}
		if target > newSpecReplicas {
			if err := r.scaleReplicaSet(ctx, newRS, target); err != nil {
				logger.Error(err, "failed to scale up new ReplicaSet", "replicaSet", newRS.Name)
				return err
			}
			currentTotal = currentTotal - newSpecReplicas + target
			newSpecReplicas = target
		}
	}

	totalReady := newRS.Status.ReadyReplicas
	for _, rs := range oldReplicaSets {
		totalReady += rs.Status.ReadyReplicas
	}

	excessReady := totalReady - minAvailable
	if excessReady < 0 {
		excessReady = 0
	}

	sort.SliceStable(oldReplicaSets, func(i, j int) bool {
		return oldReplicaSets[i].CreationTimestamp.Time.After(oldReplicaSets[j].CreationTimestamp.Time)
	})

	for _, rs := range oldReplicaSets {
		if replicasOrZero(rs.Spec.Replicas) == 0 {
			continue
		}
		if excessReady <= 0 && currentTotal <= maxTotal {
			break
		}

		curr := replicasOrZero(rs.Spec.Replicas)
		reduce := curr
		if excessReady > 0 && reduce > excessReady {
			reduce = excessReady
		}
		if currentTotal > maxTotal {
			over := currentTotal - maxTotal
			if reduce > over {
				reduce = over
			}
		}
		if reduce <= 0 {
			continue
		}

		target := curr - reduce
		if err := r.scaleReplicaSet(ctx, rs, target); err != nil {
			logger.Error(err, "failed to scale down old ReplicaSet", "replicaSet", rs.Name)
			return err
		}
		currentTotal -= (curr - target)
		if excessReady > 0 {
			delta := curr - target
			if delta > excessReady {
				delta = excessReady
			}
			excessReady -= delta
		}
	}

	return nil
}

func (r *DeploymentReconciler) scaleReplicaSet(ctx context.Context, rs *appsv1.ReplicaSet, replicas int32) error {
	if replicas < 0 {
		replicas = 0
	}
	current := replicasOrZero(rs.Spec.Replicas)
	if current == replicas {
		return nil
	}

	rs.Spec.Replicas = ptr.To(replicas)
	return r.Update(ctx, rs)
}

func replicasOrZero(replicas *int32) int32 {
	if replicas == nil {
		return 0
	}
	return *replicas
}

func totalSpecReplicas(replicaSets []*appsv1.ReplicaSet) int32 {
	var total int32
	for _, rs := range replicaSets {
		total += replicasOrZero(rs.Spec.Replicas)
	}
	return total
}

func resolveRollingUpdateParams(deployment *appsv1.Deployment, desired int32) (int32, int32, error) {
	rolling := deployment.Spec.Strategy.RollingUpdate
	if rolling == nil {
		rolling = &appsv1.RollingUpdateDeployment{}
	}

	defaultMaxUnavailable := intstr.FromString("25%")
	defaultMaxSurge := intstr.FromString("25%")

	maxUnavailableSource := rolling.MaxUnavailable
	if maxUnavailableSource == nil {
		maxUnavailableSource = &defaultMaxUnavailable
	}
	maxSurgeSource := rolling.MaxSurge
	if maxSurgeSource == nil {
		maxSurgeSource = &defaultMaxSurge
	}

	maxUnavailable, err := intstr.GetValueFromIntOrPercent(maxUnavailableSource, int(desired), false)
	if err != nil {
		return 0, 0, err
	}
	maxSurge, err := intstr.GetValueFromIntOrPercent(maxSurgeSource, int(desired), true)
	if err != nil {
		return 0, 0, err
	}

	if desired == 0 {
		return int32(maxSurge), int32(maxUnavailable), nil
	}
	if maxUnavailable == 0 && maxSurge == 0 {
		maxUnavailable = 1
	}

	return int32(maxSurge), int32(maxUnavailable), nil
}

// getOrCreateReplicaSet returns the appropriate ReplicaSet for the Deployment,
// creating a new one if necessary
func (r *DeploymentReconciler) getOrCreateReplicaSet(
	ctx context.Context,
	deployment *appsv1.Deployment,
	existingRSs []*appsv1.ReplicaSet) (*appsv1.ReplicaSet, error) {

	// Work on deep copies so we never mutate the Deployment's spec during reconciliation.
	templateCopy := deployment.Spec.Template.DeepCopy()

	var selectorCopy *metav1.LabelSelector
	if deployment.Spec.Selector != nil {
		selectorCopy = deployment.Spec.Selector.DeepCopy()
	}

	// Use a version of the template without any existing pod-template-hash label when hashing.
	templateForHash := templateCopy.DeepCopy()
	if templateForHash.Labels != nil {
		delete(templateForHash.Labels, "pod-template-hash")
	}

	// Generate a hash for the deployment template
	podTemplateHash := computeTemplateHash(*templateForHash)

	// Check if we already have a ReplicaSet with this hash
	for i := range existingRSs {
		rs := existingRSs[i]
		if rs.Labels["pod-template-hash"] == podTemplateHash {
			return rs, nil
		}
	}

	// Create a new ReplicaSet
	// Ensure metadata and pod template labels are isolated copies before we add hash label.
	var rsLabels map[string]string
	if deployment.Spec.Selector != nil {
		rsLabels = cloneStringMap(deployment.Spec.Selector.MatchLabels)
	}
	if rsLabels == nil {
		rsLabels = make(map[string]string)
	}
	rsLabels["pod-template-hash"] = podTemplateHash

	podLabels := cloneStringMap(templateCopy.Labels)
	if podLabels == nil {
		podLabels = make(map[string]string)
	}
	podLabels["pod-template-hash"] = podTemplateHash
	templateCopy.Labels = podLabels

	newRS := &appsv1.ReplicaSet{
		TypeMeta: metav1.TypeMeta{
			APIVersion: appsv1.SchemeGroupVersion.String(),
			Kind:       "ReplicaSet",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%s", deployment.Name, podTemplateHash),
			Namespace: deployment.Namespace,
			Labels:    rsLabels,
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: deployment.Spec.Replicas,
			Selector: selectorCopy,
			Template: *templateCopy,
		},
	}

	if err := controllerutil.SetControllerReference(deployment, newRS, r.Scheme); err != nil {
		return nil, err
	}

	// Create the ReplicaSet
	if err := r.Create(ctx, newRS); err != nil {
		return nil, err
	}

	return newRS, nil
}

// computeTemplateHash generates a deterministic hash for a pod template
func computeTemplateHash(template corev1.PodTemplateSpec) string {
	hasher := fnv.New32a()
	hashState.Fprintf(hasher, "%#v", template)
	return fmt.Sprintf("%d", hasher.Sum32())
}

var hashState = &spew.ConfigState{
	Indent:         "",
	SortKeys:       true,
	DisableMethods: true,
	SpewKeys:       true,
}

func cloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}

	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func newReplicaSetName(hash string, deployment *appsv1.Deployment) string {
	return fmt.Sprintf("%s-%s", deployment.Name, hash)
}

// calculateStatus calculates the deployment status based on associated ReplicaSets
func (r *DeploymentReconciler) calculateStatus(
	deployment *appsv1.Deployment,
	replicaSets []*appsv1.ReplicaSet,
	desiredReplicas, maxUnavailable int32) (appsv1.DeploymentStatus, error) {

	status := deployment.Status.DeepCopy()
	status.ObservedGeneration = deployment.Generation

	templateCopy := deployment.Spec.Template.DeepCopy()
	if templateCopy.Labels != nil {
		delete(templateCopy.Labels, "pod-template-hash")
	}
	templateHash := computeTemplateHash(*templateCopy)

	var replicas, readyReplicas, availableReplicas, updatedReplicas int32
	for _, rs := range replicaSets {
		replicas += rs.Status.Replicas
		readyReplicas += rs.Status.ReadyReplicas
		availableReplicas += rs.Status.AvailableReplicas
		if rs.Labels["pod-template-hash"] == templateHash {
			updatedReplicas += rs.Status.Replicas
		}
	}

	status.Replicas = replicas
	status.ReadyReplicas = readyReplicas
	status.AvailableReplicas = availableReplicas
	status.UpdatedReplicas = updatedReplicas

	if desiredReplicas >= availableReplicas {
		status.UnavailableReplicas = desiredReplicas - availableReplicas
	} else {
		status.UnavailableReplicas = 0
	}

	progressingCondition := appsv1.DeploymentCondition{
		Type:   appsv1.DeploymentProgressing,
		Status: corev1.ConditionTrue,
		Reason: "ReplicaSetUpdated",
		Message: fmt.Sprintf("ReplicaSet %s is reconciling new Pods",
			newReplicaSetName(templateHash, deployment)),
	}
	if updatedReplicas == desiredReplicas && availableReplicas == desiredReplicas {
		progressingCondition.Reason = "NewReplicaSetAvailable"
		progressingCondition.Message = "Deployment has completed rolling out new Pods"
	}

	minAvailable := desiredReplicas - maxUnavailable
	if minAvailable < 0 {
		minAvailable = 0
	}

	availableCondition := appsv1.DeploymentCondition{
		Type: appsv1.DeploymentAvailable,
	}
	if availableReplicas >= minAvailable {
		availableCondition.Status = corev1.ConditionTrue
		availableCondition.Reason = "MinimumReplicasAvailable"
		availableCondition.Message = "Deployment has minimum availability"
	} else {
		availableCondition.Status = corev1.ConditionFalse
		availableCondition.Reason = "MinimumReplicasUnavailable"
		availableCondition.Message = "Deployment does not have minimum availability"
	}

	status.Conditions = mergeDeploymentConditions(
		deployment.Status.Conditions,
		[]appsv1.DeploymentCondition{progressingCondition, availableCondition},
	)

	return *status, nil
}

func mergeDeploymentConditions(existing, desired []appsv1.DeploymentCondition) []appsv1.DeploymentCondition {
	now := metav1.Now()
	existingByType := make(map[appsv1.DeploymentConditionType]appsv1.DeploymentCondition, len(existing))
	for _, cond := range existing {
		existingByType[cond.Type] = cond
	}

	result := make([]appsv1.DeploymentCondition, len(desired))
	for i, cond := range desired {
		if prev, ok := existingByType[cond.Type]; ok {
			cond.LastTransitionTime = prev.LastTransitionTime
			cond.LastUpdateTime = prev.LastUpdateTime

			if cond.Status != prev.Status {
				cond.LastTransitionTime = now
				cond.LastUpdateTime = now
			} else if cond.Reason != prev.Reason || cond.Message != prev.Message {
				cond.LastUpdateTime = now
			}
		} else {
			cond.LastTransitionTime = now
			cond.LastUpdateTime = now
		}
		result[i] = cond
	}

	return result
}

// SetupWithManager sets up the controller with the Manager.
func (r *DeploymentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1.Deployment{}).
		Owns(&appsv1.ReplicaSet{}).
		Complete(r)
}
