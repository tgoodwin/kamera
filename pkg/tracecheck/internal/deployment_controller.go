package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/replay"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
)

const (
	podTemplateHashLabel = "pod-template-hash"
)

// deploymentStrategy implements the tracecheck.Strategy interface for the
// Kubernetes Deployment controller.
type deploymentStrategy struct {
	recorder replay.EffectRecorder

	state *deploymentWorldView
}

type deploymentWorldView struct {
	deployments map[types.NamespacedName]*appsv1.Deployment
	replicaSets map[types.NamespacedName]*appsv1.ReplicaSet
}

// NewDeploymentStrategy returns a tracecheck.Strategy that simulates the
// behaviour of the Kubernetes Deployment controller.
func NewDeploymentStrategy(recorder replay.EffectRecorder) tracecheck.Strategy {
	return &deploymentStrategy{
		recorder: recorder,
	}
}

func (d *deploymentStrategy) PrepareState(ctx context.Context, state []runtime.Object) (context.Context, func(), error) {
	world := &deploymentWorldView{
		deployments: make(map[types.NamespacedName]*appsv1.Deployment),
		replicaSets: make(map[types.NamespacedName]*appsv1.ReplicaSet),
	}

	for _, obj := range state {
		switch typed := obj.(type) {
		case *appsv1.Deployment:
			world.deployments[nnKey(typed.Namespace, typed.Name)] = typed.DeepCopy()
		case *appsv1.ReplicaSet:
			world.replicaSets[nnKey(typed.Namespace, typed.Name)] = typed.DeepCopy()
		case *unstructured.Unstructured:
			switch typed.GetKind() {
			case "Deployment":
				dep := &appsv1.Deployment{}
				if err := runtime.DefaultUnstructuredConverter.FromUnstructured(typed.Object, dep); err != nil {
					return ctx, nil, fmt.Errorf("convert deployment: %w", err)
				}
				world.deployments[nnKey(dep.Namespace, dep.Name)] = dep
			case "ReplicaSet":
				rs := &appsv1.ReplicaSet{}
				if err := runtime.DefaultUnstructuredConverter.FromUnstructured(typed.Object, rs); err != nil {
					return ctx, nil, fmt.Errorf("convert replicaset: %w", err)
				}
				world.replicaSets[nnKey(rs.Namespace, rs.Name)] = rs
			}
		default:
			// ignore other objects – Pods are handled by the ReplicaSet strategy
		}
	}

	d.state = world
	cleanup := func() {
		d.state = nil
	}

	return ctx, cleanup, nil
}

func (d *deploymentStrategy) ReconcileAtState(ctx context.Context, name types.NamespacedName) (reconcile.Result, error) {
	if d.state == nil {
		return reconcile.Result{}, fmt.Errorf("deployment strategy state not prepared")
	}

	deployment := d.state.deployments[name]
	if deployment == nil {
		// Nothing to do – object not in view.
		return reconcile.Result{}, nil
	}

	// Deployments default replicas to 1.
	desiredReplicas := int32(1)
	if deployment.Spec.Replicas != nil {
		desiredReplicas = *deployment.Spec.Replicas
	} else {
		deployment.Spec.Replicas = pointerTo(desiredReplicas)
	}

	if !deployment.DeletionTimestamp.IsZero() {
		// Core controller lets garbage collector handle cleanup.
		return reconcile.Result{}, nil
	}

	if deployment.Spec.Paused {
		return reconcile.Result{}, nil
	}

	replicaSets, err := d.replicaSetsForDeployment(deployment)
	if err != nil {
		return reconcile.Result{}, err
	}

	podTemplateHash := computePodTemplateHash(deployment.Spec.Template)

	newRS := findReplicaSetByHash(replicaSets, podTemplateHash)
	var createdRS *appsv1.ReplicaSet

	if newRS == nil {
		createdRS = buildReplicaSetForDeployment(deployment, podTemplateHash)
		tag.AddSleeveObjectID(createdRS)
		tag.LabelChange(createdRS)
		setResourceVersion(createdRS, 1)

		if err := d.recorder.RecordEffect(ctx, createdRS, event.CREATE, nil); err != nil {
			return reconcile.Result{}, fmt.Errorf("record replicaset create: %w", err)
		}

		replicaSets = append(replicaSets, createdRS.DeepCopy())
		newRS = createdRS
	} else if newRS.Spec.Replicas == nil || *newRS.Spec.Replicas != desiredReplicas {
		updated := newRS.DeepCopy()
		updated.Spec.Replicas = pointerTo(desiredReplicas)
		tag.AddSleeveObjectID(updated)
		tag.LabelChange(updated)
		bumpResourceVersion(updated)

		if err := d.recorder.RecordEffect(ctx, updated, event.UPDATE, nil); err != nil {
			return reconcile.Result{}, fmt.Errorf("record replicaset scale: %w", err)
		}

		*newRS = *updated
	}

	// Scale down old ReplicaSets.
	for _, rs := range replicaSets {
		if newRS != nil && rs.Name == newRS.Name {
			continue
		}
		if rs.Spec.Replicas != nil && *rs.Spec.Replicas > 0 {
			updated := rs.DeepCopy()
			updated.Spec.Replicas = pointerTo(int32(0))
			tag.AddSleeveObjectID(updated)
			tag.LabelChange(updated)
			bumpResourceVersion(updated)

			if err := d.recorder.RecordEffect(ctx, updated, event.UPDATE, nil); err != nil {
				return reconcile.Result{}, fmt.Errorf("record replicaset scale down: %w", err)
			}

			*rs = *updated
		}
	}

	// Update deployment status if needed.
	status, err := calculateDeploymentStatus(deployment, replicaSets, podTemplateHash)
	if err != nil {
		return reconcile.Result{}, err
	}

	if !equality.Semantic.DeepEqual(deployment.Status, status) {
		updated := deployment.DeepCopy()
		updated.Status = status
		tag.AddSleeveObjectID(updated)
		tag.LabelChange(updated)
		bumpResourceVersion(updated)

		if err := d.recorder.RecordEffect(ctx, updated, event.UPDATE, nil); err != nil {
			return reconcile.Result{}, fmt.Errorf("record deployment status update: %w", err)
		}
	}

	return reconcile.Result{}, nil
}

func (d *deploymentStrategy) replicaSetsForDeployment(dep *appsv1.Deployment) ([]*appsv1.ReplicaSet, error) {
	if d.state == nil {
		return nil, fmt.Errorf("state not prepared")
	}

	var selector labels.Selector
	var err error
	if dep.Spec.Selector != nil {
		selector, err = metav1.LabelSelectorAsSelector(dep.Spec.Selector)
		if err != nil {
			return nil, fmt.Errorf("build selector: %w", err)
		}
	}

	var result []*appsv1.ReplicaSet
	for nn, rs := range d.state.replicaSets {
		if nn.Namespace != dep.Namespace {
			continue
		}
		// Prioritise owner reference
		if ctrl := metav1.GetControllerOf(rs); ctrl != nil && ctrl.UID == dep.UID {
			result = append(result, rs.DeepCopy())
			continue
		}
		if selector != nil && selector.Matches(labels.Set(rs.Labels)) {
			result = append(result, rs.DeepCopy())
		}
	}
	return result, nil
}

func calculateDeploymentStatus(dep *appsv1.Deployment, rsList []*appsv1.ReplicaSet, templateHash string) (appsv1.DeploymentStatus, error) {
	status := dep.Status.DeepCopy()
	status.ObservedGeneration = dep.Generation

	var replicas, readyReplicas, availableReplicas, updatedReplicas int32
	for _, rs := range rsList {
		replicas += rs.Status.Replicas
		readyReplicas += rs.Status.ReadyReplicas
		availableReplicas += rs.Status.AvailableReplicas

		if rs.Labels[podTemplateHashLabel] == templateHash {
			updatedReplicas += rs.Status.Replicas
		}
	}

	status.Replicas = replicas
	status.ReadyReplicas = readyReplicas
	status.AvailableReplicas = availableReplicas
	status.UpdatedReplicas = updatedReplicas

	status.Conditions = []appsv1.DeploymentCondition{
		{
			Type:               appsv1.DeploymentProgressing,
			LastUpdateTime:     metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Status: func() corev1.ConditionStatus {
				if updatedReplicas == desiredOrOne(dep.Spec.Replicas) &&
					replicas == desiredOrOne(dep.Spec.Replicas) &&
					availableReplicas >= minAvailable(dep) {
					return corev1.ConditionTrue
				}
				return corev1.ConditionFalse
			}(),
			Reason: func() string {
				if updatedReplicas == desiredOrOne(dep.Spec.Replicas) &&
					replicas == desiredOrOne(dep.Spec.Replicas) &&
					availableReplicas >= minAvailable(dep) {
					return "NewReplicaSetAvailable"
				}
				return "ProgressDeadlineExceeded"
			}(),
			Message: func() string {
				if updatedReplicas == desiredOrOne(dep.Spec.Replicas) &&
					replicas == desiredOrOne(dep.Spec.Replicas) &&
					availableReplicas >= minAvailable(dep) {
					return "Deployment has minimum availability"
				}
				return "Deployment is progressing"
			}(),
		},
		{
			Type:               appsv1.DeploymentAvailable,
			LastUpdateTime:     metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Status: func() corev1.ConditionStatus {
				if availableReplicas >= minAvailable(dep) {
					return corev1.ConditionTrue
				}
				return corev1.ConditionFalse
			}(),
			Reason: func() string {
				if availableReplicas >= minAvailable(dep) {
					return "MinimumReplicasAvailable"
				}
				return "MinimumReplicasUnavailable"
			}(),
			Message: func() string {
				if availableReplicas >= minAvailable(dep) {
					return "Deployment has minimum availability"
				}
				return "Deployment does not have minimum availability"
			}(),
		},
	}

	return *status, nil
}

func minAvailable(dep *appsv1.Deployment) int32 {
	if dep.Spec.Strategy.RollingUpdate != nil && dep.Spec.Strategy.RollingUpdate.MaxUnavailable != nil {
		maxUnavailable := resolveIntOrPercent(*dep.Spec.Strategy.RollingUpdate.MaxUnavailable, desiredOrOne(dep.Spec.Replicas))
		return desiredOrOne(dep.Spec.Replicas) - maxUnavailable
	}
	return desiredOrOne(dep.Spec.Replicas)
}

func resolveIntOrPercent(val intstr.IntOrString, total int32) int32 {
	if val.Type == intstr.String {
		percentage, err := intstr.GetValueFromIntOrPercent(&val, int(total), true)
		if err != nil {
			return 0
		}
		return int32(percentage)
	}
	return int32(val.IntValue())
}

func desiredOrOne(ptr *int32) int32 {
	if ptr == nil {
		return 1
	}
	return *ptr
}

func findReplicaSetByHash(rss []*appsv1.ReplicaSet, hash string) *appsv1.ReplicaSet {
	for _, rs := range rss {
		if rs.Labels[podTemplateHashLabel] == hash {
			return rs
		}
	}
	return nil
}

func buildReplicaSetForDeployment(dep *appsv1.Deployment, hash string) *appsv1.ReplicaSet {
	labelsCopy := make(map[string]string)
	if dep.Spec.Selector != nil {
		for k, v := range dep.Spec.Selector.MatchLabels {
			labelsCopy[k] = v
		}
	}
	labelsCopy[podTemplateHashLabel] = hash

	template := dep.Spec.Template.DeepCopy()
	if template.Labels == nil {
		template.Labels = make(map[string]string)
	}
	template.Labels[podTemplateHashLabel] = hash

	var selector *metav1.LabelSelector
	if dep.Spec.Selector != nil {
		selector = dep.Spec.Selector.DeepCopy()
	}

	rs := &appsv1.ReplicaSet{
		TypeMeta: metav1.TypeMeta{
			APIVersion: appsv1.SchemeGroupVersion.String(),
			Kind:       "ReplicaSet",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%s", dep.Name, hash),
			Namespace: dep.Namespace,
			Labels:    labelsCopy,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(dep, appsv1.SchemeGroupVersion.WithKind("Deployment")),
			},
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: dep.Spec.Replicas,
			Selector: selector,
			Template: *template,
		},
	}
	return rs
}

func computePodTemplateHash(template corev1.PodTemplateSpec) string {
	data, _ := json.Marshal(template.Spec)
	hasher := fnv.New32a()
	_, _ = hasher.Write(data)
	return fmt.Sprintf("%d", hasher.Sum32())
}

func nnKey(ns, name string) types.NamespacedName {
	return types.NamespacedName{
		Namespace: ns,
		Name:      name,
	}
}

func pointerTo(val int32) *int32 {
	out := val
	return &out
}

func bumpResourceVersion(obj client.Object) {
	if obj == nil {
		return
	}
	current := obj.GetResourceVersion()
	if current == "" {
		obj.SetResourceVersion("1")
		return
	}
	val, err := strconv.Atoi(current)
	if err != nil {
		obj.SetResourceVersion(current + "-1")
		return
	}
	obj.SetResourceVersion(strconv.Itoa(val + 1))
}

func setResourceVersion(obj client.Object, value int64) {
	if obj == nil {
		return
	}
	obj.SetResourceVersion(strconv.FormatInt(value, 10))
}
