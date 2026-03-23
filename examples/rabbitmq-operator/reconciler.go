// Adapted RabbitmqClusterReconciler for Kamera harness.
// Original source: github.com/rabbitmq/cluster-operator/controllers (commit 4f13b9a)
//
// Adaptations:
// - PodExecutor stubbed as no-op (CLI commands to rabbitmq pods)
// - EventRecorder stubbed as no-op
// - Clientset/ClusterConfig removed (only needed for pod exec)
// - All imports repointed to local rmq package
// - retryWithInterval sleep removed (deterministic simulation)
// - context.Context passed through (required by Kamera replay client)

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/go-logr/logr"

	rmq "github.com/tgoodwin/kamera/examples/rabbitmq-operator/rmq"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	k8sresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientretry "k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var _ reconcile.Reconciler = &RabbitmqClusterReconciler{}

// Resource name constants (from internal/resource)
const (
	PluginsConfigName     = "plugins-conf"
	ServerConfigMapName   = "server-conf"
	DefaultUserSecretName = "default-user"
	ServiceSuffix         = ""
	DeletionMarker        = "skipPreStopChecks"
)

// stableTimeStr is a fixed timestamp for annotations to ensure deterministic simulation.
const stableTimeStr = "2025-01-01T00:00:00Z"

// Annotation constants
const (
	pluginsUpdateAnnotation  = "rabbitmq.com/pluginsUpdatedAt"
	serverConfAnnotation     = "rabbitmq.com/serverConfUpdatedAt"
	stsRestartAnnotation     = "rabbitmq.com/lastRestartAt"
	stsCreateAnnotation      = "rabbitmq.com/createdAt"
	queueRebalanceAnnotation = "rabbitmq.com/queueRebalanceNeededAt"
	deletionFinalizer        = "deletion.finalizers.rabbitmqclusters.rabbitmq.com"
)

// noopEventRecorder stubs out the event recorder for simulation.
type noopEventRecorder struct{}

func (n *noopEventRecorder) Event(object runtime.Object, eventtype, reason, message string)                {}
func (n *noopEventRecorder) Eventf(object runtime.Object, eventtype, reason, messageFmt string, args ...interface{}) {}
func (n *noopEventRecorder) AnnotatedEventf(object runtime.Object, annotations map[string]string, eventtype, reason, messageFmt string, args ...interface{}) {}

// RabbitmqClusterReconciler reconciles a RabbitmqCluster object.
type RabbitmqClusterReconciler struct {
	client.Client
	ctx      context.Context
	Scheme   *runtime.Scheme
	Recorder *noopEventRecorder
}

func (r *RabbitmqClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	r.ctx = ctx
	logger := ctrl.LoggerFrom(ctx)

	rabbitmqCluster, err := r.getRabbitmqCluster(ctx, req.NamespacedName)
	if client.IgnoreNotFound(err) != nil {
		return ctrl.Result{}, err
	} else if k8serrors.IsNotFound(err) {
		return ctrl.Result{}, nil
	}

	// Check if the resource has been marked for deletion
	if !rabbitmqCluster.ObjectMeta.DeletionTimestamp.IsZero() {
		logger.Info("Deleting")
		return ctrl.Result{}, r.prepareForDeletion(ctx, rabbitmqCluster)
	}

	// Ensure the resource has a deletion finalizer
	if err := r.addFinalizerIfNeeded(ctx, rabbitmqCluster); err != nil {
		return ctrl.Result{}, err
	}

	// Skip TLS reconciliation in simulation (no secrets to validate)

	if requeueAfter, err := r.updateStatus(ctx, rabbitmqCluster); err != nil || requeueAfter > 0 {
		return ctrl.Result{RequeueAfter: requeueAfter}, err
	}

	sts, err := r.statefulSet(ctx, rabbitmqCluster)
	if client.IgnoreNotFound(err) != nil {
		return ctrl.Result{}, err
	}
	if sts != nil && statefulSetNeedsQueueRebalance(sts, rabbitmqCluster) {
		if err := r.markForQueueRebalance(ctx, rabbitmqCluster); err != nil {
			return ctrl.Result{}, err
		}
	}

	instanceSpec, err := json.Marshal(rabbitmqCluster.Spec)
	if err != nil {
		logger.Error(err, "Failed to marshal cluster spec")
	}

	logger.Info("Start reconciling", "spec", string(instanceSpec))

	// Build and reconcile child resources
	builders := r.resourceBuilders(rabbitmqCluster)

	for _, builder := range builders {
		resource := builder.Build()

		if builder.UpdateMayRequireStsRecreate {
			sts := resource.(*appsv1.StatefulSet)

			current, err := r.statefulSet(ctx, rabbitmqCluster)
			if client.IgnoreNotFound(err) != nil {
				return ctrl.Result{}, err
			}

			if !k8serrors.IsNotFound(err) {
				builder.Update(sts)
				if err = r.reconcilePVC(ctx, rabbitmqCluster, current, sts); err != nil {
					rabbitmqCluster.Status.SetCondition(rmq.ReconcileSuccess, corev1.ConditionFalse, "FailedReconcilePVC", err.Error())
					r.Status().Update(ctx, rabbitmqCluster)
					return ctrl.Result{}, err
				}
				if r.scaleDown(ctx, rabbitmqCluster, current, sts) {
					return ctrl.Result{}, nil
				}
			}
		}

		var operationResult controllerutil.OperationResult
		err = clientretry.RetryOnConflict(clientretry.DefaultRetry, func() error {
			var apiError error
			operationResult, apiError = controllerutil.CreateOrUpdate(ctx, r.Client, resource, func() error {
				return builder.Update(resource)
			})
			return apiError
		})
		r.logAndRecordOperationResult(logger, rabbitmqCluster, resource, operationResult, err)
		if err != nil {
			rabbitmqCluster.Status.SetCondition(rmq.ReconcileSuccess, corev1.ConditionFalse, "Error", err.Error())
			r.Status().Update(ctx, rabbitmqCluster)
			return ctrl.Result{}, err
		}

		if err = r.annotateIfNeeded(ctx, logger, builder, operationResult, rabbitmqCluster); err != nil {
			return ctrl.Result{}, err
		}
	}

	if requeueAfter, err := r.restartStatefulSetIfNeeded(ctx, logger, rabbitmqCluster); err != nil || requeueAfter > 0 {
		return ctrl.Result{RequeueAfter: requeueAfter}, err
	}

	if err := r.setDefaultUserStatus(ctx, rabbitmqCluster); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.setBinding(ctx, rabbitmqCluster); err != nil {
		return ctrl.Result{}, err
	}

	// Skip CLI commands in simulation (no real pods to exec into)
	// In real operator, this runs rabbitmq-plugins set, enable_feature_flag, rebalance all
	// These are no-ops in our simulation since we're testing K8s resource management

	// Set ReconcileSuccess (only write if not already true to avoid infinite re-reconciliation)
	needsUpdate := true
	for _, c := range rabbitmqCluster.Status.Conditions {
		if c.Type == rmq.ReconcileSuccess && c.Status == corev1.ConditionTrue {
			needsUpdate = false
			break
		}
	}
	if needsUpdate {
		rabbitmqCluster.Status.SetCondition(rmq.ReconcileSuccess, corev1.ConditionTrue, "Success", "Finish reconciling")
		if writerErr := r.Status().Update(ctx, rabbitmqCluster); writerErr != nil {
			logger.Error(writerErr, "Failed to Update Custom Resource status")
		}
	}

	logger.Info("Finished reconciling")
	return ctrl.Result{}, nil
}

// --- Finalizer handling ---

func (r *RabbitmqClusterReconciler) addFinalizerIfNeeded(ctx context.Context, rabbitmqCluster *rmq.RabbitmqCluster) error {
	if rabbitmqCluster.ObjectMeta.DeletionTimestamp.IsZero() && !controllerutil.ContainsFinalizer(rabbitmqCluster, deletionFinalizer) {
		controllerutil.AddFinalizer(rabbitmqCluster, deletionFinalizer)
		if err := r.Client.Update(ctx, rabbitmqCluster); err != nil {
			return err
		}
	}
	return nil
}

func (r *RabbitmqClusterReconciler) removeFinalizer(ctx context.Context, rabbitmqCluster *rmq.RabbitmqCluster) error {
	controllerutil.RemoveFinalizer(rabbitmqCluster, deletionFinalizer)
	return r.Client.Update(ctx, rabbitmqCluster)
}

func (r *RabbitmqClusterReconciler) prepareForDeletion(ctx context.Context, rabbitmqCluster *rmq.RabbitmqCluster) error {
	if controllerutil.ContainsFinalizer(rabbitmqCluster, deletionFinalizer) {
		if err := clientretry.RetryOnConflict(clientretry.DefaultRetry, func() error {
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      rabbitmqCluster.ChildResourceName("server"),
					Namespace: rabbitmqCluster.Namespace,
				},
			}
			// Add deletion label to pods
			if err := r.addRabbitmqDeletionLabel(ctx, rabbitmqCluster); err != nil {
				return fmt.Errorf("failed to add deletion markers to RabbitmqCluster Pods: %s", err.Error())
			}
			// Delete StatefulSet
			if err := r.Client.Delete(ctx, sts); client.IgnoreNotFound(err) != nil {
				return fmt.Errorf("cannot delete StatefulSet: %s", err.Error())
			}
			return nil
		}); err != nil {
			ctrl.LoggerFrom(ctx).Error(err, "RabbitmqCluster deletion")
		}

		if err := r.removeFinalizer(ctx, rabbitmqCluster); err != nil {
			ctrl.LoggerFrom(ctx).Error(err, "Failed to remove finalizer for deletion")
			return err
		}
	}
	return nil
}

func (r *RabbitmqClusterReconciler) addRabbitmqDeletionLabel(ctx context.Context, rabbitmqCluster *rmq.RabbitmqCluster) error {
	pods := &corev1.PodList{}
	selector, err := labels.Parse(fmt.Sprintf("app.kubernetes.io/name=%s", rabbitmqCluster.Name))
	if err != nil {
		return err
	}
	listOptions := client.ListOptions{LabelSelector: selector}

	if err := r.Client.List(ctx, pods, &listOptions); err != nil {
		return err
	}

	for i := 0; i < len(pods.Items); i++ {
		pod := &pods.Items[i]
		if pod.Labels == nil {
			pod.Labels = make(map[string]string)
		}
		pod.Labels[DeletionMarker] = "true"
		if err := r.Client.Update(ctx, pod); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("cannot Update Pod %s in Namespace %s: %s", pod.Name, pod.Namespace, err.Error())
		}
	}
	return nil
}

// --- Status ---

func (r *RabbitmqClusterReconciler) updateStatus(ctx context.Context, cluster *rmq.RabbitmqCluster) (time.Duration, error) {
	logger := ctrl.LoggerFrom(ctx)
	childResources, err := r.getChildResources(ctx, cluster)
	if err != nil {
		return 0, err
	}

	oldConditions := make([]rmq.RabbitmqClusterCondition, len(cluster.Status.Conditions))
	copy(oldConditions, cluster.Status.Conditions)
	cluster.Status.SetConditions(childResources)

	if !reflect.DeepEqual(cluster.Status.Conditions, oldConditions) {
		if err = r.Status().Update(ctx, cluster); err != nil {
			if k8serrors.IsConflict(err) {
				logger.Info("failed to update status because of conflict; requeueing...")
				return 2 * time.Second, nil
			}
			return 0, err
		}
	}
	return 0, nil
}

func (r *RabbitmqClusterReconciler) getChildResources(ctx context.Context, cluster *rmq.RabbitmqCluster) ([]runtime.Object, error) {
	sts := &appsv1.StatefulSet{}
	endPoints := &corev1.Endpoints{}

	if err := r.Client.Get(ctx,
		types.NamespacedName{Name: cluster.ChildResourceName("server"), Namespace: cluster.Namespace},
		sts); err != nil && !k8serrors.IsNotFound(err) {
		return nil, err
	} else if k8serrors.IsNotFound(err) {
		sts = nil
	}

	if err := r.Client.Get(ctx,
		types.NamespacedName{Name: cluster.ChildResourceName(ServiceSuffix), Namespace: cluster.Namespace},
		endPoints); err != nil && !k8serrors.IsNotFound(err) {
		return nil, err
	} else if k8serrors.IsNotFound(err) {
		endPoints = nil
	}

	return []runtime.Object{sts, endPoints}, nil
}

func (r *RabbitmqClusterReconciler) setDefaultUserStatus(ctx context.Context, cluster *rmq.RabbitmqCluster) error {
	defaultUserStatus := &rmq.RabbitmqClusterDefaultUser{
		ServiceReference: &rmq.RabbitmqClusterServiceReference{
			Name:      cluster.ChildResourceName(""),
			Namespace: cluster.Namespace,
		},
		SecretReference: &rmq.RabbitmqClusterSecretReference{
			Name:      cluster.ChildResourceName(DefaultUserSecretName),
			Namespace: cluster.Namespace,
			Keys:      map[string]string{"username": "username", "password": "password"},
		},
	}
	if !reflect.DeepEqual(cluster.Status.DefaultUser, defaultUserStatus) {
		cluster.Status.DefaultUser = defaultUserStatus
		if err := r.Status().Update(ctx, cluster); err != nil {
			return err
		}
	}
	return nil
}

func (r *RabbitmqClusterReconciler) setBinding(ctx context.Context, cluster *rmq.RabbitmqCluster) error {
	binding := &corev1.LocalObjectReference{
		Name: cluster.ChildResourceName(DefaultUserSecretName),
	}
	if !reflect.DeepEqual(cluster.Status.Binding, binding) {
		cluster.Status.Binding = binding
		if err := r.Status().Update(ctx, cluster); err != nil {
			return err
		}
	}
	return nil
}

// --- PVC reconciliation ---

func (r *RabbitmqClusterReconciler) reconcilePVC(ctx context.Context, cluster *rmq.RabbitmqCluster, current, desired *appsv1.StatefulSet) error {
	resize, err := needsPVCResize(current, desired)
	if err != nil {
		return err
	}
	if resize {
		return r.expandPVC(ctx, cluster, current, desired)
	}
	return nil
}

func (r *RabbitmqClusterReconciler) expandPVC(ctx context.Context, cluster *rmq.RabbitmqCluster, current, desired *appsv1.StatefulSet) error {
	logger := ctrl.LoggerFrom(ctx)

	desiredCapacity, err := persistenceStorageCapacity(desired.Spec.VolumeClaimTemplates)
	if err != nil {
		return err
	}

	logger.Info(fmt.Sprintf("updating storage capacity to %s", desiredCapacity.String()))

	if err := r.deleteSts(ctx, cluster); err != nil {
		return err
	}
	return r.updatePVC(ctx, cluster, *current.Spec.Replicas, desiredCapacity)
}

func (r *RabbitmqClusterReconciler) updatePVC(ctx context.Context, cluster *rmq.RabbitmqCluster, replicas int32, desiredCapacity k8sresource.Quantity) error {
	logger := ctrl.LoggerFrom(ctx)
	logger.Info("expanding PersistentVolumeClaims")

	for i := 0; i < int(replicas); i++ {
		PVCName := cluster.PVCName(i)
		PVC := corev1.PersistentVolumeClaim{}

		if err := r.Client.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: PVCName}, &PVC); err != nil {
			return fmt.Errorf("failed to get PersistentVolumeClaim %s: %v", PVCName, err)
		}
		PVC.Spec.Resources.Requests[corev1.ResourceStorage] = desiredCapacity
		if err := r.Client.Update(ctx, &PVC); err != nil {
			return fmt.Errorf("failed to update PersistentVolumeClaim %s: %v", PVCName, err)
		}
		logger.Info("successfully expanded", "PVC", PVCName)
	}
	return nil
}

func needsPVCResize(current, desired *appsv1.StatefulSet) (bool, error) {
	currentCapacity, err := persistenceStorageCapacity(current.Spec.VolumeClaimTemplates)
	if err != nil {
		return false, err
	}
	desiredCapacity, err := persistenceStorageCapacity(desired.Spec.VolumeClaimTemplates)
	if err != nil {
		return false, err
	}
	return currentCapacity.Cmp(desiredCapacity) != 0, nil
}

func persistenceStorageCapacity(templates []corev1.PersistentVolumeClaim) (k8sresource.Quantity, error) {
	for _, t := range templates {
		if t.Name == "persistence" {
			return t.Spec.Resources.Requests[corev1.ResourceStorage], nil
		}
	}
	return k8sresource.Quantity{}, errors.New("cannot find PersistentVolumeClaim 'persistence'")
}

func (r *RabbitmqClusterReconciler) deleteSts(ctx context.Context, cluster *rmq.RabbitmqCluster) error {
	logger := ctrl.LoggerFrom(ctx)
	logger.Info("deleting statefulSet (pods won't be deleted)", "statefulSet", cluster.ChildResourceName("server"))
	deletePropagationPolicy := metav1.DeletePropagationOrphan
	deleteOptions := &client.DeleteOptions{PropagationPolicy: &deletePropagationPolicy}
	currentSts, err := r.statefulSet(ctx, cluster)
	if err != nil {
		return err
	}
	if err := r.Delete(ctx, currentSts, deleteOptions); err != nil {
		return fmt.Errorf("failed to delete statefulSet %s: %v", currentSts.Name, err)
	}
	// In real operator, retries 10 times with 3s sleep. In simulation, deletion is immediate.
	logger.Info("statefulSet deleted", "statefulSet", currentSts.Name)
	return nil
}

// --- Scale down ---

func (r *RabbitmqClusterReconciler) scaleDown(ctx context.Context, cluster *rmq.RabbitmqCluster, current, sts *appsv1.StatefulSet) bool {
	logger := ctrl.LoggerFrom(ctx)
	currentReplicas := *current.Spec.Replicas
	desiredReplicas := *sts.Spec.Replicas
	if currentReplicas > desiredReplicas {
		logger.Error(errors.New("UnsupportedOperation"), "Cluster Scale down not supported")
		// Only write status if not already set to avoid infinite re-reconciliation
		alreadySet := false
		for _, c := range cluster.Status.Conditions {
			if c.Type == rmq.ReconcileSuccess && c.Status == corev1.ConditionFalse && c.Reason == "UnsupportedOperation" {
				alreadySet = true
				break
			}
		}
		if !alreadySet {
			cluster.Status.SetCondition(rmq.ReconcileSuccess, corev1.ConditionFalse, "UnsupportedOperation", "Cluster Scale down not supported")
			r.Status().Update(ctx, cluster)
		}
		return true
	}
	return false
}

// --- Annotations and restarts ---

func (r *RabbitmqClusterReconciler) annotateIfNeeded(ctx context.Context, logger logr.Logger, builder resourceBuilder, operationResult controllerutil.OperationResult, cluster *rmq.RabbitmqCluster) error {
	var (
		obj           client.Object
		objName       string
		annotationKey string
	)

	switch {
	case builder.Kind == "PluginsConfigMap" && operationResult == controllerutil.OperationResultUpdated:
		obj = &corev1.ConfigMap{}
		objName = cluster.ChildResourceName(PluginsConfigName)
		annotationKey = pluginsUpdateAnnotation
	case builder.Kind == "ServerConfigMap" && operationResult == controllerutil.OperationResultUpdated:
		obj = &corev1.ConfigMap{}
		objName = cluster.ChildResourceName(ServerConfigMapName)
		annotationKey = serverConfAnnotation
	case builder.Kind == "StatefulSet" && operationResult == controllerutil.OperationResultCreated:
		obj = &appsv1.StatefulSet{}
		objName = cluster.ChildResourceName("server")
		annotationKey = stsCreateAnnotation
	default:
		return nil
	}

	return r.updateAnnotation(ctx, obj, cluster.Namespace, objName, annotationKey, stableTimeStr)
}

func (r *RabbitmqClusterReconciler) restartStatefulSetIfNeeded(ctx context.Context, logger logr.Logger, cluster *rmq.RabbitmqCluster) (time.Duration, error) {
	serverConf, err := r.configMap(ctx, cluster, cluster.ChildResourceName(ServerConfigMapName))
	if err != nil {
		return 10 * time.Second, client.IgnoreNotFound(err)
	}

	serverConfigUpdatedAt, ok := serverConf.Annotations[serverConfAnnotation]
	if !ok {
		return 0, nil
	}

	sts, err := r.statefulSet(ctx, cluster)
	if err != nil {
		return 10 * time.Second, client.IgnoreNotFound(err)
	}

	stsRestartedAt, ok := sts.Spec.Template.ObjectMeta.Annotations[stsRestartAnnotation]
	if ok && stsRestartedAt >= serverConfigUpdatedAt {
		return 0, nil
	}

	if err := clientretry.RetryOnConflict(clientretry.DefaultRetry, func() error {
		sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: cluster.ChildResourceName("server"), Namespace: cluster.Namespace}}
		if err := r.Get(ctx, types.NamespacedName{Name: sts.Name, Namespace: sts.Namespace}, sts); err != nil {
			return err
		}
		if sts.Spec.Template.ObjectMeta.Annotations == nil {
			sts.Spec.Template.ObjectMeta.Annotations = make(map[string]string)
		}
		sts.Spec.Template.ObjectMeta.Annotations[stsRestartAnnotation] = stableTimeStr
		return r.Update(ctx, sts)
	}); err != nil {
		return 0, err
	}

	return 0, nil
}

func (r *RabbitmqClusterReconciler) markForQueueRebalance(ctx context.Context, cluster *rmq.RabbitmqCluster) error {
	if cluster.ObjectMeta.Annotations == nil {
		cluster.ObjectMeta.Annotations = make(map[string]string)
	}
	if len(cluster.ObjectMeta.Annotations[queueRebalanceAnnotation]) > 0 {
		return nil
	}
	cluster.ObjectMeta.Annotations[queueRebalanceAnnotation] = stableTimeStr
	return r.Update(ctx, cluster)
}

// --- Helper methods ---

func (r *RabbitmqClusterReconciler) getRabbitmqCluster(ctx context.Context, namespacedName types.NamespacedName) (*rmq.RabbitmqCluster, error) {
	instance := &rmq.RabbitmqCluster{}
	err := r.Get(ctx, namespacedName, instance)
	return instance, err
}

func (r *RabbitmqClusterReconciler) statefulSet(ctx context.Context, cluster *rmq.RabbitmqCluster) (*appsv1.StatefulSet, error) {
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: cluster.ChildResourceName("server"), Namespace: cluster.Namespace}, sts); err != nil {
		return nil, err
	}
	return sts, nil
}

func (r *RabbitmqClusterReconciler) configMap(ctx context.Context, cluster *rmq.RabbitmqCluster, name string) (*corev1.ConfigMap, error) {
	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, cm); err != nil {
		return nil, err
	}
	return cm, nil
}

func (r *RabbitmqClusterReconciler) updateAnnotation(ctx context.Context, obj client.Object, namespace, objName, key, value string) error {
	return clientretry.RetryOnConflict(clientretry.DefaultRetry, func() error {
		if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: objName}, obj); err != nil {
			return err
		}
		accessor, err := meta.Accessor(obj)
		if err != nil {
			return err
		}
		annotations := accessor.GetAnnotations()
		if annotations == nil {
			annotations = make(map[string]string)
		}
		annotations[key] = value
		accessor.SetAnnotations(annotations)
		return r.Update(ctx, obj)
	})
}

func (r *RabbitmqClusterReconciler) logAndRecordOperationResult(logger logr.Logger, _ runtime.Object, resource runtime.Object, operationResult controllerutil.OperationResult, err error) {
	if operationResult == controllerutil.OperationResultNone && err == nil {
		return
	}
	var operation string
	if operationResult == controllerutil.OperationResultCreated {
		operation = "create"
	}
	if operationResult == controllerutil.OperationResultUpdated {
		operation = "update"
	}
	if err == nil {
		logger.Info(fmt.Sprintf("%sd resource %s of Type %T", operation, resource.(metav1.Object).GetName(), resource.(metav1.Object)))
	}
	if err != nil {
		logger.Error(err, fmt.Sprintf("failed to %s resource %s of Type %T", operation, resource.(metav1.Object).GetName(), resource.(metav1.Object)))
	}
}

func statefulSetNeedsQueueRebalance(sts *appsv1.StatefulSet, cluster *rmq.RabbitmqCluster) bool {
	return sts.Status.CurrentRevision != sts.Status.UpdateRevision &&
		!cluster.Spec.SkipPostDeploySteps &&
		cluster.Spec.Replicas != nil && *cluster.Spec.Replicas > 1
}

// --- Simplified resource builders ---
// Instead of the full internal/resource package, we inline lightweight builders
// that produce the same K8s objects the real operator creates.

type resourceBuilder struct {
	Kind                       string
	UpdateMayRequireStsRecreate bool
	buildFn                    func() client.Object
	updateFn                   func(client.Object) error
}

func (b resourceBuilder) Build() client.Object   { return b.buildFn() }
func (b resourceBuilder) Update(obj client.Object) error {
	if b.updateFn != nil {
		return b.updateFn(obj)
	}
	return nil
}

func (r *RabbitmqClusterReconciler) resourceBuilders(instance *rmq.RabbitmqCluster) []resourceBuilder {
	replicas := int32(1)
	if instance.Spec.Replicas != nil {
		replicas = *instance.Spec.Replicas
	}
	storage := k8sresource.MustParse("10Gi")
	if instance.Spec.Persistence.Storage != nil {
		storage = *instance.Spec.Persistence.Storage
	}

	appLabels := map[string]string{
		"app.kubernetes.io/name":    instance.Name,
		"app.kubernetes.io/part-of": "rabbitmq",
	}

	return []resourceBuilder{
		// HeadlessService
		{Kind: "HeadlessService", buildFn: func() client.Object {
			return &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      instance.ChildResourceName("nodes"),
					Namespace: instance.Namespace,
					Labels:    appLabels,
					OwnerReferences: []metav1.OwnerReference{ownerRef(instance)},
				},
				Spec: corev1.ServiceSpec{
					Type:      corev1.ServiceTypeClusterIP,
					ClusterIP: "None",
					Selector:  appLabels,
					Ports: []corev1.ServicePort{
						{Name: "epmd", Port: 4369},
						{Name: "cluster-rpc", Port: 25672},
					},
					PublishNotReadyAddresses: true,
				},
			}
		}, updateFn: func(obj client.Object) error {
			svc := obj.(*corev1.Service)
			svc.Labels = appLabels
			if svc.OwnerReferences == nil {
				svc.OwnerReferences = []metav1.OwnerReference{ownerRef(instance)}
			}
			return nil
		}},
		// Service (client-facing)
		{Kind: "Service", buildFn: func() client.Object {
			svcType := corev1.ServiceTypeClusterIP
			if instance.Spec.Service.Type != "" {
				svcType = instance.Spec.Service.Type
			}
			return &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      instance.ChildResourceName(ServiceSuffix),
					Namespace: instance.Namespace,
					Labels:    appLabels,
					OwnerReferences: []metav1.OwnerReference{ownerRef(instance)},
				},
				Spec: corev1.ServiceSpec{
					Type:     svcType,
					Selector: appLabels,
					Ports: []corev1.ServicePort{
						{Name: "amqp", Port: 5672},
						{Name: "management", Port: 15672},
						{Name: "prometheus", Port: 15692},
					},
				},
			}
		}, updateFn: func(obj client.Object) error {
			svc := obj.(*corev1.Service)
			svc.Labels = appLabels
			if svc.OwnerReferences == nil {
				svc.OwnerReferences = []metav1.OwnerReference{ownerRef(instance)}
			}
			return nil
		}},
		// ErlangCookie Secret
		{Kind: "ErlangCookie", buildFn: func() client.Object {
			return &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      instance.ChildResourceName("erlang-cookie"),
					Namespace: instance.Namespace,
					Labels:    appLabels,
					OwnerReferences: []metav1.OwnerReference{ownerRef(instance)},
				},
				Type: corev1.SecretTypeOpaque,
				Data: map[string][]byte{".erlang.cookie": []byte("simulated-cookie")},
			}
		}},
		// DefaultUserSecret
		{Kind: "DefaultUserSecret", buildFn: func() client.Object {
			return &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      instance.ChildResourceName(DefaultUserSecretName),
					Namespace: instance.Namespace,
					Labels:    appLabels,
					OwnerReferences: []metav1.OwnerReference{ownerRef(instance)},
				},
				Type: corev1.SecretTypeOpaque,
				Data: map[string][]byte{
					"username": []byte("guest"),
					"password": []byte("guest"),
				},
			}
		}},
		// PluginsConfigMap
		{Kind: "PluginsConfigMap", buildFn: func() client.Object {
			plugins := "rabbitmq_peer_discovery_k8s,rabbitmq_prometheus,rabbitmq_management"
			return &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      instance.ChildResourceName(PluginsConfigName),
					Namespace: instance.Namespace,
					Labels:    appLabels,
					OwnerReferences: []metav1.OwnerReference{ownerRef(instance)},
				},
				Data: map[string]string{"enabled_plugins": "[" + plugins + "]."},
			}
		}, updateFn: func(obj client.Object) error {
			cm := obj.(*corev1.ConfigMap)
			cm.Labels = appLabels
			if cm.OwnerReferences == nil {
				cm.OwnerReferences = []metav1.OwnerReference{ownerRef(instance)}
			}
			return nil
		}},
		// ServerConfigMap
		{Kind: "ServerConfigMap", buildFn: func() client.Object {
			return &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      instance.ChildResourceName(ServerConfigMapName),
					Namespace: instance.Namespace,
					Labels:    appLabels,
					OwnerReferences: []metav1.OwnerReference{ownerRef(instance)},
				},
				Data: map[string]string{"rabbitmq.conf": "cluster_formation.peer_discovery_backend = rabbit_peer_discovery_k8s\n"},
			}
		}, updateFn: func(obj client.Object) error {
			cm := obj.(*corev1.ConfigMap)
			cm.Labels = appLabels
			if cm.OwnerReferences == nil {
				cm.OwnerReferences = []metav1.OwnerReference{ownerRef(instance)}
			}
			return nil
		}},
		// ServiceAccount
		{Kind: "ServiceAccount", buildFn: func() client.Object {
			return &corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{
					Name:      instance.ChildResourceName("server"),
					Namespace: instance.Namespace,
					Labels:    appLabels,
					OwnerReferences: []metav1.OwnerReference{ownerRef(instance)},
				},
			}
		}},
		// StatefulSet
		{Kind: "StatefulSet", UpdateMayRequireStsRecreate: true, buildFn: func() client.Object {
			return &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      instance.ChildResourceName("server"),
					Namespace: instance.Namespace,
					Labels:    appLabels,
					OwnerReferences: []metav1.OwnerReference{ownerRef(instance)},
				},
				Spec: appsv1.StatefulSetSpec{
					ServiceName: instance.ChildResourceName("nodes"),
					Replicas:    &replicas,
					Selector: &metav1.LabelSelector{
						MatchLabels: appLabels,
					},
					PodManagementPolicy: appsv1.ParallelPodManagement,
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: appLabels},
						Spec: corev1.PodSpec{
							ServiceAccountName: instance.ChildResourceName("server"),
							Containers: []corev1.Container{
								{
									Name:  "rabbitmq",
									Image: instance.Spec.Image,
									Ports: []corev1.ContainerPort{
										{Name: "amqp", ContainerPort: 5672},
										{Name: "management", ContainerPort: 15672},
										{Name: "prometheus", ContainerPort: 15692},
									},
								},
							},
						},
					},
					VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
						{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "persistence",
								Namespace: instance.Namespace,
								Labels:    appLabels,
							},
							Spec: corev1.PersistentVolumeClaimSpec{
								AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
								Resources: corev1.VolumeResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourceStorage: storage,
									},
								},
							},
						},
					},
				},
			}
		}, updateFn: func(obj client.Object) error {
			sts := obj.(*appsv1.StatefulSet)
			sts.Spec.Replicas = &replicas
			sts.Labels = appLabels
			if sts.OwnerReferences == nil {
				sts.OwnerReferences = []metav1.OwnerReference{ownerRef(instance)}
			}
			// Update VCT storage
			for i := range sts.Spec.VolumeClaimTemplates {
				if sts.Spec.VolumeClaimTemplates[i].Name == "persistence" {
					sts.Spec.VolumeClaimTemplates[i].Spec.Resources.Requests[corev1.ResourceStorage] = storage
				}
			}
			return nil
		}},
	}
}

func ownerRef(instance *rmq.RabbitmqCluster) metav1.OwnerReference {
	t := true
	return metav1.OwnerReference{
		APIVersion:         rmq.GroupVersion.String(),
		Kind:               "RabbitmqCluster",
		Name:               instance.Name,
		UID:                instance.UID,
		Controller:         &t,
		BlockOwnerDeletion: &t,
	}
}

// Ensure it does compile (unused imports guard)
var _ = strings.Title
