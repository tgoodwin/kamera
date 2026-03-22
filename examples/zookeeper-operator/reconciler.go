/**
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	zookeeperv1beta1 "github.com/tgoodwin/kamera/examples/zookeeper-operator/zk"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// ReconcileTime is the delay between reconciliations
const ReconcileTime = 30 * time.Second

var log = logf.Log.WithName("controller_zookeepercluster")

var _ reconcile.Reconciler = &ZookeeperClusterReconciler{}

// ZookeeperClient is the interface for talking to ZK nodes.
// In the kamera harness, we use a no-op stub since real ZK pods don't exist.
type ZookeeperClient interface {
	Connect(string) error
	CreateNode(*zookeeperv1beta1.ZookeeperCluster, string) error
	NodeExists(string) (int32, error)
	UpdateNode(string, string, int32) error
	Close()
}

// noopZkClient is a no-op stub satisfying ZookeeperClient.
// The bug under test is in K8s resource management, not ZK client interaction.
type noopZkClient struct{}

func (n *noopZkClient) Connect(string) error                                           { return nil }
func (n *noopZkClient) CreateNode(*zookeeperv1beta1.ZookeeperCluster, string) error    { return nil }
func (n *noopZkClient) NodeExists(string) (int32, error)                               { return 0, nil }
func (n *noopZkClient) UpdateNode(string, string, int32) error                         { return nil }
func (n *noopZkClient) Close()                                                         {}

// ZookeeperClusterReconciler reconciles a ZookeeperCluster object
type ZookeeperClusterReconciler struct {
	Client   client.Client
	Log      logr.Logger
	Scheme   *runtime.Scheme
	ZkClient ZookeeperClient
	ctx      context.Context // set per-reconcile; used by sub-methods
}

type reconcileFun func(cluster *zookeeperv1beta1.ZookeeperCluster) error

func (r *ZookeeperClusterReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	r.ctx = ctx
	r.Log = log.WithValues(
		"Request.Namespace", request.Namespace,
		"Request.Name", request.Name)
	r.Log.Info("Reconciling ZookeeperCluster")

	// Fetch the ZookeeperCluster instance
	instance := &zookeeperv1beta1.ZookeeperCluster{}
	err := r.Client.Get(r.ctx, request.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}
	changed := instance.WithDefaults()
	if instance.GetTriggerRollingRestart() {
		r.Log.Info("Restarting zookeeper cluster")
		annotationkey, annotationvalue := getRollingRestartAnnotation()
		if instance.Spec.Pod.Annotations == nil {
			instance.Spec.Pod.Annotations = make(map[string]string)
		}
		instance.Spec.Pod.Annotations[annotationkey] = annotationvalue
		instance.SetTriggerRollingRestart(false)
		changed = true
	}
	if changed {
		r.Log.Info("Setting default settings for zookeeper-cluster")
		if err := r.Client.Update(r.ctx, instance); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{Requeue: true}, nil
	}
	for _, fun := range []reconcileFun{
		r.reconcileFinalizers,
		r.reconcileConfigMap,
		r.reconcileStatefulSet,
		r.reconcileClientService,
		r.reconcileHeadlessService,
		r.reconcileAdminServerService,
		r.reconcilePodDisruptionBudget,
		r.reconcileClusterStatus,
	} {
		if err = fun(instance); err != nil {
			return reconcile.Result{}, err
		}
	}
	// Recreate any missing resources every 'ReconcileTime'
	return reconcile.Result{RequeueAfter: ReconcileTime}, nil
}

func getRollingRestartAnnotation() (string, string) {
	return "restartTime", time.Now().Format(time.RFC850)
}

// compareResourceVersion compare resource versions for the supplied ZookeeperCluster and StatefulSet
func compareResourceVersion(zkObj *zookeeperv1beta1.ZookeeperCluster, sts *appsv1.StatefulSet) int {
	zkResourceVersion, zkErr := strconv.Atoi(zkObj.ResourceVersion)
	stsVersion, stsVersionFound := sts.Labels["owner-rv"]

	if !stsVersionFound {
		if zkErr != nil {
			log.Info("Fail to parse ZookeeperCluster version. Cannot decide zookeeper StatefulSet version")
			return 0
		}
		return 1
	}
	stsResourceVersion, err := strconv.Atoi(stsVersion)
	if err != nil {
		if zkErr != nil {
			log.Info("Fail to parse ZookeeperCluster version. Cannot decide zookeeper StatefulSet version")
			return 0
		}
		log.Info("Fail to convert StatefulSet version to integer; setting it to ZookeeperCluster version", "stsVersion", stsVersion)
		return 1
	}
	if zkResourceVersion < stsResourceVersion {
		return -1
	} else if zkResourceVersion > stsResourceVersion {
		return 1
	}
	return 0
}

func (r *ZookeeperClusterReconciler) reconcileStatefulSet(instance *zookeeperv1beta1.ZookeeperCluster) (err error) {
	// we cannot upgrade if cluster is in UpgradeFailed
	if instance.Status.IsClusterInUpgradeFailedState() {
		return nil
	}
	if instance.Spec.Pod.ServiceAccountName != "default" {
		serviceAccount := zookeeperv1beta1.MakeServiceAccount(instance)
		if err = controllerutil.SetControllerReference(instance, serviceAccount, r.Scheme); err != nil {
			return err
		}
		foundServiceAccount := &corev1.ServiceAccount{}
		err = r.Client.Get(r.ctx, types.NamespacedName{Name: serviceAccount.Name, Namespace: serviceAccount.Namespace}, foundServiceAccount)
		if err != nil && errors.IsNotFound(err) {
			r.Log.Info("Creating a new ServiceAccount", "ServiceAccount.Namespace", serviceAccount.Namespace, "ServiceAccount.Name", serviceAccount.Name)
			err = r.Client.Create(r.ctx, serviceAccount)
			if err != nil {
				return err
			}
		} else if err != nil {
			return err
		} else {
			foundServiceAccount.ImagePullSecrets = serviceAccount.ImagePullSecrets
			r.Log.Info("Updating ServiceAccount", "ServiceAccount.Namespace", serviceAccount.Namespace, "ServiceAccount.Name", serviceAccount.Name)
			err = r.Client.Update(r.ctx, foundServiceAccount)
			if err != nil {
				return err
			}
		}
	}
	sts := zookeeperv1beta1.MakeStatefulSet(instance)
	if err = controllerutil.SetControllerReference(instance, sts, r.Scheme); err != nil {
		return err
	}
	foundSts := &appsv1.StatefulSet{}
	err = r.Client.Get(r.ctx, types.NamespacedName{
		Name:      sts.Name,
		Namespace: sts.Namespace,
	}, foundSts)
	if err != nil && errors.IsNotFound(err) {
		r.Log.Info("Creating a new Zookeeper StatefulSet",
			"StatefulSet.Namespace", sts.Namespace,
			"StatefulSet.Name", sts.Name)
		// label the RV of the zookeeperCluster when creating the sts
		sts.Labels["owner-rv"] = instance.ResourceVersion
		err = r.Client.Create(r.ctx, sts)
		if err != nil {
			return err
		}
		return nil
	} else if err != nil {
		return err
	} else {
		// check whether zookeeperCluster is updated before updating the sts
		cmp := compareResourceVersion(instance, foundSts)
		if cmp < 0 {
			return fmt.Errorf("Staleness: cr.ResourceVersion %s is smaller than labeledRV %s", instance.ResourceVersion, foundSts.Labels["owner-rv"])
		} else if cmp > 0 {
			// Zookeeper StatefulSet version inherits ZookeeperCluster resource version
			foundSts.Labels["owner-rv"] = instance.ResourceVersion
		}
		foundSTSSize := *foundSts.Spec.Replicas
		newSTSSize := *sts.Spec.Replicas
		if newSTSSize != foundSTSSize {
			zkUri := zookeeperv1beta1.GetZkServiceUri(instance)
			err = r.ZkClient.Connect(zkUri)
			if err != nil {
				return fmt.Errorf("Error storing cluster size %v", err)
			}
			defer r.ZkClient.Close()
			r.Log.Info("Connected to ZK", "ZKURI", zkUri)

			path := zookeeperv1beta1.GetMetaPath(instance)
			version, err := r.ZkClient.NodeExists(path)
			if err != nil {
				return fmt.Errorf("Error doing exists check for znode %s: %v", path, err)
			}

			data := "CLUSTER_SIZE=" + strconv.Itoa(int(newSTSSize))
			r.Log.Info("Updating Cluster Size.", "New Data:", data, "Version", version)
			r.ZkClient.UpdateNode(path, data, version)
		}
		err = r.updateStatefulSet(instance, foundSts, sts)
		if err != nil {
			return err
		}
		return r.upgradeStatefulSet(instance, foundSts)
	}
}

func (r *ZookeeperClusterReconciler) updateStatefulSet(instance *zookeeperv1beta1.ZookeeperCluster, foundSts *appsv1.StatefulSet, sts *appsv1.StatefulSet) (err error) {
	r.Log.Info("Updating StatefulSet",
		"StatefulSet.Namespace", foundSts.Namespace,
		"StatefulSet.Name", foundSts.Name)
	zookeeperv1beta1.SyncStatefulSet(foundSts, sts)

	err = r.Client.Update(r.ctx, foundSts)
	if err != nil {
		return err
	}
	instance.Status.Replicas = foundSts.Status.Replicas
	instance.Status.ReadyReplicas = foundSts.Status.ReadyReplicas
	return nil
}

func (r *ZookeeperClusterReconciler) upgradeStatefulSet(instance *zookeeperv1beta1.ZookeeperCluster, foundSts *appsv1.StatefulSet) (err error) {
	// Getting the upgradeCondition from the zk clustercondition
	_, upgradeCondition := instance.Status.GetClusterCondition(zookeeperv1beta1.ClusterConditionUpgrading)

	if upgradeCondition == nil {
		// Initially set upgrading condition to false
		instance.Status.SetUpgradingConditionFalse()
		return nil
	}

	// Setting the upgrade condition to true to trigger the upgrade
	if upgradeCondition.Status == corev1.ConditionFalse {
		if instance.Status.IsClusterInReadyState() && foundSts.Status.CurrentRevision != foundSts.Status.UpdateRevision && instance.Spec.Image.Tag != instance.Status.CurrentVersion {
			instance.Status.TargetVersion = instance.Spec.Image.Tag
			instance.Status.SetPodsReadyConditionFalse()
			instance.Status.SetUpgradingConditionTrue("", "")
		}
	}

	// checking if the upgrade is in progress
	if upgradeCondition.Status == corev1.ConditionTrue {
		if instance.Status.TargetVersion == "" {
			r.Log.Info("upgrading to an unknown version: cancelling upgrade process")
			return r.clearUpgradeStatus(instance)
		}
		if foundSts.Status.CurrentRevision == foundSts.Status.UpdateRevision {
			instance.Status.CurrentVersion = instance.Status.TargetVersion
			r.Log.Info("upgrade completed")
			return r.clearUpgradeStatus(instance)
		}
		if foundSts.Status.CurrentRevision != foundSts.Status.UpdateRevision {
			r.Log.Info("upgrade in progress")
			if fmt.Sprint(foundSts.Status.UpdatedReplicas) != upgradeCondition.Message {
				instance.Status.UpdateProgress(zookeeperv1beta1.UpdatingZookeeperReason, fmt.Sprint(foundSts.Status.UpdatedReplicas))
			} else {
				err = checkSyncTimeout(instance, zookeeperv1beta1.UpdatingZookeeperReason, foundSts.Status.UpdatedReplicas, 10*time.Minute)
				if err != nil {
					instance.Status.SetErrorConditionTrue("UpgradeFailed", err.Error())
					return r.Client.Status().Update(r.ctx, instance)
				} else {
					return nil
				}
			}
		}
	}
	return r.Client.Status().Update(r.ctx, instance)
}

func (r *ZookeeperClusterReconciler) clearUpgradeStatus(z *zookeeperv1beta1.ZookeeperCluster) (err error) {
	z.Status.SetUpgradingConditionFalse()
	z.Status.TargetVersion = ""
	status := z.Status.DeepCopy()

	err = r.Client.Update(r.ctx, z)
	if err != nil {
		return err
	}

	z.Status = *status
	return nil
}

func checkSyncTimeout(z *zookeeperv1beta1.ZookeeperCluster, reason string, updatedReplicas int32, t time.Duration) error {
	lastCondition := z.Status.GetLastCondition()
	if lastCondition == nil {
		return nil
	}
	if lastCondition.Reason == reason && lastCondition.Message == fmt.Sprint(updatedReplicas) {
		parsedTime, _ := time.Parse(time.RFC3339, lastCondition.LastUpdateTime)
		if time.Now().After(parsedTime.Add(t)) {
			return fmt.Errorf("progress deadline exceeded")
		}
	}
	return nil
}

func (r *ZookeeperClusterReconciler) reconcileClientService(instance *zookeeperv1beta1.ZookeeperCluster) (err error) {
	svc := zookeeperv1beta1.MakeClientService(instance)
	if err = controllerutil.SetControllerReference(instance, svc, r.Scheme); err != nil {
		return err
	}
	foundSvc := &corev1.Service{}
	err = r.Client.Get(r.ctx, types.NamespacedName{
		Name:      svc.Name,
		Namespace: svc.Namespace,
	}, foundSvc)
	if err != nil && errors.IsNotFound(err) {
		r.Log.Info("Creating new client service",
			"Service.Namespace", svc.Namespace,
			"Service.Name", svc.Name)
		err = r.Client.Create(r.ctx, svc)
		if err != nil {
			return err
		}
		return nil
	} else if err != nil {
		return err
	} else {
		r.Log.Info("Updating existing client service",
			"Service.Namespace", foundSvc.Namespace,
			"Service.Name", foundSvc.Name)
		zookeeperv1beta1.SyncService(foundSvc, svc)
		err = r.Client.Update(r.ctx, foundSvc)
		if err != nil {
			return err
		}
		port := instance.ZookeeperPorts().Client
		instance.Status.InternalClientEndpoint = fmt.Sprintf("%s:%d",
			foundSvc.Spec.ClusterIP, port)
		if foundSvc.Spec.Type == "LoadBalancer" {
			for _, i := range foundSvc.Status.LoadBalancer.Ingress {
				if i.IP != "" {
					instance.Status.ExternalClientEndpoint = fmt.Sprintf("%s:%d",
						i.IP, port)
				}
			}
		} else {
			instance.Status.ExternalClientEndpoint = "N/A"
		}
	}
	return nil
}

func (r *ZookeeperClusterReconciler) reconcileHeadlessService(instance *zookeeperv1beta1.ZookeeperCluster) (err error) {
	svc := zookeeperv1beta1.MakeHeadlessService(instance)
	if err = controllerutil.SetControllerReference(instance, svc, r.Scheme); err != nil {
		return err
	}
	foundSvc := &corev1.Service{}
	err = r.Client.Get(r.ctx, types.NamespacedName{
		Name:      svc.Name,
		Namespace: svc.Namespace,
	}, foundSvc)
	if err != nil && errors.IsNotFound(err) {
		r.Log.Info("Creating new headless service",
			"Service.Namespace", svc.Namespace,
			"Service.Name", svc.Name)
		err = r.Client.Create(r.ctx, svc)
		if err != nil {
			return err
		}
		return nil
	} else if err != nil {
		return err
	} else {
		r.Log.Info("Updating existing headless service",
			"Service.Namespace", foundSvc.Namespace,
			"Service.Name", foundSvc.Name)
		zookeeperv1beta1.SyncService(foundSvc, svc)
		err = r.Client.Update(r.ctx, foundSvc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *ZookeeperClusterReconciler) reconcileAdminServerService(instance *zookeeperv1beta1.ZookeeperCluster) (err error) {
	svc := zookeeperv1beta1.MakeAdminServerService(instance)
	if err = controllerutil.SetControllerReference(instance, svc, r.Scheme); err != nil {
		return err
	}
	foundSvc := &corev1.Service{}
	err = r.Client.Get(r.ctx, types.NamespacedName{
		Name:      svc.Name,
		Namespace: svc.Namespace,
	}, foundSvc)
	if err != nil && errors.IsNotFound(err) {
		r.Log.Info("Creating admin server service",
			"Service.Namespace", svc.Namespace,
			"Service.Name", svc.Name)
		err = r.Client.Create(r.ctx, svc)
		if err != nil {
			return err
		}
		return nil
	} else if err != nil {
		return err
	} else {
		r.Log.Info("Updating existing admin server service",
			"Service.Namespace", foundSvc.Namespace,
			"Service.Name", foundSvc.Name)
		zookeeperv1beta1.SyncService(foundSvc, svc)
		err = r.Client.Update(r.ctx, foundSvc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *ZookeeperClusterReconciler) reconcilePodDisruptionBudget(instance *zookeeperv1beta1.ZookeeperCluster) (err error) {
	pdb := zookeeperv1beta1.MakePodDisruptionBudget(instance)
	if err = controllerutil.SetControllerReference(instance, pdb, r.Scheme); err != nil {
		return err
	}
	foundPdb := &policyv1.PodDisruptionBudget{}
	err = r.Client.Get(r.ctx, types.NamespacedName{
		Name:      pdb.Name,
		Namespace: pdb.Namespace,
	}, foundPdb)
	if err != nil && errors.IsNotFound(err) {
		r.Log.Info("Creating new pod-disruption-budget",
			"PodDisruptionBudget.Namespace", pdb.Namespace,
			"PodDisruptionBudget.Name", pdb.Name)
		err = r.Client.Create(r.ctx, pdb)
		if err != nil {
			return err
		}
		return nil
	} else if err != nil {
		return err
	}
	return nil
}

func (r *ZookeeperClusterReconciler) reconcileConfigMap(instance *zookeeperv1beta1.ZookeeperCluster) (err error) {
	cm := zookeeperv1beta1.MakeConfigMap(instance)
	if err = controllerutil.SetControllerReference(instance, cm, r.Scheme); err != nil {
		return err
	}
	foundCm := &corev1.ConfigMap{}
	err = r.Client.Get(r.ctx, types.NamespacedName{
		Name:      cm.Name,
		Namespace: cm.Namespace,
	}, foundCm)
	if err != nil && errors.IsNotFound(err) {
		r.Log.Info("Creating a new Zookeeper Config Map",
			"ConfigMap.Namespace", cm.Namespace,
			"ConfigMap.Name", cm.Name)
		err = r.Client.Create(r.ctx, cm)
		if err != nil {
			return err
		}
		return nil
	} else if err != nil {
		return err
	} else {
		r.Log.Info("Updating existing config-map",
			"ConfigMap.Namespace", foundCm.Namespace,
			"ConfigMap.Name", foundCm.Name)
		zookeeperv1beta1.SyncConfigMap(foundCm, cm)
		err = r.Client.Update(r.ctx, foundCm)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *ZookeeperClusterReconciler) reconcileClusterStatus(instance *zookeeperv1beta1.ZookeeperCluster) (err error) {
	if instance.Status.IsClusterInUpgradingState() || instance.Status.IsClusterInUpgradeFailedState() {
		return nil
	}
	instance.Status.Init()
	foundPods := &corev1.PodList{}
	labelSelector := labels.SelectorFromSet(map[string]string{"app": instance.GetName()})
	listOps := &client.ListOptions{
		Namespace:     instance.Namespace,
		LabelSelector: labelSelector,
	}
	err = r.Client.List(r.ctx, foundPods, listOps)
	if err != nil {
		return err
	}
	var (
		readyMembers   []string
		unreadyMembers []string
	)
	for _, p := range foundPods.Items {
		ready := true
		for _, c := range p.Status.ContainerStatuses {
			if !c.Ready {
				ready = false
			}
		}
		if ready {
			readyMembers = append(readyMembers, p.Name)
		} else {
			unreadyMembers = append(unreadyMembers, p.Name)
		}
	}
	instance.Status.Members.Ready = readyMembers
	instance.Status.Members.Unready = unreadyMembers

	// If Cluster is in a ready state, create ZK metadata (no-op in simulation)
	if instance.Spec.Replicas == instance.Status.ReadyReplicas && (!instance.Status.MetaRootCreated) {
		r.Log.Info("Cluster is Ready, Creating ZK Metadata...")
		zkUri := zookeeperv1beta1.GetZkServiceUri(instance)
		err := r.ZkClient.Connect(zkUri)
		if err != nil {
			return fmt.Errorf("Error creating cluster metaroot. Connect to zk failed %v", err)
		}
		defer r.ZkClient.Close()
		metaPath := zookeeperv1beta1.GetMetaPath(instance)
		r.Log.Info("Connected to zookeeper:", "ZKUri", zkUri, "Creating Path", metaPath)
		if err := r.ZkClient.CreateNode(instance, metaPath); err != nil {
			return fmt.Errorf("Error creating cluster metadata path %s, %v", metaPath, err)
		}
		r.Log.Info("Metadata znode created.")
		instance.Status.MetaRootCreated = true
	}
	r.Log.Info("Updating zookeeper status",
		"StatefulSet.Namespace", instance.Namespace,
		"StatefulSet.Name", instance.Name)
	if instance.Status.ReadyReplicas == instance.Spec.Replicas {
		instance.Status.SetPodsReadyConditionTrue()
	} else {
		instance.Status.SetPodsReadyConditionFalse()
	}
	if instance.Status.CurrentVersion == "" && instance.Status.IsClusterInReadyState() {
		instance.Status.CurrentVersion = instance.Spec.Image.Tag
	}
	return r.Client.Status().Update(r.ctx, instance)
}

func (r *ZookeeperClusterReconciler) reconcileFinalizers(instance *zookeeperv1beta1.ZookeeperCluster) (err error) {
	if instance.Spec.Persistence != nil && instance.Spec.Persistence.VolumeReclaimPolicy != zookeeperv1beta1.VolumeReclaimPolicyDelete {
		return nil
	}
	if instance.DeletionTimestamp.IsZero() {
		if !zookeeperv1beta1.ContainsString(instance.ObjectMeta.Finalizers, zookeeperv1beta1.ZkFinalizer) && !zookeeperv1beta1.DisableFinalizer {
			instance.ObjectMeta.Finalizers = append(instance.ObjectMeta.Finalizers, zookeeperv1beta1.ZkFinalizer)
			if err = r.Client.Update(r.ctx, instance); err != nil {
				return err
			}
		}
		return r.cleanupOrphanPVCs(instance)
	} else {
		if zookeeperv1beta1.ContainsString(instance.ObjectMeta.Finalizers, zookeeperv1beta1.ZkFinalizer) {
			if err = r.cleanUpAllPVCs(instance); err != nil {
				return err
			}
			instance.ObjectMeta.Finalizers = zookeeperv1beta1.RemoveString(instance.ObjectMeta.Finalizers, zookeeperv1beta1.ZkFinalizer)
			if err = r.Client.Update(r.ctx, instance); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *ZookeeperClusterReconciler) getPVCCount(instance *zookeeperv1beta1.ZookeeperCluster) (pvcCount int, err error) {
	pvcList, err := r.getPVCList(instance)
	if err != nil {
		return -1, err
	}
	pvcCount = len(pvcList.Items)
	return pvcCount, nil
}

func (r *ZookeeperClusterReconciler) cleanupOrphanPVCs(instance *zookeeperv1beta1.ZookeeperCluster) (err error) {
	if instance.Status.ReadyReplicas == instance.Spec.Replicas {
		pvcCount, err := r.getPVCCount(instance)
		if err != nil {
			return err
		}
		r.Log.Info("cleanupOrphanPVCs", "PVC Count", pvcCount, "ReadyReplicas Count", instance.Status.ReadyReplicas)
		if pvcCount > int(instance.Spec.Replicas) {
			pvcList, err := r.getPVCList(instance)
			if err != nil {
				return err
			}
			for _, pvcItem := range pvcList.Items {
				if zookeeperv1beta1.IsPVCOrphan(pvcItem.Name, instance.Spec.Replicas) {
					r.deletePVC(pvcItem)
				}
			}
		}
	}
	return nil
}

func (r *ZookeeperClusterReconciler) getPVCList(instance *zookeeperv1beta1.ZookeeperCluster) (pvList corev1.PersistentVolumeClaimList, err error) {
	selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: map[string]string{"app": instance.GetName(), "uid": string(instance.UID)},
	})
	pvclistOps := &client.ListOptions{
		Namespace:     instance.Namespace,
		LabelSelector: selector,
	}
	pvcList := &corev1.PersistentVolumeClaimList{}
	err = r.Client.List(r.ctx, pvcList, pvclistOps)
	return *pvcList, err
}

func (r *ZookeeperClusterReconciler) cleanUpAllPVCs(instance *zookeeperv1beta1.ZookeeperCluster) (err error) {
	pvcList, err := r.getPVCList(instance)
	if err != nil {
		r.Log.Error(err, "cleanUpAllPVCs: error listing PVCs")
		return err
	}
	r.Log.Info("cleanUpAllPVCs", "instance.UID", instance.UID, "PVC Count", len(pvcList.Items))
	for _, pvcItem := range pvcList.Items {
		r.deletePVC(pvcItem)
	}
	return nil
}

func (r *ZookeeperClusterReconciler) deletePVC(pvcItem corev1.PersistentVolumeClaim) {
	pvcDelete := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcItem.Name,
			Namespace: pvcItem.Namespace,
		},
	}
	r.Log.Info("Deleting PVC", "With Name", pvcItem.Name)
	err := r.Client.Delete(r.ctx, pvcDelete)
	if err != nil {
		r.Log.Error(err, "Error deleting PVC.", "Name", pvcDelete.Name)
	}
}
