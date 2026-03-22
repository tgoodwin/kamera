/**
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package zk

import (
	"fmt"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is group version used to register these objects
	GroupVersion = schema.GroupVersion{Group: "zookeeper.pravega.io", Version: "v1beta1"}

	// SchemeBuilder is used to add go types to the GroupVersionKind scheme
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	// AddToScheme adds the types in this group-version to the given scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

func init() {
	SchemeBuilder.Register(&ZookeeperCluster{}, &ZookeeperClusterList{})
}

const (
	DefaultZkContainerRepository = "pravega/zookeeper"
	DefaultZkContainerVersion    = "0.2.13"
	DefaultZkContainerPolicy     = "Always"
	DefaultTerminationGracePeriod = 30
	DefaultZookeeperCacheVolumeSize = "20Gi"

	DefaultReadinessProbeInitialDelaySeconds = 10
	DefaultReadinessProbePeriodSeconds        = 10
	DefaultReadinessProbeFailureThreshold     = 3
	DefaultReadinessProbeSuccessThreshold     = 1
	DefaultReadinessProbeTimeoutSeconds       = 10

	DefaultLivenessProbeInitialDelaySeconds = 10
	DefaultLivenessProbePeriodSeconds        = 10
	DefaultLivenessProbeFailureThreshold     = 3
	DefaultLivenessProbeTimeoutSeconds       = 10
)

// ZookeeperCluster is the Schema for the zookeeperclusters API
type ZookeeperCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ZookeeperClusterSpec   `json:"spec,omitempty"`
	Status ZookeeperClusterStatus `json:"status,omitempty"`
}

// WithDefaults set default values when not defined in the spec.
func (z *ZookeeperCluster) WithDefaults() bool {
	return z.Spec.withDefaults(z)
}

// ConfigMapName returns the name of the cluster config-map
func (z *ZookeeperCluster) ConfigMapName() string {
	return fmt.Sprintf("%s-configmap", z.GetName())
}

// GetKubernetesClusterDomain returns the cluster domain of kubernetes
func (z *ZookeeperCluster) GetKubernetesClusterDomain() string {
	if z.Spec.KubernetesClusterDomain == "" {
		return "cluster.local"
	}
	return z.Spec.KubernetesClusterDomain
}

// ZookeeperPorts returns a struct of ports
func (z *ZookeeperCluster) ZookeeperPorts() Ports {
	ports := Ports{}
	for _, p := range z.Spec.Ports {
		if p.Name == "client" {
			ports.Client = p.ContainerPort
		} else if p.Name == "quorum" {
			ports.Quorum = p.ContainerPort
		} else if p.Name == "leader-election" {
			ports.Leader = p.ContainerPort
		} else if p.Name == "metrics" {
			ports.Metrics = p.ContainerPort
		} else if p.Name == "admin-server" {
			ports.AdminServer = p.ContainerPort
		}
	}
	return ports
}

// GetClientServiceName returns the name of the client service for the cluster
func (z *ZookeeperCluster) GetClientServiceName() string {
	return fmt.Sprintf("%s-client", z.GetName())
}

// GetAdminServerServiceName returns the name of the admin server service for the cluster
func (z *ZookeeperCluster) GetAdminServerServiceName() string {
	return fmt.Sprintf("%s-admin-server", z.GetName())
}

func (z *ZookeeperCluster) GetTriggerRollingRestart() bool {
	return z.Spec.TriggerRollingRestart
}

func (z *ZookeeperCluster) SetTriggerRollingRestart(val bool) {
	z.Spec.TriggerRollingRestart = val
}

// Ports groups the ports for a zookeeper cluster node for easy access
type Ports struct {
	Client      int32
	Quorum      int32
	Leader      int32
	Metrics     int32
	AdminServer int32
}

// ZookeeperClusterSpec defines the desired state of ZookeeperCluster
type ZookeeperClusterSpec struct {
	Image                  ContainerImage           `json:"image,omitempty"`
	Labels                 map[string]string        `json:"labels,omitempty"`
	Replicas               int32                    `json:"replicas,omitempty"`
	Ports                  []v1.ContainerPort       `json:"ports,omitempty"`
	Pod                    PodPolicy                `json:"pod,omitempty"`
	AdminServerService     AdminServerServicePolicy `json:"adminServerService,omitempty"`
	ClientService          ClientServicePolicy      `json:"clientService,omitempty"`
	TriggerRollingRestart  bool                     `json:"triggerRollingRestart,omitempty"`
	HeadlessService        HeadlessServicePolicy    `json:"headlessService,omitempty"`
	StorageType            string                   `json:"storageType,omitempty"`
	Persistence            *Persistence             `json:"persistence,omitempty"`
	Ephemeral              *Ephemeral               `json:"ephemeral,omitempty"`
	Conf                   ZookeeperConfig          `json:"config,omitempty"`
	DomainName             string                   `json:"domainName,omitempty"`
	KubernetesClusterDomain string                  `json:"kubernetesClusterDomain,omitempty"`
	Containers             []v1.Container           `json:"containers,omitempty"`
	InitContainers         []v1.Container           `json:"initContainers,omitempty"`
	Volumes                []v1.Volume              `json:"volumes,omitempty"`
	VolumeMounts           []v1.VolumeMount         `json:"volumeMounts,omitempty"`
	Probes                 *Probes                  `json:"probes,omitempty"`
	MaxUnavailableReplicas int32                    `json:"maxUnavailableReplicas,omitempty"`
}

type Probes struct {
	ReadinessProbe *Probe `json:"readinessProbe,omitempty"`
	LivenessProbe  *Probe `json:"livenessProbe,omitempty"`
}

func (s *Probes) withDefaults() (changed bool) {
	if s.ReadinessProbe == nil {
		changed = true
		s.ReadinessProbe = &Probe{}
		s.ReadinessProbe.InitialDelaySeconds = DefaultReadinessProbeInitialDelaySeconds
		s.ReadinessProbe.PeriodSeconds = DefaultReadinessProbePeriodSeconds
		s.ReadinessProbe.FailureThreshold = DefaultReadinessProbeFailureThreshold
		s.ReadinessProbe.SuccessThreshold = DefaultReadinessProbeSuccessThreshold
		s.ReadinessProbe.TimeoutSeconds = DefaultReadinessProbeTimeoutSeconds
	}
	if s.LivenessProbe == nil {
		changed = true
		s.LivenessProbe = &Probe{}
		s.LivenessProbe.InitialDelaySeconds = DefaultLivenessProbeInitialDelaySeconds
		s.LivenessProbe.PeriodSeconds = DefaultLivenessProbePeriodSeconds
		s.LivenessProbe.FailureThreshold = DefaultLivenessProbeFailureThreshold
		s.LivenessProbe.TimeoutSeconds = DefaultLivenessProbeTimeoutSeconds
	}
	return changed
}

type Probe struct {
	InitialDelaySeconds int32 `json:"initialDelaySeconds"`
	PeriodSeconds       int32 `json:"periodSeconds"`
	FailureThreshold    int32 `json:"failureThreshold"`
	SuccessThreshold    int32 `json:"successThreshold"`
	TimeoutSeconds      int32 `json:"timeoutSeconds"`
}

func (s *ZookeeperClusterSpec) withDefaults(z *ZookeeperCluster) (changed bool) {
	changed = s.Image.withDefaults()
	if s.Conf.withDefaults() {
		changed = true
	}
	if s.Replicas == 0 {
		s.Replicas = 3
		changed = true
	}
	if s.Probes == nil {
		changed = true
		s.Probes = &Probes{}
	}
	if s.Probes.withDefaults() {
		changed = true
	}
	if s.Ports == nil {
		s.Ports = []v1.ContainerPort{
			{Name: "client", ContainerPort: 2181},
			{Name: "quorum", ContainerPort: 2888},
			{Name: "leader-election", ContainerPort: 3888},
			{Name: "metrics", ContainerPort: 7000},
			{Name: "admin-server", ContainerPort: 8080},
		}
		changed = true
	} else {
		var foundClient, foundQuorum, foundLeader, foundMetrics, foundAdmin bool
		for i := 0; i < len(s.Ports); i++ {
			if s.Ports[i].Name == "client" {
				foundClient = true
			} else if s.Ports[i].Name == "quorum" {
				foundQuorum = true
			} else if s.Ports[i].Name == "leader-election" {
				foundLeader = true
			} else if s.Ports[i].Name == "metrics" {
				foundMetrics = true
			} else if s.Ports[i].Name == "admin-server" {
				foundAdmin = true
			}
		}
		if !foundClient {
			s.Ports = append(s.Ports, v1.ContainerPort{Name: "client", ContainerPort: 2181})
			changed = true
		}
		if !foundQuorum {
			s.Ports = append(s.Ports, v1.ContainerPort{Name: "quorum", ContainerPort: 2888})
			changed = true
		}
		if !foundLeader {
			s.Ports = append(s.Ports, v1.ContainerPort{Name: "leader-election", ContainerPort: 3888})
			changed = true
		}
		if !foundMetrics {
			s.Ports = append(s.Ports, v1.ContainerPort{Name: "metrics", ContainerPort: 7000})
			changed = true
		}
		if !foundAdmin {
			s.Ports = append(s.Ports, v1.ContainerPort{Name: "admin-server", ContainerPort: 8080})
			changed = true
		}
	}

	if z.Spec.Labels == nil {
		z.Spec.Labels = map[string]string{}
		changed = true
	}
	if _, ok := z.Spec.Labels["app"]; !ok {
		z.Spec.Labels["app"] = z.GetName()
		changed = true
	}
	if _, ok := z.Spec.Labels["release"]; !ok {
		z.Spec.Labels["release"] = z.GetName()
		changed = true
	}
	if s.Pod.withDefaults(z) {
		changed = true
	}
	if strings.EqualFold(s.StorageType, "ephemeral") {
		if s.Ephemeral == nil {
			s.Ephemeral = &Ephemeral{}
			s.Ephemeral.EmptyDirVolumeSource = v1.EmptyDirVolumeSource{}
			changed = true
		}
	} else {
		if s.Persistence == nil {
			s.StorageType = "persistence"
			s.Persistence = &Persistence{}
			changed = true
		}
		if s.Persistence.withDefaults() {
			s.StorageType = "persistence"
			changed = true
		}
	}
	if s.MaxUnavailableReplicas < 1 {
		s.MaxUnavailableReplicas = 1
		changed = true
	}
	return changed
}

// ContainerImage defines the fields needed for a Docker repository image.
type ContainerImage struct {
	Repository string        `json:"repository,omitempty"`
	Tag        string        `json:"tag,omitempty"`
	PullPolicy v1.PullPolicy `json:"pullPolicy,omitempty"`
}

func (c *ContainerImage) withDefaults() (changed bool) {
	if c.Repository == "" {
		changed = true
		c.Repository = DefaultZkContainerRepository
	}
	if c.Tag == "" {
		changed = true
		c.Tag = DefaultZkContainerVersion
	}
	if c.PullPolicy == "" {
		changed = true
		c.PullPolicy = DefaultZkContainerPolicy
	}
	return changed
}

func (c *ContainerImage) ToString() string {
	return fmt.Sprintf("%s:%s", c.Repository, c.Tag)
}

// PodPolicy defines the common pod configuration for Pods.
type PodPolicy struct {
	Labels                        map[string]string          `json:"labels,omitempty"`
	NodeSelector                  map[string]string          `json:"nodeSelector,omitempty"`
	Affinity                      *v1.Affinity               `json:"affinity,omitempty"`
	Resources                     v1.ResourceRequirements    `json:"resources,omitempty"`
	Tolerations                   []v1.Toleration            `json:"tolerations,omitempty"`
	Env                           []v1.EnvVar                `json:"env,omitempty"`
	Annotations                   map[string]string          `json:"annotations,omitempty"`
	SecurityContext               *v1.PodSecurityContext      `json:"securityContext,omitempty"`
	TerminationGracePeriodSeconds int64                      `json:"terminationGracePeriodSeconds,omitempty"`
	ServiceAccountName            string                     `json:"serviceAccountName,omitempty"`
	ImagePullSecrets              []v1.LocalObjectReference  `json:"imagePullSecrets,omitempty"`
}

func (p *PodPolicy) withDefaults(z *ZookeeperCluster) (changed bool) {
	if p.Labels == nil {
		p.Labels = map[string]string{}
		changed = true
	}
	if p.TerminationGracePeriodSeconds == 0 {
		p.TerminationGracePeriodSeconds = DefaultTerminationGracePeriod
		changed = true
	}
	if p.ServiceAccountName == "" {
		p.ServiceAccountName = "default"
		changed = true
	}
	if z.Spec.Pod.Labels == nil {
		p.Labels = map[string]string{}
		changed = true
	}
	if _, ok := p.Labels["app"]; !ok {
		p.Labels["app"] = z.GetName()
		changed = true
	}
	if _, ok := p.Labels["release"]; !ok {
		p.Labels["release"] = z.GetName()
		changed = true
	}
	if p.Affinity == nil {
		p.Affinity = &v1.Affinity{
			PodAntiAffinity: &v1.PodAntiAffinity{
				PreferredDuringSchedulingIgnoredDuringExecution: []v1.WeightedPodAffinityTerm{
					{
						Weight: 20,
						PodAffinityTerm: v1.PodAffinityTerm{
							TopologyKey: "kubernetes.io/hostname",
							LabelSelector: &metav1.LabelSelector{
								MatchExpressions: []metav1.LabelSelectorRequirement{
									{
										Key:      "app",
										Operator: metav1.LabelSelectorOpIn,
										Values:   []string{z.GetName()},
									},
								},
							},
						},
					},
				},
			},
		}
		changed = true
	}
	return changed
}

type AdminServerServicePolicy struct {
	Annotations map[string]string `json:"annotations,omitempty"`
	External    bool              `json:"external,omitempty"`
}

type ClientServicePolicy struct {
	Annotations map[string]string `json:"annotations,omitempty"`
}

type HeadlessServicePolicy struct {
	Annotations map[string]string `json:"annotations,omitempty"`
}

// ZookeeperConfig is the current configuration of each Zookeeper node.
type ZookeeperConfig struct {
	InitLimit                int               `json:"initLimit,omitempty"`
	TickTime                 int               `json:"tickTime,omitempty"`
	SyncLimit                int               `json:"syncLimit,omitempty"`
	GlobalOutstandingLimit   int               `json:"globalOutstandingLimit,omitempty"`
	PreAllocSize             int               `json:"preAllocSize,omitempty"`
	SnapCount                int               `json:"snapCount,omitempty"`
	CommitLogCount           int               `json:"commitLogCount,omitempty"`
	SnapSizeLimitInKb        int               `json:"snapSizeLimitInKb,omitempty"`
	MaxCnxns                 int               `json:"maxCnxns,omitempty"`
	MaxClientCnxns           int               `json:"maxClientCnxns,omitempty"`
	MinSessionTimeout        int               `json:"minSessionTimeout,omitempty"`
	MaxSessionTimeout        int               `json:"maxSessionTimeout,omitempty"`
	AutoPurgeSnapRetainCount int               `json:"autoPurgeSnapRetainCount,omitempty"`
	AutoPurgePurgeInterval   int               `json:"autoPurgePurgeInterval,omitempty"`
	QuorumListenOnAllIPs     bool              `json:"quorumListenOnAllIPs,omitempty"`
	AdditionalConfig         map[string]string `json:"additionalConfig,omitempty"`
}

func (c *ZookeeperConfig) withDefaults() (changed bool) {
	if c.InitLimit == 0 {
		changed = true
		c.InitLimit = 10
	}
	if c.TickTime == 0 {
		changed = true
		c.TickTime = 2000
	}
	if c.SyncLimit == 0 {
		changed = true
		c.SyncLimit = 2
	}
	if c.GlobalOutstandingLimit == 0 {
		changed = true
		c.GlobalOutstandingLimit = 1000
	}
	if c.PreAllocSize == 0 {
		changed = true
		c.PreAllocSize = 65536
	}
	if c.SnapCount == 0 {
		changed = true
		c.SnapCount = 10000
	}
	if c.CommitLogCount == 0 {
		changed = true
		c.CommitLogCount = 500
	}
	if c.SnapSizeLimitInKb == 0 {
		changed = true
		c.SnapSizeLimitInKb = 4194304
	}
	if c.MaxClientCnxns == 0 {
		changed = true
		c.MaxClientCnxns = 60
	}
	if c.MinSessionTimeout == 0 {
		changed = true
		c.MinSessionTimeout = 2 * c.TickTime
	}
	if c.MaxSessionTimeout == 0 {
		changed = true
		c.MaxSessionTimeout = 20 * c.TickTime
	}
	if c.AutoPurgeSnapRetainCount == 0 {
		changed = true
		c.AutoPurgeSnapRetainCount = 3
	}
	if c.AutoPurgePurgeInterval == 0 {
		changed = true
		c.AutoPurgePurgeInterval = 1
	}
	return changed
}

type Persistence struct {
	VolumeReclaimPolicy       VolumeReclaimPolicy            `json:"reclaimPolicy,omitempty"`
	PersistentVolumeClaimSpec v1.PersistentVolumeClaimSpec    `json:"spec,omitempty"`
	Annotations               map[string]string              `json:"annotations,omitempty"`
}

type Ephemeral struct {
	EmptyDirVolumeSource v1.EmptyDirVolumeSource `json:"emptydirvolumesource,omitempty"`
}

func (p *Persistence) withDefaults() (changed bool) {
	if !p.VolumeReclaimPolicy.isValid() {
		changed = true
		p.VolumeReclaimPolicy = VolumeReclaimPolicyRetain
	}
	p.PersistentVolumeClaimSpec.AccessModes = []v1.PersistentVolumeAccessMode{
		v1.ReadWriteOnce,
	}
	storage, _ := p.PersistentVolumeClaimSpec.Resources.Requests["storage"]
	if storage.IsZero() {
		p.PersistentVolumeClaimSpec.Resources.Requests = v1.ResourceList{
			v1.ResourceStorage: resource.MustParse(DefaultZookeeperCacheVolumeSize),
		}
		changed = true
	}
	return changed
}

func (v VolumeReclaimPolicy) isValid() bool {
	return v == VolumeReclaimPolicyDelete || v == VolumeReclaimPolicyRetain
}

type VolumeReclaimPolicy string

const (
	VolumeReclaimPolicyRetain VolumeReclaimPolicy = "Retain"
	VolumeReclaimPolicyDelete VolumeReclaimPolicy = "Delete"
)

// --- Status types ---

type ClusterConditionType string

const (
	ClusterConditionPodsReady ClusterConditionType = "PodsReady"
	ClusterConditionUpgrading ClusterConditionType = "Upgrading"
	ClusterConditionError     ClusterConditionType = "Error"

	UpdatingZookeeperReason = "Updating Zookeeper"
	UpgradeErrorReason      = "Upgrade Error"
)

// ZookeeperClusterStatus defines the observed state of ZookeeperCluster
type ZookeeperClusterStatus struct {
	Members                MembersStatus      `json:"members,omitempty"`
	Replicas               int32              `json:"replicas,omitempty"`
	ReadyReplicas          int32              `json:"readyReplicas,omitempty"`
	InternalClientEndpoint string             `json:"internalClientEndpoint,omitempty"`
	ExternalClientEndpoint string             `json:"externalClientEndpoint,omitempty"`
	MetaRootCreated        bool               `json:"metaRootCreated,omitempty"`
	CurrentVersion         string             `json:"currentVersion,omitempty"`
	TargetVersion          string             `json:"targetVersion,omitempty"`
	Conditions             []ClusterCondition `json:"conditions,omitempty"`
}

type MembersStatus struct {
	Ready   []string `json:"ready,omitempty"`
	Unready []string `json:"unready,omitempty"`
}

type ClusterCondition struct {
	Type               ClusterConditionType `json:"type,omitempty"`
	Status             v1.ConditionStatus   `json:"status,omitempty"`
	Reason             string               `json:"reason,omitempty"`
	Message            string               `json:"message,omitempty"`
	LastUpdateTime     string               `json:"lastUpdateTime,omitempty"`
	LastTransitionTime string               `json:"lastTransitionTime,omitempty"`
}

func (zs *ZookeeperClusterStatus) Init() {
	conditionTypes := []ClusterConditionType{
		ClusterConditionPodsReady,
		ClusterConditionUpgrading,
		ClusterConditionError,
	}
	for _, conditionType := range conditionTypes {
		if _, condition := zs.GetClusterCondition(conditionType); condition == nil {
			c := newClusterCondition(conditionType, v1.ConditionFalse, "", "")
			zs.setClusterCondition(*c)
		}
	}
}

func newClusterCondition(condType ClusterConditionType, status v1.ConditionStatus, reason, message string) *ClusterCondition {
	return &ClusterCondition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastUpdateTime:     "",
		LastTransitionTime: "",
	}
}

func (zs *ZookeeperClusterStatus) SetPodsReadyConditionTrue() {
	c := newClusterCondition(ClusterConditionPodsReady, v1.ConditionTrue, "", "")
	zs.setClusterCondition(*c)
}

func (zs *ZookeeperClusterStatus) SetPodsReadyConditionFalse() {
	c := newClusterCondition(ClusterConditionPodsReady, v1.ConditionFalse, "", "")
	zs.setClusterCondition(*c)
}

func (zs *ZookeeperClusterStatus) SetUpgradingConditionTrue(reason, message string) {
	c := newClusterCondition(ClusterConditionUpgrading, v1.ConditionTrue, reason, message)
	zs.setClusterCondition(*c)
}

func (zs *ZookeeperClusterStatus) SetUpgradingConditionFalse() {
	c := newClusterCondition(ClusterConditionUpgrading, v1.ConditionFalse, "", "")
	zs.setClusterCondition(*c)
}

func (zs *ZookeeperClusterStatus) SetErrorConditionTrue(reason, message string) {
	c := newClusterCondition(ClusterConditionError, v1.ConditionTrue, reason, message)
	zs.setClusterCondition(*c)
}

func (zs *ZookeeperClusterStatus) SetErrorConditionFalse() {
	c := newClusterCondition(ClusterConditionError, v1.ConditionFalse, "", "")
	zs.setClusterCondition(*c)
}

func (zs *ZookeeperClusterStatus) GetClusterCondition(t ClusterConditionType) (int, *ClusterCondition) {
	for i, c := range zs.Conditions {
		if t == c.Type {
			return i, &c
		}
	}
	return -1, nil
}

func (zs *ZookeeperClusterStatus) setClusterCondition(newCondition ClusterCondition) {
	now := time.Now().Format(time.RFC3339)
	position, existingCondition := zs.GetClusterCondition(newCondition.Type)

	if existingCondition == nil {
		zs.Conditions = append(zs.Conditions, newCondition)
		return
	}

	if existingCondition.Status != newCondition.Status {
		existingCondition.Status = newCondition.Status
		existingCondition.LastTransitionTime = now
		existingCondition.LastUpdateTime = now
	}

	if existingCondition.Reason != newCondition.Reason || existingCondition.Message != newCondition.Message {
		existingCondition.Reason = newCondition.Reason
		existingCondition.Message = newCondition.Message
		existingCondition.LastUpdateTime = now
	}

	zs.Conditions[position] = *existingCondition
}

func (zs *ZookeeperClusterStatus) IsClusterInUpgradeFailedState() bool {
	_, errorCondition := zs.GetClusterCondition(ClusterConditionError)
	if errorCondition == nil {
		return false
	}
	if errorCondition.Status == v1.ConditionTrue && errorCondition.Reason == "UpgradeFailed" {
		return true
	}
	return false
}

func (zs *ZookeeperClusterStatus) IsClusterInUpgradingState() bool {
	_, upgradeCondition := zs.GetClusterCondition(ClusterConditionUpgrading)
	if upgradeCondition == nil {
		return false
	}
	if upgradeCondition.Status == v1.ConditionTrue {
		return true
	}
	return false
}

func (zs *ZookeeperClusterStatus) IsClusterInReadyState() bool {
	_, readyCondition := zs.GetClusterCondition(ClusterConditionPodsReady)
	if readyCondition != nil && readyCondition.Status == v1.ConditionTrue {
		return true
	}
	return false
}

func (zs *ZookeeperClusterStatus) UpdateProgress(reason, updatedReplicas string) {
	if zs.IsClusterInUpgradingState() {
		zs.SetUpgradingConditionTrue(reason, updatedReplicas)
	}
}

func (zs *ZookeeperClusterStatus) GetLastCondition() (lastCondition *ClusterCondition) {
	if zs.IsClusterInUpgradingState() {
		_, lastCondition := zs.GetClusterCondition(ClusterConditionUpgrading)
		return lastCondition
	}
	return nil
}

// ZookeeperClusterList contains a list of ZookeeperCluster
type ZookeeperClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ZookeeperCluster `json:"items"`
}

// --- DeepCopy methods ---

func (in *AdminServerServicePolicy) DeepCopyInto(out *AdminServerServicePolicy) {
	*out = *in
	if in.Annotations != nil {
		in, out := &in.Annotations, &out.Annotations
		*out = make(map[string]string, len(*in))
		for key, val := range *in {
			(*out)[key] = val
		}
	}
}

func (in *AdminServerServicePolicy) DeepCopy() *AdminServerServicePolicy {
	if in == nil {
		return nil
	}
	out := new(AdminServerServicePolicy)
	in.DeepCopyInto(out)
	return out
}

func (in *ClientServicePolicy) DeepCopyInto(out *ClientServicePolicy) {
	*out = *in
	if in.Annotations != nil {
		in, out := &in.Annotations, &out.Annotations
		*out = make(map[string]string, len(*in))
		for key, val := range *in {
			(*out)[key] = val
		}
	}
}

func (in *ClientServicePolicy) DeepCopy() *ClientServicePolicy {
	if in == nil {
		return nil
	}
	out := new(ClientServicePolicy)
	in.DeepCopyInto(out)
	return out
}

func (in *ClusterCondition) DeepCopyInto(out *ClusterCondition) {
	*out = *in
}

func (in *ClusterCondition) DeepCopy() *ClusterCondition {
	if in == nil {
		return nil
	}
	out := new(ClusterCondition)
	in.DeepCopyInto(out)
	return out
}

func (in *ContainerImage) DeepCopyInto(out *ContainerImage) {
	*out = *in
}

func (in *ContainerImage) DeepCopy() *ContainerImage {
	if in == nil {
		return nil
	}
	out := new(ContainerImage)
	in.DeepCopyInto(out)
	return out
}

func (in *Ephemeral) DeepCopyInto(out *Ephemeral) {
	*out = *in
	in.EmptyDirVolumeSource.DeepCopyInto(&out.EmptyDirVolumeSource)
}

func (in *Ephemeral) DeepCopy() *Ephemeral {
	if in == nil {
		return nil
	}
	out := new(Ephemeral)
	in.DeepCopyInto(out)
	return out
}

func (in *HeadlessServicePolicy) DeepCopyInto(out *HeadlessServicePolicy) {
	*out = *in
	if in.Annotations != nil {
		in, out := &in.Annotations, &out.Annotations
		*out = make(map[string]string, len(*in))
		for key, val := range *in {
			(*out)[key] = val
		}
	}
}

func (in *HeadlessServicePolicy) DeepCopy() *HeadlessServicePolicy {
	if in == nil {
		return nil
	}
	out := new(HeadlessServicePolicy)
	in.DeepCopyInto(out)
	return out
}

func (in *MembersStatus) DeepCopyInto(out *MembersStatus) {
	*out = *in
	if in.Ready != nil {
		in, out := &in.Ready, &out.Ready
		*out = make([]string, len(*in))
		copy(*out, *in)
	}
	if in.Unready != nil {
		in, out := &in.Unready, &out.Unready
		*out = make([]string, len(*in))
		copy(*out, *in)
	}
}

func (in *MembersStatus) DeepCopy() *MembersStatus {
	if in == nil {
		return nil
	}
	out := new(MembersStatus)
	in.DeepCopyInto(out)
	return out
}

func (in *Persistence) DeepCopyInto(out *Persistence) {
	*out = *in
	in.PersistentVolumeClaimSpec.DeepCopyInto(&out.PersistentVolumeClaimSpec)
	if in.Annotations != nil {
		in, out := &in.Annotations, &out.Annotations
		*out = make(map[string]string, len(*in))
		for key, val := range *in {
			(*out)[key] = val
		}
	}
}

func (in *Persistence) DeepCopy() *Persistence {
	if in == nil {
		return nil
	}
	out := new(Persistence)
	in.DeepCopyInto(out)
	return out
}

func (in *PodPolicy) DeepCopyInto(out *PodPolicy) {
	*out = *in
	if in.Labels != nil {
		in, out := &in.Labels, &out.Labels
		*out = make(map[string]string, len(*in))
		for key, val := range *in {
			(*out)[key] = val
		}
	}
	if in.NodeSelector != nil {
		in, out := &in.NodeSelector, &out.NodeSelector
		*out = make(map[string]string, len(*in))
		for key, val := range *in {
			(*out)[key] = val
		}
	}
	if in.Affinity != nil {
		in, out := &in.Affinity, &out.Affinity
		*out = new(v1.Affinity)
		(*in).DeepCopyInto(*out)
	}
	in.Resources.DeepCopyInto(&out.Resources)
	if in.Tolerations != nil {
		in, out := &in.Tolerations, &out.Tolerations
		*out = make([]v1.Toleration, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	if in.Env != nil {
		in, out := &in.Env, &out.Env
		*out = make([]v1.EnvVar, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	if in.Annotations != nil {
		in, out := &in.Annotations, &out.Annotations
		*out = make(map[string]string, len(*in))
		for key, val := range *in {
			(*out)[key] = val
		}
	}
	if in.SecurityContext != nil {
		in, out := &in.SecurityContext, &out.SecurityContext
		*out = new(v1.PodSecurityContext)
		(*in).DeepCopyInto(*out)
	}
	if in.ImagePullSecrets != nil {
		in, out := &in.ImagePullSecrets, &out.ImagePullSecrets
		*out = make([]v1.LocalObjectReference, len(*in))
		copy(*out, *in)
	}
}

func (in *PodPolicy) DeepCopy() *PodPolicy {
	if in == nil {
		return nil
	}
	out := new(PodPolicy)
	in.DeepCopyInto(out)
	return out
}

func (in *Ports) DeepCopyInto(out *Ports) {
	*out = *in
}

func (in *Ports) DeepCopy() *Ports {
	if in == nil {
		return nil
	}
	out := new(Ports)
	in.DeepCopyInto(out)
	return out
}

func (in *Probe) DeepCopyInto(out *Probe) {
	*out = *in
}

func (in *Probe) DeepCopy() *Probe {
	if in == nil {
		return nil
	}
	out := new(Probe)
	in.DeepCopyInto(out)
	return out
}

func (in *Probes) DeepCopyInto(out *Probes) {
	*out = *in
	if in.ReadinessProbe != nil {
		in, out := &in.ReadinessProbe, &out.ReadinessProbe
		*out = new(Probe)
		**out = **in
	}
	if in.LivenessProbe != nil {
		in, out := &in.LivenessProbe, &out.LivenessProbe
		*out = new(Probe)
		**out = **in
	}
}

func (in *Probes) DeepCopy() *Probes {
	if in == nil {
		return nil
	}
	out := new(Probes)
	in.DeepCopyInto(out)
	return out
}

func (in *ZookeeperCluster) DeepCopyInto(out *ZookeeperCluster) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

func (in *ZookeeperCluster) DeepCopy() *ZookeeperCluster {
	if in == nil {
		return nil
	}
	out := new(ZookeeperCluster)
	in.DeepCopyInto(out)
	return out
}

func (in *ZookeeperCluster) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *ZookeeperClusterList) DeepCopyInto(out *ZookeeperClusterList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]ZookeeperCluster, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

func (in *ZookeeperClusterList) DeepCopy() *ZookeeperClusterList {
	if in == nil {
		return nil
	}
	out := new(ZookeeperClusterList)
	in.DeepCopyInto(out)
	return out
}

func (in *ZookeeperClusterList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *ZookeeperClusterSpec) DeepCopyInto(out *ZookeeperClusterSpec) {
	*out = *in
	out.Image = in.Image
	if in.Labels != nil {
		in, out := &in.Labels, &out.Labels
		*out = make(map[string]string, len(*in))
		for key, val := range *in {
			(*out)[key] = val
		}
	}
	if in.Ports != nil {
		in, out := &in.Ports, &out.Ports
		*out = make([]v1.ContainerPort, len(*in))
		copy(*out, *in)
	}
	in.Pod.DeepCopyInto(&out.Pod)
	in.AdminServerService.DeepCopyInto(&out.AdminServerService)
	in.ClientService.DeepCopyInto(&out.ClientService)
	in.HeadlessService.DeepCopyInto(&out.HeadlessService)
	if in.Persistence != nil {
		in, out := &in.Persistence, &out.Persistence
		*out = new(Persistence)
		(*in).DeepCopyInto(*out)
	}
	if in.Ephemeral != nil {
		in, out := &in.Ephemeral, &out.Ephemeral
		*out = new(Ephemeral)
		(*in).DeepCopyInto(*out)
	}
	in.Conf.DeepCopyInto(&out.Conf)
	if in.Containers != nil {
		in, out := &in.Containers, &out.Containers
		*out = make([]v1.Container, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	if in.InitContainers != nil {
		in, out := &in.InitContainers, &out.InitContainers
		*out = make([]v1.Container, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	if in.Volumes != nil {
		in, out := &in.Volumes, &out.Volumes
		*out = make([]v1.Volume, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	if in.VolumeMounts != nil {
		in, out := &in.VolumeMounts, &out.VolumeMounts
		*out = make([]v1.VolumeMount, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	if in.Probes != nil {
		in, out := &in.Probes, &out.Probes
		*out = new(Probes)
		(*in).DeepCopyInto(*out)
	}
}

func (in *ZookeeperClusterSpec) DeepCopy() *ZookeeperClusterSpec {
	if in == nil {
		return nil
	}
	out := new(ZookeeperClusterSpec)
	in.DeepCopyInto(out)
	return out
}

func (in *ZookeeperClusterStatus) DeepCopyInto(out *ZookeeperClusterStatus) {
	*out = *in
	in.Members.DeepCopyInto(&out.Members)
	if in.Conditions != nil {
		in, out := &in.Conditions, &out.Conditions
		*out = make([]ClusterCondition, len(*in))
		copy(*out, *in)
	}
}

func (in *ZookeeperClusterStatus) DeepCopy() *ZookeeperClusterStatus {
	if in == nil {
		return nil
	}
	out := new(ZookeeperClusterStatus)
	in.DeepCopyInto(out)
	return out
}

func (in *ZookeeperConfig) DeepCopyInto(out *ZookeeperConfig) {
	*out = *in
	if in.AdditionalConfig != nil {
		in, out := &in.AdditionalConfig, &out.AdditionalConfig
		*out = make(map[string]string, len(*in))
		for key, val := range *in {
			(*out)[key] = val
		}
	}
}

func (in *ZookeeperConfig) DeepCopy() *ZookeeperConfig {
	if in == nil {
		return nil
	}
	out := new(ZookeeperConfig)
	in.DeepCopyInto(out)
	return out
}
