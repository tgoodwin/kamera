// RabbitMQ Cluster Operator types adapted for Kamera harness.
// Original source: github.com/rabbitmq/cluster-operator (commit 4f13b9a)

package rmq

import (
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8sresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	GroupVersion  = schema.GroupVersion{Group: "rabbitmq.com", Version: "v1beta1"}
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}
	AddToScheme   = SchemeBuilder.AddToScheme
)

// +kubebuilder:object:root=true
type RabbitmqCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              RabbitmqClusterSpec   `json:"spec,omitempty"`
	Status            RabbitmqClusterStatus `json:"status,omitempty"`
}

type RabbitmqClusterSpec struct {
	Replicas                      *int32                       `json:"replicas,omitempty"`
	Image                         string                       `json:"image,omitempty"`
	ImagePullSecrets              []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`
	Service                       RabbitmqClusterServiceSpec   `json:"service,omitempty"`
	Persistence                   RabbitmqClusterPersistenceSpec `json:"persistence,omitempty"`
	Resources                     *corev1.ResourceRequirements `json:"resources,omitempty"`
	Affinity                      *corev1.Affinity             `json:"affinity,omitempty"`
	Tolerations                   []corev1.Toleration          `json:"tolerations,omitempty"`
	Rabbitmq                      RabbitmqClusterConfigurationSpec `json:"rabbitmq,omitempty"`
	TLS                           TLSSpec                      `json:"tls,omitempty"`
	Override                      RabbitmqClusterOverrideSpec  `json:"override,omitempty"`
	SkipPostDeploySteps           bool                         `json:"skipPostDeploySteps,omitempty"`
	TerminationGracePeriodSeconds *int64                       `json:"terminationGracePeriodSeconds,omitempty"`
}

type RabbitmqClusterOverrideSpec struct {
	StatefulSet *StatefulSet `json:"statefulSet,omitempty"`
	Service     *Service     `json:"service,omitempty"`
}

type Service struct {
	*EmbeddedLabelsAnnotations `json:"metadata,omitempty"`
	Spec                       *corev1.ServiceSpec `json:"spec,omitempty"`
}

type StatefulSet struct {
	*EmbeddedLabelsAnnotations `json:"metadata,omitempty"`
	Spec                       *StatefulSetSpec `json:"spec,omitempty"`
}

type StatefulSetSpec struct {
	Replicas            *int32                              `json:"replicas,omitempty"`
	Selector            *metav1.LabelSelector               `json:"selector,omitempty"`
	Template            *PodTemplateSpec                    `json:"template,omitempty"`
	VolumeClaimTemplates []PersistentVolumeClaim            `json:"volumeClaimTemplates,omitempty"`
	ServiceName         string                              `json:"serviceName,omitempty"`
	PodManagementPolicy appsv1.PodManagementPolicyType      `json:"podManagementPolicy,omitempty"`
	UpdateStrategy      *appsv1.StatefulSetUpdateStrategy   `json:"updateStrategy,omitempty"`
}

type EmbeddedLabelsAnnotations struct {
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

type EmbeddedObjectMeta struct {
	Name        string            `json:"name,omitempty"`
	Namespace   string            `json:"namespace,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

type PodTemplateSpec struct {
	*EmbeddedObjectMeta `json:"metadata,omitempty"`
	Spec                *corev1.PodSpec `json:"spec,omitempty"`
}

type PersistentVolumeClaim struct {
	metav1.TypeMeta    `json:",inline"`
	EmbeddedObjectMeta `json:"metadata,omitempty"`
	Spec               corev1.PersistentVolumeClaimSpec `json:"spec,omitempty"`
}

type TLSSpec struct {
	SecretName            string `json:"secretName,omitempty"`
	CaSecretName          string `json:"caSecretName,omitempty"`
	DisableNonTLSListeners bool  `json:"disableNonTLSListeners,omitempty"`
}

type Plugin string

type RabbitmqClusterConfigurationSpec struct {
	AdditionalPlugins []Plugin `json:"additionalPlugins,omitempty"`
	AdditionalConfig  string   `json:"additionalConfig,omitempty"`
	AdvancedConfig    string   `json:"advancedConfig,omitempty"`
	EnvConfig         string   `json:"envConfig,omitempty"`
}

type RabbitmqClusterPersistenceSpec struct {
	StorageClassName *string              `json:"storageClassName,omitempty"`
	Storage          *k8sresource.Quantity `json:"storage,omitempty"`
}

type RabbitmqClusterServiceSpec struct {
	Type        corev1.ServiceType `json:"type,omitempty"`
	Annotations map[string]string  `json:"annotations,omitempty"`
}

// Status types

type RabbitmqClusterStatus struct {
	Conditions  []RabbitmqClusterCondition    `json:"conditions"`
	DefaultUser *RabbitmqClusterDefaultUser   `json:"defaultUser,omitempty"`
	Binding     *corev1.LocalObjectReference  `json:"binding,omitempty"`
}

type RabbitmqClusterConditionType string

const (
	AllReplicasReady RabbitmqClusterConditionType = "AllReplicasReady"
	ClusterAvailable RabbitmqClusterConditionType = "ClusterAvailable"
	NoWarnings       RabbitmqClusterConditionType = "NoWarnings"
	ReconcileSuccess RabbitmqClusterConditionType = "ReconcileSuccess"
)

type RabbitmqClusterCondition struct {
	Type               RabbitmqClusterConditionType `json:"type"`
	Status             corev1.ConditionStatus       `json:"status"`
	LastTransitionTime metav1.Time                  `json:"lastTransitionTime,omitempty"`
	Reason             string                       `json:"reason,omitempty"`
	Message            string                       `json:"message,omitempty"`
}

func (c *RabbitmqClusterCondition) DeepCopy() *RabbitmqClusterCondition {
	if c == nil {
		return nil
	}
	out := *c
	out.LastTransitionTime = *c.LastTransitionTime.DeepCopy()
	return &out
}

// stableTime is a fixed reference timestamp used for condition transitions
// in simulation to avoid infinite reconcile loops from time.Now() drift.
var stableTime = metav1.Time{Time: time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC)}

func (c *RabbitmqClusterCondition) UpdateState(status corev1.ConditionStatus) {
	if c.Status != status {
		c.LastTransitionTime = stableTime
	}
	c.Status = status
}

func (c *RabbitmqClusterCondition) UpdateReason(reason string, messages ...string) {
	c.Reason = reason
	c.Message = strings.Join(messages, ". ")
}

type RabbitmqClusterDefaultUser struct {
	SecretReference  *RabbitmqClusterSecretReference  `json:"secretReference,omitempty"`
	ServiceReference *RabbitmqClusterServiceReference `json:"serviceReference,omitempty"`
}

type RabbitmqClusterSecretReference struct {
	Name      string            `json:"name"`
	Namespace string            `json:"namespace"`
	Keys      map[string]string `json:"keys"`
}

type RabbitmqClusterServiceReference struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

// +kubebuilder:object:root=true
type RabbitmqClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RabbitmqCluster `json:"items"`
}

// Helper methods

func (cluster *RabbitmqCluster) TLSEnabled() bool {
	return cluster.Spec.TLS.SecretName != ""
}

func (cluster *RabbitmqCluster) MutualTLSEnabled() bool {
	return cluster.TLSEnabled() && cluster.Spec.TLS.CaSecretName != ""
}

func (cluster *RabbitmqCluster) MemoryLimited() bool {
	return cluster.Spec.Resources != nil && cluster.Spec.Resources.Limits != nil && !cluster.Spec.Resources.Limits.Memory().IsZero()
}

func (cluster *RabbitmqCluster) SingleTLSSecret() bool {
	return cluster.MutualTLSEnabled() && cluster.Spec.TLS.CaSecretName == cluster.Spec.TLS.SecretName
}

func (cluster *RabbitmqCluster) DisableNonTLSListeners() bool {
	return cluster.Spec.TLS.DisableNonTLSListeners
}

func (cluster *RabbitmqCluster) AdditionalPluginEnabled(plugin Plugin) bool {
	for _, p := range cluster.Spec.Rabbitmq.AdditionalPlugins {
		if p == plugin {
			return true
		}
	}
	return false
}

func (cluster RabbitmqCluster) ChildResourceName(name string) string {
	return strings.TrimSuffix(strings.Join([]string{cluster.Name, name}, "-"), "-")
}

func (cluster RabbitmqCluster) PVCName(i int) string {
	return strings.Join([]string{"persistence", cluster.Name, "server", strconv.Itoa(i)}, "-")
}

// SetConditions updates status conditions from child resources.
// Preserves existing LastTransitionTime when status hasn't changed to avoid
// infinite reconciliation loops in simulation (where time.Now() always differs).
func (s *RabbitmqClusterStatus) SetConditions(resources []runtime.Object) {
	oldByType := make(map[RabbitmqClusterConditionType]*RabbitmqClusterCondition)
	for i := range s.Conditions {
		c := s.Conditions[i].DeepCopy()
		oldByType[c.Type] = c
	}

	newConds := []RabbitmqClusterCondition{
		allReplicasReadyCondition(resources),
		clusterAvailableCondition(resources),
		noWarningsCondition(resources),
	}

	// Preserve timestamps for conditions whose status hasn't changed
	for i := range newConds {
		if old, ok := oldByType[newConds[i].Type]; ok {
			if old.Status == newConds[i].Status {
				newConds[i].LastTransitionTime = old.LastTransitionTime
			}
		}
	}

	var reconciledCondition RabbitmqClusterCondition
	if old, ok := oldByType[ReconcileSuccess]; ok {
		reconciledCondition = *old
	} else {
		reconciledCondition = RabbitmqClusterCondition{
			Type:               ReconcileSuccess,
			Status:             corev1.ConditionUnknown,
			LastTransitionTime: stableTime,
			Reason:             "Initialising",
		}
	}

	s.Conditions = append(newConds, reconciledCondition)
}

func (s *RabbitmqClusterStatus) SetCondition(condType RabbitmqClusterConditionType, condStatus corev1.ConditionStatus, reason string, messages ...string) {
	for i := range s.Conditions {
		if s.Conditions[i].Type == condType {
			s.Conditions[i].UpdateState(condStatus)
			s.Conditions[i].UpdateReason(reason, messages...)
			break
		}
	}
}

// Simplified status condition builders (inlined from internal/status)

func allReplicasReadyCondition(resources []runtime.Object) RabbitmqClusterCondition {
	condition := RabbitmqClusterCondition{Type: AllReplicasReady, Status: corev1.ConditionUnknown}
	for _, res := range resources {
		if sts, ok := res.(*appsv1.StatefulSet); ok {
			if sts == nil {
				condition.Reason = "MissingStatefulSet"
				return condition
			}
			var desired int32 = 1
			if sts.Spec.Replicas != nil {
				desired = *sts.Spec.Replicas
			}
			if desired == sts.Status.ReadyReplicas {
				condition.Status = corev1.ConditionTrue
				condition.Reason = "AllPodsAreReady"
			} else {
				condition.Status = corev1.ConditionFalse
				condition.Reason = "NotAllPodsReady"
			}
		}
	}
	return condition
}

func clusterAvailableCondition(resources []runtime.Object) RabbitmqClusterCondition {
	condition := RabbitmqClusterCondition{Type: ClusterAvailable, Status: corev1.ConditionUnknown}
	for _, res := range resources {
		if ep, ok := res.(*corev1.Endpoints); ok {
			if ep == nil {
				condition.Reason = "CouldNotRetrieveEndpoints"
				return condition
			}
			for _, subset := range ep.Subsets {
				if len(subset.Addresses) > 0 {
					condition.Status = corev1.ConditionTrue
					condition.Reason = "AtLeastOneEndpointAvailable"
					return condition
				}
			}
			condition.Status = corev1.ConditionFalse
			condition.Reason = "NoEndpointsAvailable"
		}
	}
	return condition
}

func noWarningsCondition(resources []runtime.Object) RabbitmqClusterCondition {
	condition := RabbitmqClusterCondition{Type: NoWarnings, Status: corev1.ConditionUnknown}
	for _, res := range resources {
		if sts, ok := res.(*appsv1.StatefulSet); ok {
			if sts == nil {
				condition.Reason = "MissingStatefulSet"
				return condition
			}
			condition.Status = corev1.ConditionTrue
			condition.Reason = "NoWarnings"
		}
	}
	return condition
}

func init() {
	SchemeBuilder.Register(&RabbitmqCluster{}, &RabbitmqClusterList{})
}
