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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// OrchestrationSpec defines the desired state of Orchestration
type OrchestrationSpec struct {
	// User specifies target services to deploy
	Services []string `json:"services"`
	// User specifies deployment strategy
	Strategy string `json:"strategy"` // "rolling", "blue-green", etc.
}

type ServiceState struct {
	Name     string `json:"name"`
	Version  string `json:"version"`
	Replicas int    `json:"replicas"`
	Health   string `json:"health"` // "healthy", "degraded", "failed"
}

// OrchestrationStatus defines the observed state of Orchestration
type OrchestrationStatus struct {
	// Current state of each service
	ServiceStates []ServiceState `json:"serviceStates"`
	// Overall deployment phase
	Phase string `json:"phase"` // "preparing", "in-progress", "completed", "failed"
	// Deployment progress percentage
	Progress int `json:"progress"`

	HealthCheckAttempts *int `json:"healthCheckAttempts"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// Orchestration is the Schema for the orchestrations API
type Orchestration struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   OrchestrationSpec   `json:"spec,omitempty"`
	Status OrchestrationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// OrchestrationList contains a list of Orchestration
type OrchestrationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Orchestration `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Orchestration{}, &OrchestrationList{})
}
