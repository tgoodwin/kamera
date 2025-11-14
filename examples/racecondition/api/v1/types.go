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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	Group   = "race.examples"
	Version = "v1"
)

var (
	// SchemeGroupVersion identifies the group and version for Toggle resources.
	SchemeGroupVersion = schema.GroupVersion{Group: Group, Version: Version}
	SchemeBuilder      = runtime.NewSchemeBuilder(addKnownTypes)
	AddToScheme        = SchemeBuilder.AddToScheme
)

type ToggleSpec struct {
	// Desired indicates which controller should end up owning the toggle.
	Desired string `json:"desired,omitempty"`
}

type ToggleStatus struct {
	Owner string `json:"owner,omitempty"`
}

type Toggle struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ToggleSpec   `json:"spec,omitempty"`
	Status ToggleStatus `json:"status,omitempty"`
}

func (t *Toggle) DeepCopyObject() runtime.Object {
	if t == nil {
		return nil
	}
	out := *t
	out.ObjectMeta = *t.ObjectMeta.DeepCopy()
	return &out
}

type ToggleList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Toggle `json:"items"`
}

func (tl *ToggleList) DeepCopyObject() runtime.Object {
	if tl == nil {
		return nil
	}
	out := *tl
	out.ListMeta = tl.ListMeta
	if tl.Items != nil {
		out.Items = make([]Toggle, len(tl.Items))
		copy(out.Items, tl.Items)
	}
	return &out
}

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion, &Toggle{}, &ToggleList{})
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
