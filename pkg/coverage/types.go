package coverage

import "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

// Input captures the concrete objects and pending reconciles for a single scenario seed.
type Input struct {
	Name    string                      `json:"name"`
	Objects []*unstructured.Unstructured `json:"objects"`
	Pending []Pending                   `json:"pending"`
	Tuning  InputTuning                 `json:"tuning"`
}

// Pending is a controller + namespaced key pair to enqueue for reconciliation.
type Pending struct {
	ControllerID string         `json:"controllerId"`
	Key          NamespacedName `json:"key"`
}

// NamespacedName is a minimal namespaced object identity.
type NamespacedName struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

// InputTuning carries compact hints for later ExploreConfig construction.
type InputTuning struct {
	MaxDepth           int                 `json:"maxDepth"`
	PermuteControllers []string            `json:"permuteControllers"`
	StaleReads         map[string][]string `json:"staleReads"`
	StaleLookback      map[string]int      `json:"staleLookback"`
}

// InputMap is the on-disk schema seed mapping from GVK to a single template object.
type InputMap struct {
	Mapping map[string][]InputTemplate `json:"mapping"`
}

// InputTemplate is a single named template for a GVK.
type InputTemplate struct {
	Name   string                     `json:"name"`
	Object *unstructured.Unstructured `json:"object"`
}
