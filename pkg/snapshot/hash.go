package snapshot

import (
	"encoding/json"

	"github.com/tgoodwin/sleeve/pkg/tag"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type Hasher interface {
	Hash(obj *unstructured.Unstructured) VersionHash
}

type VersionHash string

type JSONHasher struct {
}

func (h *JSONHasher) Hash(obj *unstructured.Unstructured) VersionHash {
	// first, craete a copy of the object
	objCopy := obj.DeepCopy()
	str, err := json.Marshal(objCopy)
	if err != nil {
		panic("version hashing object")
	}
	return VersionHash(str)
}

type AnonymizingHasher struct {
	Replacements map[string]string
}

func NewAnonymizingHasher(labelReplacements map[string]string) *AnonymizingHasher {
	return &AnonymizingHasher{Replacements: labelReplacements}
}

func (h *AnonymizingHasher) Hash(obj *unstructured.Unstructured) VersionHash {
	origLabels := obj.GetLabels()
	anonymizedLabels := make(map[string]string, len(origLabels))
	for k, v := range origLabels {
		if replacement, ok := h.Replacements[k]; ok {
			anonymizedLabels[k] = replacement
		} else {
			anonymizedLabels[k] = v
		}
	}
	obj.SetLabels(anonymizedLabels)
	str, err := json.Marshal(obj)
	if err != nil {
		panic("version hashing object")
	}
	return VersionHash(str)
}

// DefaultLabelReplacements is a map of label replacements for
// the labels that take on non-deterministic values.
var DefaultLabelReplacements = map[string]string{
	tag.TraceyReconcileID: "RECONCILE_ID",
	tag.ChangeID:          "CHANGE_ID",
	tag.TraceyObjectID:    "OBJECT_ID",
	tag.DeletionID:        "DELETION_ID",
}
