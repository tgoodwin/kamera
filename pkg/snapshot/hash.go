package snapshot

import (
	"encoding/json"

	"github.com/tgoodwin/sleeve/pkg/tag"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// type Hasher interface {
// 	Hash(obj *unstructured.Unstructured) VersionHash
// }

type VersionHash string

type JSONHasher struct {
}

func NewDefaultHasher() *JSONHasher {
	return &JSONHasher{}
}

func (h *JSONHasher) Hash(obj *unstructured.Unstructured) (VersionHash, error) {
	// first, craete a copy of the object
	objCopy := obj.DeepCopy()
	str, err := json.Marshal(objCopy)
	if err != nil {
		return "", err
	}
	return VersionHash(str), nil
}

type AnonymizingHasher struct {
	Replacements map[string]string
}

func NewAnonymizingHasher(labelReplacements map[string]string) *AnonymizingHasher {
	return &AnonymizingHasher{Replacements: labelReplacements}
}

func (h *AnonymizingHasher) Hash(obj *unstructured.Unstructured) (VersionHash, error) {
	objCopy := obj.DeepCopy()
	origLabels := objCopy.GetLabels()
	anonymizedLabels := make(map[string]string, len(origLabels))
	for k, v := range origLabels {
		if replacement, ok := h.Replacements[k]; ok {
			anonymizedLabels[k] = replacement
		} else {
			anonymizedLabels[k] = v
		}
	}
	objCopy.SetLabels(anonymizedLabels)
	str, err := json.Marshal(objCopy)
	if err != nil {
		return "", err
	}
	return VersionHash(str), nil
}

// DefaultLabelReplacements is a map of label replacements for
// the labels that take on non-deterministic values.
var DefaultLabelReplacements = map[string]string{
	tag.TraceyReconcileID: "RECONCILE_ID",
	tag.ChangeID:          "CHANGE_ID",
	tag.TraceyObjectID:    "OBJECT_ID",
	tag.DeletionID:        "DELETION_ID",
	tag.TraceyCreatorID:   "CREATOR_ID",
}
