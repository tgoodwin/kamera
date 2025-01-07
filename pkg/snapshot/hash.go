package snapshot

import (
	"encoding/json"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type Hasher interface {
	Hash(obj *unstructured.Unstructured) VersionHash
}

type VersionHash string

type JSONHasher struct {
	// TODO
	fieldsToIgnore []string
}

func (h *JSONHasher) Hash(obj *unstructured.Unstructured) VersionHash {
	str, err := json.Marshal(obj)
	if err != nil {
		panic("version hashing object")
	}
	return VersionHash(str)
}

// func (h *Hasher) Hash(obj client.Object) VersionHash {
// 	return ""
// }
