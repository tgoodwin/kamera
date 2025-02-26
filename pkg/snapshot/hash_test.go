package snapshot

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestJSONHasher_Hash(t *testing.T) {
	hasher := &JSONHasher{}
	obj := &unstructured.Unstructured{}
	obj.SetLabels(map[string]string{"key": "value"})

	hash, err := hasher.Hash(obj)
	assert.NoError(t, err)
	if hash.Value == "" {
		t.Errorf("expected non-empty hash, got empty")
	}
}

func TestAnonymizingHasher_Hash(t *testing.T) {
	labelReplacements := map[string]string{
		"key1": "replacement1",
		"key2": "replacement2",
	}
	hasher := NewAnonymizingHasher(labelReplacements)
	obj := &unstructured.Unstructured{}
	labels := map[string]string{"key1": "value1", "key2": "value2", "key3": "value3"}
	obj.SetLabels(labels)

	hash, err := hasher.Hash(obj)
	assert.NoError(t, err)
	if hash.Value == "" {
		t.Errorf("expected non-empty hash, got empty")
	}

	for key, value := range labels {
		if _, shouldReplace := labelReplacements[key]; shouldReplace {
			if strings.Contains(string(hash.Value), value) {
				t.Error(string(hash.Value))
				t.Errorf("expected value %s to not be present in hash, but it was", value)
			}
		}
	}

	labelsAfterHash := obj.GetLabels()
	for key, value := range labelsAfterHash {
		assert.Equal(t, labels[key], value)
	}
}
