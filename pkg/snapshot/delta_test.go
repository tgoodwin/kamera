package snapshot

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestComputeDeltaIgnoresDefaultKeys(t *testing.T) {
	oldObj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"metadata": map[string]interface{}{
				"resourceVersion": "1",
			},
		},
	}

	newObj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"metadata": map[string]interface{}{
				"resourceVersion": "2",
			},
		},
	}

	diffStr := ComputeDelta(oldObj, newObj)
	if diffStr != "" {
		t.Fatalf("expected diff to ignore resourceVersion changes, got:\n%s", diffStr)
	}
}
