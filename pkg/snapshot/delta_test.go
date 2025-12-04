package snapshot

import (
	"strings"
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

func TestComputeDeltaUsesUnifiedDiff(t *testing.T) {
	oldObj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"spec": map[string]interface{}{
				"replicas": int64(1),
			},
		},
	}
	newObj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"spec": map[string]interface{}{
				"replicas": int64(0),
			},
		},
	}

	diffStr := ComputeDelta(oldObj, newObj)
	if diffStr == "" {
		t.Fatalf("expected diff to contain content")
	}
	if strings.Contains(diffStr, "strings.Join") {
		t.Fatalf("expected diff to be YAML-like, got:\n%s", diffStr)
	}
	if strings.Contains(diffStr, `\\n`) {
		t.Fatalf("expected diff to render real newlines, got:\n%s", diffStr)
	}
	if !strings.Contains(diffStr, "-  replicas: 1") || !strings.Contains(diffStr, "+  replicas: 0") {
		t.Fatalf("expected diff to include replica change, got:\n%s", diffStr)
	}
}
