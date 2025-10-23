package snapshot

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestComputeDeltaProducesYAMLDiff(t *testing.T) {
	oldObj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"status": map[string]interface{}{
				"conditions": []interface{}{
					map[string]interface{}{
						"type":   "Ready",
						"status": "Unknown",
					},
				},
			},
		},
	}

	newObj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"status": map[string]interface{}{
				"conditions": []interface{}{
					map[string]interface{}{
						"type":               "ContainerHealthy",
						"status":             "Unknown",
						"reason":             "Deploying",
						"lastTransitionTime": "2025-10-23T20:32:29Z",
					},
					map[string]interface{}{
						"type":               "Ready",
						"status":             "Unknown",
						"reason":             "Deploying",
						"lastTransitionTime": "2025-10-23T20:32:29Z",
					},
				},
			},
		},
	}

	diffStr := ComputeDelta(oldObj, newObj)
	if diffStr == "" {
		t.Fatalf("expected diff output, got empty string")
	}

	normalized := strings.ReplaceAll(diffStr, " ", "")
	normalized = strings.ReplaceAll(normalized, "\t", "")
	if !strings.Contains(normalized, "+type:ContainerHealthy") {
		t.Fatalf("expected diff to record addition, got:\n%s", diffStr)
	}
	if !strings.Contains(normalized, "-status:Unknown") {
		t.Fatalf("expected diff to record removal, got:\n%s", diffStr)
	}
}

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
