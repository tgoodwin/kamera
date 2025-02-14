package snapshot

import (
	"reflect"
	"testing"
)

func TestNormalize(t *testing.T) {
	// Test case from the question
	key := IdentityKey{
		Kind:     "Pod",
		ObjectID: "pod-1",
	}
	value := VersionHash(`{"kind":"Pod","metadata":{"namespace":"default","name":"pod-1"},"spec":{"containers":[{"name":"nginx","image":"nginx"}]}}`)
	expected := NormalizedObject{
		Kind:      "Pod",
		Namespace: "default",
		Name:      "pod-1",
		Spec: map[string]interface{}{
			"containers": []interface{}{
				map[string]interface{}{
					"name":  "nginx",
					"image": "nginx",
				},
			},
		},
		Status: map[string]interface{}{},
	}

	actual := NormalizeObject(key, value)
	if actual.Kind != expected.Kind {
		t.Errorf("expected Kind %v, got %v", expected.Kind, actual.Kind)
	}
	if actual.Namespace != expected.Namespace {
		t.Errorf("expected Namespace %v, got %v", expected.Namespace, actual.Namespace)
	}
	if actual.Name != expected.Name {
		t.Errorf("expected Name %v, got %v", expected.Name, actual.Name)
	}
	if !reflect.DeepEqual(actual.Spec, expected.Spec) {
		t.Errorf("expected Spec %v, got %v", expected.Spec, actual.Spec)
	}
	if !reflect.DeepEqual(actual.Status, expected.Status) {
		t.Errorf("expected Status %v, got %v", expected.Status, actual.Status)
	}
}
