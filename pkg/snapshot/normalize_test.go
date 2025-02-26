package snapshot

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNormalize(t *testing.T) {
	value := NewDefaultHash(`{"apiVersion":"v1","kind":"Pod","metadata":{"namespace":"default","name":"pod-1"},"spec":{"containers":[{"name":"nginx","image":"nginx"}]}}`)
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

	actual := NormalizeObject(value)
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

func TestNormalizedDiff(t *testing.T) {
	tests := []struct {
		name          string
		key           IdentityKey
		value1        VersionHash
		value2        VersionHash
		expected      *NormalizedDiff
		expectedError bool
	}{
		{
			name: "Identical objects",
			key: IdentityKey{
				Kind:     "Pod",
				ObjectID: "pod-1",
			},
			value1: NewDefaultHash(`{"apiVersion": "appsv1", "kind":"Pod","metadata":{"namespace":"default","name":"pod-1"},"spec":{"containers":[{"name":"nginx","image":"nginx"}]}}`),
			value2: NewDefaultHash(`{"apiVersion": "appsv1", "kind":"Pod","metadata":{"namespace":"default","name":"pod-1"},"spec":{"containers":[{"name":"nginx","image":"nginx"}]}}`),
			expected: &NormalizedDiff{
				Base:       NormalizeObject(NewDefaultHash(`{"apiVersion": "appsv1", "kind":"Pod","metadata":{"namespace":"default","name":"pod-1"},"spec":{"containers":[{"name":"nginx","image":"nginx"}]}}`)),
				Other:      NormalizeObject(NewDefaultHash(`{"apiVersion": "appsv1", "kind":"Pod","metadata":{"namespace":"default","name":"pod-1"},"spec":{"containers":[{"name":"nginx","image":"nginx"}]}}`)),
				SpecDiff:   []string{},
				StatusDiff: []string{},
			},
		},
		{
			name: "Different identities",
			key: IdentityKey{
				Kind:     "Pod",
				ObjectID: "pod-1",
			},
			value1:        NewDefaultHash(`{"apiVersion": "appsv1", "kind":"Pod","metadata":{"namespace":"default","name":"pod-1"},"spec":{"containers":[{"name":"nginx","image":"nginx"}]}}`),
			value2:        NewDefaultHash(`{"apiVersion": "appsv2", "kind":"Pod","metadata":{"namespace":"default","name":"pod-1"},"spec":{"containers":[{"name":"nginx","image":"nginx"}]}}`),
			expected:      nil,
			expectedError: true,
		},
		{
			name: "Different spec and status",
			key: IdentityKey{
				Kind:     "Pod",
				ObjectID: "pod-1",
			},
			value1: NewDefaultHash(`{"apiVersion": "appsv1", "kind":"Pod","metadata":{"namespace":"default","name":"pod-1"},"spec":{"containers":[{"name":"nginx","image":"nginx"}]},"status":{"phase":"Running"}}`),
			value2: NewDefaultHash(`{"apiVersion": "appsv1", "kind":"Pod","metadata":{"namespace":"default","name":"pod-1"},"spec":{"containers":[{"name":"nginx","image":"nginx:latest"}]},"status":{"phase":"Pending"}}`),
			expected: &NormalizedDiff{
				Base:       NormalizeObject(NewDefaultHash(`{"apiVersion": "appsv1", "kind":"Pod","metadata":{"namespace":"default","name":"pod-1"},"spec":{"containers":[{"name":"nginx","image":"nginx"}]},"status":{"phase":"Running"}}`)),
				Other:      NormalizeObject(NewDefaultHash(`{"apiVersion": "appsv1", "kind":"Pod","metadata":{"namespace":"default","name":"pod-1"},"spec":{"containers":[{"name":"nginx","image":"nginx:latest"}]},"status":{"phase":"Pending"}}`)),
				SpecDiff:   []string{`{"op":"replace","path":"/containers[0]/image","value":"nginx:latest"}`},
				StatusDiff: []string{`{"op":"replace","path":"/phase","value":"Pending"}`},
			},
			expectedError: false,
		},
		// Add more test cases as needed
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := NormalizeObject(tt.value1).Diff(NormalizeObject(tt.value2))
			if (err != nil) != tt.expectedError {
				t.Fatalf("expected error: %v, got: %v", tt.expectedError, err)
			}
			if !tt.expectedError {
				if !assert.ElementsMatch(t, actual.SpecDiff, tt.expected.SpecDiff) {
					t.Errorf("expected SpecDiff %v, got %v", tt.expected.SpecDiff, actual.SpecDiff)
				}
				if !assert.ElementsMatch(t, actual.StatusDiff, tt.expected.StatusDiff) {
					t.Errorf("expected StatusDiff %v, got %v", tt.expected.StatusDiff, actual.StatusDiff)
				}
			}
		})
	}
}
