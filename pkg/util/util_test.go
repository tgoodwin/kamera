package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestGetKind(t *testing.T) {
	// Test case 1: Object with GroupVersionKind set
	objWithGVK := &unstructured.Unstructured{}
	objWithGVK.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "apps",
		Version: "v1",
		Kind:    "Deployment",
	})
	assert.Equal(t, "Deployment", GetKind(objWithGVK))

	// Test case 2: Object without GroupVersionKind set
	objWithoutGVK := &unstructured.Unstructured{}
	assert.Equal(t, "Unstructured", GetKind(objWithoutGVK))

	assert.Equal(t, "Deployment", GetKind(&appsv1.Deployment{}))
}
func TestInferListKind(t *testing.T) {
	// Test case 1: UnstructuredList with kind suffixed by "List"
	unstructuredList := &unstructured.UnstructuredList{}
	unstructuredList.SetKind("DeploymentList")
	assert.Equal(t, "Deployment", InferListKind(unstructuredList))

	// Test case 2: UnstructuredList without kind suffixed by "List"
	unstructuredList.SetKind("CustomList")
	assert.Equal(t, "Custom", InferListKind(unstructuredList))

	// Test case 3: Typed list with items
	deploymentList := &appsv1.DeploymentList{}
	assert.Equal(t, "Deployment", InferListKind(deploymentList))
}

func TestShortenHash(t *testing.T) {
	//  Shorten hash must be deterministic
	assert.Equal(t, "pp1cx731", ShortenHash("1a2b3c4d5e6f7g8h9i0j"))
	assert.Equal(t, "pp1cx731", ShortenHash("1a2b3c4d5e6f7g8h9i0j"))
}
func TestMostCommonElementCount(t *testing.T) {
	// Test case 1: Empty slice
	assert.Equal(t, 0, MostCommonElementCount([]int{}))

	// Test case 2: Slice with one element
	assert.Equal(t, 1, MostCommonElementCount([]int{42}))

	// Test case 3: Slice with all unique elements
	assert.Equal(t, 1, MostCommonElementCount([]int{1, 2, 3, 4, 5}))

	// Test case 4: Slice with multiple occurrences of the most common element
	assert.Equal(t, 3, MostCommonElementCount([]int{1, 2, 2, 3, 2, 4, 5}))

	// Test case 5: Slice with multiple elements having the same maximum count
	assert.Equal(t, 2, MostCommonElementCount([]int{1, 1, 2, 2, 3, 4}))

	// Test case 6: Slice with strings
	assert.Equal(t, 2, MostCommonElementCount([]string{"apple", "banana", "apple", "cherry"}))

	// Test case 7: Slice with custom comparable type
	type customType struct {
		ID int
	}
	assert.Equal(t, 2, MostCommonElementCount([]customType{
		{ID: 1}, {ID: 2}, {ID: 1}, {ID: 3},
	}))
}
