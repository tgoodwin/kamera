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
