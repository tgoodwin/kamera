package coverage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestNormalizeTemplate(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":              "orig",
			"namespace":         "orig-ns",
			"uid":               "u1",
			"resourceVersion":   "2",
			"generation":        int64(3),
			"managedFields":     []any{"field"},
			"creationTimestamp": "2020-01-01T00:00:00Z",
			"selfLink":          "/api/v1/configmaps/orig",
			"finalizers":        []any{"finalizer.test"},
		},
		"status": map[string]any{
			"phase": "Active",
		},
	}}

	norm := NormalizeTemplate(obj, "new-name", "default")
	require.Equal(t, "new-name", norm.GetName())
	require.Equal(t, "default", norm.GetNamespace())

	_, found, err := unstructured.NestedFieldNoCopy(norm.Object, "status")
	require.NoError(t, err)
	require.False(t, found)

	metaFields := []string{"uid", "resourceVersion", "generation", "managedFields", "creationTimestamp", "selfLink", "finalizers"}
	for _, field := range metaFields {
		_, found, err = unstructured.NestedFieldNoCopy(norm.Object, "metadata", field)
		require.NoError(t, err)
		require.False(t, found)
	}
}
