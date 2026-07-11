package apiserver

import (
	"testing"

	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

var testGVK = schema.GroupVersionKind{Group: "testing.kamera.io", Version: "v1", Kind: "Widget"}

func testResourceSchema(t *testing.T, hasStatus bool) *ResourceSchema {
	t.Helper()
	registry := NewRegistry()
	require.NoError(t, registry.RegisterResourceSchema(testGVK, true, hasStatus, &apiextensionsv1.JSONSchemaProps{
		Type: "object",
		Properties: map[string]apiextensionsv1.JSONSchemaProps{
			"spec": {
				Type: "object",
				Properties: map[string]apiextensionsv1.JSONSchemaProps{
					"first":  {Type: "string"},
					"second": {Type: "string"},
					"items": {
						Type:         "array",
						XListType:    stringPtr("map"),
						XListMapKeys: []string{"name"},
						Items: &apiextensionsv1.JSONSchemaPropsOrArray{
							Schema: &apiextensionsv1.JSONSchemaProps{
								Type: "object",
								Properties: map[string]apiextensionsv1.JSONSchemaProps{
									"name":  {Type: "string"},
									"value": {Type: "string"},
								},
							},
						},
					},
					"atomic": {
						Type:     "object",
						XMapType: stringPtr("atomic"),
						Properties: map[string]apiextensionsv1.JSONSchemaProps{
							"alpha": {Type: "string"},
							"beta":  {Type: "string"},
						},
					},
					"tags": {
						Type:      "array",
						XListType: stringPtr("set"),
						Items: &apiextensionsv1.JSONSchemaPropsOrArray{
							Schema: &apiextensionsv1.JSONSchemaProps{Type: "string"},
						},
					},
				},
			},
			"status": {
				Type: "object",
				Properties: map[string]apiextensionsv1.JSONSchemaProps{
					"phase": {Type: "string"},
				},
			},
		},
	}))
	rs, ok := registry.Lookup(testGVK)
	require.True(t, ok)
	return rs
}

func stringPtr(value string) *string { return &value }

func widget(name string, fields map[string]interface{}) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": testGVK.GroupVersion().String(),
		"kind":       testGVK.Kind,
		"metadata": map[string]interface{}{
			"name":      name,
			"namespace": "default",
		},
	}}
	for key, value := range fields {
		obj.Object[key] = value
	}
	return obj
}

func TestApplyUsesSchemaTopologyAndManagedOwnership(t *testing.T) {
	rs := testResourceSchema(t, false)
	live, err := rs.Create(widget("example", map[string]interface{}{
		"spec": map[string]interface{}{"first": "one"},
	}), "creator")
	require.NoError(t, err)
	require.NotEmpty(t, live.GetManagedFields())

	merged, err := rs.Apply(live, widget("example", map[string]interface{}{
		"spec": map[string]interface{}{
			"second": "two",
			"items": []interface{}{
				map[string]interface{}{"name": "a", "value": "one"},
			},
		},
	}), "second-manager", false, "")
	require.NoError(t, err)
	require.Equal(t, "one", merged.Object["spec"].(map[string]interface{})["first"])
	require.Equal(t, "two", merged.Object["spec"].(map[string]interface{})["second"])

	merged, err = rs.Apply(merged, widget("example", map[string]interface{}{
		"spec": map[string]interface{}{
			"second": "two",
			"items": []interface{}{
				map[string]interface{}{"name": "b", "value": "two"},
			},
		},
	}), "third-manager", false, "")
	require.NoError(t, err)
	require.Len(t, merged.Object["spec"].(map[string]interface{})["items"], 2)
}

func TestApplyConflictForceAndOmission(t *testing.T) {
	rs := testResourceSchema(t, false)
	live, err := rs.Apply(nil, widget("example", map[string]interface{}{
		"spec": map[string]interface{}{"first": "one"},
	}), "first-manager", false, "")
	require.NoError(t, err)

	_, err = rs.Apply(live, widget("example", map[string]interface{}{
		"spec": map[string]interface{}{"first": "two"},
	}), "second-manager", false, "")
	require.True(t, apierrors.IsConflict(err), "expected conflict, got %v", err)

	forced, err := rs.Apply(live, widget("example", map[string]interface{}{
		"spec": map[string]interface{}{"first": "two"},
	}), "second-manager", true, "")
	require.NoError(t, err)
	require.Equal(t, "two", forced.Object["spec"].(map[string]interface{})["first"])

	omitted, err := rs.Apply(forced, widget("example", nil), "second-manager", false, "")
	require.NoError(t, err)
	_, hasSpec := omitted.Object["spec"]
	require.False(t, hasSpec, "solely-owned omitted field should be removed")
}

func TestApplySharedOwnershipAndTrueNoOp(t *testing.T) {
	rs := testResourceSchema(t, false)
	live, err := rs.Apply(nil, widget("example", map[string]interface{}{
		"spec": map[string]interface{}{"first": "one"},
	}), "first-manager", false, "")
	require.NoError(t, err)

	shared, err := rs.Apply(live, widget("example", map[string]interface{}{
		"spec": map[string]interface{}{"first": "one"},
	}), "second-manager", false, "")
	require.NoError(t, err)
	require.Len(t, shared.GetManagedFields(), 2)

	relinquished, err := rs.Apply(shared, widget("example", nil), "first-manager", false, "")
	require.NoError(t, err)
	require.Equal(t, "one", relinquished.Object["spec"].(map[string]interface{})["first"])

	unchanged, err := rs.Apply(relinquished, widget("example", map[string]interface{}{
		"spec": map[string]interface{}{"first": "one"},
	}), "second-manager", false, "")
	require.NoError(t, err)
	require.Equal(t, relinquished.Object, unchanged.Object)
}

func TestApplyHonorsAtomicMapAndSetListTopology(t *testing.T) {
	rs := testResourceSchema(t, false)
	live, err := rs.Apply(nil, widget("example", map[string]interface{}{
		"spec": map[string]interface{}{
			"atomic": map[string]interface{}{"alpha": "one"},
			"tags":   []interface{}{"a"},
		},
	}), "first-manager", false, "")
	require.NoError(t, err)

	_, err = rs.Apply(live, widget("example", map[string]interface{}{
		"spec": map[string]interface{}{
			"atomic": map[string]interface{}{"beta": "two"},
		},
	}), "second-manager", false, "")
	require.True(t, apierrors.IsConflict(err), "atomic map should conflict as one field: %v", err)

	merged, err := rs.Apply(live, widget("example", map[string]interface{}{
		"spec": map[string]interface{}{
			"tags": []interface{}{"b"},
		},
	}), "second-manager", false, "")
	require.NoError(t, err)
	require.ElementsMatch(t, []interface{}{"a", "b"}, merged.Object["spec"].(map[string]interface{})["tags"])
}

func TestApplyMergesOwnerReferencesThenRejectsMultipleControllers(t *testing.T) {
	rs := testResourceSchema(t, false)
	controller := true
	block := true
	ownerA := metav1.OwnerReference{
		APIVersion: "testing.kamera.io/v1", Kind: "Owner", Name: "a", UID: types.UID("owner-a"),
		Controller: &controller, BlockOwnerDeletion: &block,
	}
	ownerB := metav1.OwnerReference{
		APIVersion: "testing.kamera.io/v1", Kind: "Owner", Name: "b", UID: types.UID("owner-b"),
		Controller: &controller, BlockOwnerDeletion: &block,
	}

	first := widget("example", nil)
	first.SetOwnerReferences([]metav1.OwnerReference{ownerA})
	live, err := rs.Apply(nil, first, "owner-a-manager", true, "")
	require.NoError(t, err)

	second := widget("example", nil)
	second.SetOwnerReferences([]metav1.OwnerReference{ownerB})
	_, err = rs.Apply(live, second, "owner-b-manager", true, "")
	require.True(t, apierrors.IsInvalid(err), "ForceOwnership must not bypass metadata validation: %v", err)
}

func TestStatusApplyPreservesSpecAndGeneration(t *testing.T) {
	rs := testResourceSchema(t, true)
	live, err := rs.Apply(nil, widget("example", map[string]interface{}{
		"spec": map[string]interface{}{"first": "one"},
	}), "spec-manager", false, "")
	require.NoError(t, err)
	live.SetGeneration(7)

	status, err := rs.Apply(live, widget("example", map[string]interface{}{
		"status": map[string]interface{}{"phase": "Ready"},
	}), "status-manager", false, "status")
	require.NoError(t, err)
	require.Equal(t, "one", status.Object["spec"].(map[string]interface{})["first"])
	require.Equal(t, "Ready", status.Object["status"].(map[string]interface{})["phase"])
	require.Equal(t, int64(7), status.GetGeneration())
	require.Condition(t, func() bool {
		for _, entry := range status.GetManagedFields() {
			if entry.Manager == "status-manager" && entry.Subresource == "status" {
				return true
			}
		}
		return false
	})
}

func TestRegisterCRDRejectsMultipleServedVersionsWithoutConversion(t *testing.T) {
	registry := NewRegistry()
	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "widgets.testing.kamera.io"},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: "testing.kamera.io",
			Names: apiextensionsv1.CustomResourceDefinitionNames{Kind: "Widget", Plural: "widgets"},
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{Name: "v1", Served: true, Storage: true},
				{Name: "v2", Served: true},
			},
		},
	}
	require.ErrorContains(t, registry.RegisterCRD(crd), "currently require a single served version")
}
