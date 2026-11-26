package apiserver

import (
	"testing"

	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/kube-openapi/pkg/spec3"
)

func TestRegisterOpenAPIV3WithTypedGVKExtension(t *testing.T) {
	model, err := resourceOpenAPISchema(testGVK, &apiextensionsv1.JSONSchemaProps{
		Type: "object",
		Properties: map[string]apiextensionsv1.JSONSchemaProps{
			"spec": {
				Type: "object",
				Properties: map[string]apiextensionsv1.JSONSchemaProps{
					"first": {Type: "string"},
				},
			},
		},
	})
	require.NoError(t, err)
	entries := []map[string]string{{"group": testGVK.Group, "version": testGVK.Version, "kind": testGVK.Kind}}
	model.AddExtension(openAPIGVKExtension, entries)
	schemas := objectMetaModels()
	schemas["Widget"] = model
	doc := &spec3.OpenAPI{Components: &spec3.Components{Schemas: schemas}}

	registry := NewRegistry()
	require.NoError(t, registry.RegisterOpenAPIV3(doc, OpenAPIResource{
		GVK: testGVK, Scope: apiextensionsv1.NamespaceScoped,
	}))
	require.Equal(t, entries, doc.Components.Schemas["Widget"].Extensions[openAPIGVKExtension])
	rs, ok := registry.Lookup(testGVK)
	require.True(t, ok)

	applied, err := rs.Apply(nil, widget("example", map[string]interface{}{
		"spec": map[string]interface{}{"first": "one"},
	}), "controller", false, "")
	require.NoError(t, err)
	require.NotEmpty(t, applied.GetManagedFields())
}
