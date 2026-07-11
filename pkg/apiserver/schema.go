package apiserver

import (
	"errors"
	"fmt"
	"maps"
	"strings"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	generatedopenapi "k8s.io/apiextensions-apiserver/pkg/generated/openapi"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/managedfields"
	"k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/spec3"
	"k8s.io/kube-openapi/pkg/validation/spec"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// Registry stores immutable, schema-derived API write semantics by GVK.
// Field ownership itself is stored in each object's metadata.managedFields.
type Registry struct {
	resources map[schema.GroupVersionKind]*ResourceSchema
}

// NewRegistry returns an empty schema registry.
func NewRegistry() *Registry {
	return &Registry{resources: make(map[schema.GroupVersionKind]*ResourceSchema)}
}

// Clone returns a shallow copy. ResourceSchema values are immutable after
// construction and can safely be shared by forked explorers.
func (r *Registry) Clone() *Registry {
	if r == nil {
		return NewRegistry()
	}
	return &Registry{resources: maps.Clone(r.resources)}
}

// Lookup returns registered write semantics for a GVK.
func (r *Registry) Lookup(gvk schema.GroupVersionKind) (*ResourceSchema, bool) {
	if r == nil {
		return nil, false
	}
	rs, ok := r.resources[gvk]
	return rs, ok
}

// RegisterCRD compiles every served CRD version into Kubernetes' upstream
// structured-merge-diff type converter and field managers.
func (r *Registry) RegisterCRD(crd *apiextensionsv1.CustomResourceDefinition) error {
	if r == nil {
		return errors.New("schema registry is nil")
	}
	if crd == nil {
		return errors.New("CRD is nil")
	}
	servedVersions := 0
	for _, version := range crd.Spec.Versions {
		if version.Served {
			servedVersions++
		}
	}
	if servedVersions == 0 {
		return fmt.Errorf("CRD %s has no served versions", crd.Name)
	}
	if servedVersions > 1 {
		return fmt.Errorf("CRD %s has %d served versions; schema-backed writes currently require a single served version", crd.Name, servedVersions)
	}

	models := objectMetaModels()
	for _, version := range crd.Spec.Versions {
		if !version.Served {
			continue
		}
		if version.Schema == nil || version.Schema.OpenAPIV3Schema == nil {
			return fmt.Errorf("CRD %s version %s has no structural schema", crd.Name, version.Name)
		}
		root, err := resourceOpenAPISchema(
			schema.GroupVersionKind{Group: crd.Spec.Group, Version: version.Name, Kind: crd.Spec.Names.Kind},
			version.Schema.OpenAPIV3Schema,
		)
		if err != nil {
			return fmt.Errorf("build structural schema for %s/%s: %w", crd.Name, version.Name, err)
		}
		models[resourceModelName(crd.Spec.Group, version.Name, crd.Spec.Names.Kind)] = root
	}
	typeConverter, err := managedfields.NewTypeConverter(models, crd.Spec.PreserveUnknownFields)
	if err != nil {
		return fmt.Errorf("build type converter for %s: %w", crd.Name, err)
	}

	namespaced := crd.Spec.Scope == apiextensionsv1.NamespaceScoped
	for _, version := range crd.Spec.Versions {
		if !version.Served {
			continue
		}
		gvk := schema.GroupVersionKind{Group: crd.Spec.Group, Version: version.Name, Kind: crd.Spec.Names.Kind}
		hasStatus := version.Subresources != nil && version.Subresources.Status != nil
		rs, err := newResourceSchema(gvk, namespaced, hasStatus, typeConverter)
		if err != nil {
			return fmt.Errorf("configure field manager for %s: %w", gvk, err)
		}
		r.resources[gvk] = rs
	}
	return nil
}

func resourceOpenAPISchema(gvk schema.GroupVersionKind, input *apiextensionsv1.JSONSchemaProps) (*spec.Schema, error) {
	internal := &apiextensions.JSONSchemaProps{}
	if err := apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(input, internal, nil); err != nil {
		return nil, err
	}
	structural, err := structuralschema.NewStructural(internal)
	if err != nil {
		return nil, err
	}
	root := structural.ToKubeOpenAPI()
	if root.Properties == nil {
		root.Properties = make(map[string]spec.Schema)
	}
	root.Properties["apiVersion"] = *spec.StringProperty()
	root.Properties["kind"] = *spec.StringProperty()
	root.Properties["metadata"] = *spec.RefSchema(objectMetaRef())
	root.AddExtension("x-kubernetes-group-version-kind", []interface{}{
		map[string]interface{}{
			"group": gvk.Group, "version": gvk.Version, "kind": gvk.Kind,
		},
	})
	return root, nil
}

func resourceModelName(group, version, kind string) string {
	if group == "" {
		group = "core"
	}
	return fmt.Sprintf("io.k8s.kamera.%s.%s.%s", strings.ReplaceAll(group, ".", "_"), version, kind)
}

func objectMetaRef() string {
	return "#/definitions/" + common.EscapeJsonPointer(metav1.ObjectMeta{}.OpenAPIModelName())
}

func objectMetaModels() map[string]*spec.Schema {
	definitions := generatedopenapi.GetOpenAPIDefinitions(func(name string) spec.Ref {
		return spec.MustCreateRef("#/definitions/" + common.EscapeJsonPointer(name))
	})
	models := make(map[string]*spec.Schema, len(definitions))
	for name, definition := range definitions {
		model := definition.Schema
		models[name] = &model
	}
	return models
}

// RegisterResourceSchema registers one resource version from structural
// JSONSchemaProps. It is useful for built-in or aggregated resources when a
// complete group-version OpenAPI document is not readily available.
func (r *Registry) RegisterResourceSchema(
	gvk schema.GroupVersionKind,
	namespaced bool,
	hasStatus bool,
	openAPIV3Schema *apiextensionsv1.JSONSchemaProps,
) error {
	if openAPIV3Schema == nil {
		return fmt.Errorf("schema for %s is nil", gvk)
	}
	plural := strings.ToLower(gvk.Kind) + "s"
	crd := &apiextensionsv1.CustomResourceDefinition{
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: gvk.Group,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Kind:     gvk.Kind,
				ListKind: gvk.Kind + "List",
				Plural:   plural,
				Singular: strings.ToLower(gvk.Kind),
			},
			Scope: apiextensionsv1.ClusterScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name:    gvk.Version,
				Served:  true,
				Storage: true,
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: openAPIV3Schema.DeepCopy(),
				},
			}},
		},
	}
	if namespaced {
		crd.Spec.Scope = apiextensionsv1.NamespaceScoped
	}
	if hasStatus {
		crd.Spec.Versions[0].Subresources = &apiextensionsv1.CustomResourceSubresources{
			Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
		}
	}
	return r.RegisterCRD(crd)
}

// RegisterOpenAPIV3 registers every GVK described by a complete OpenAPI v3
// document. Resource scope and status-subresource availability are not present
// in OpenAPI, so callers that need exact metadata/subresource behavior should
// prefer RegisterCRD or RegisterResourceSchema.
func (r *Registry) RegisterOpenAPIV3(doc *spec3.OpenAPI) error {
	if r == nil {
		return errors.New("schema registry is nil")
	}
	if doc == nil || doc.Components == nil {
		return errors.New("OpenAPI v3 document has no components")
	}
	typeConverter, err := managedfields.NewTypeConverter(doc.Components.Schemas, false)
	if err != nil {
		return fmt.Errorf("build OpenAPI type converter: %w", err)
	}

	registered := 0
	groupKinds := make(map[schema.GroupKind]schema.GroupVersionKind)
	for _, model := range doc.Components.Schemas {
		for _, gvk := range schemaGVKs(model) {
			if previous, found := groupKinds[gvk.GroupKind()]; found && previous.Version != gvk.Version {
				return fmt.Errorf("OpenAPI document contains multiple versions for %s (%s and %s); schema-backed writes currently require one version", gvk.GroupKind(), previous.Version, gvk.Version)
			}
			groupKinds[gvk.GroupKind()] = gvk
			rs, err := newResourceSchema(gvk, true, false, typeConverter)
			if err != nil {
				return fmt.Errorf("configure field manager for %s: %w", gvk, err)
			}
			r.resources[gvk] = rs
			registered++
		}
	}
	if registered == 0 {
		return errors.New("OpenAPI v3 document contains no x-kubernetes-group-version-kind schemas")
	}
	return nil
}

func schemaGVKs(model *spec.Schema) []schema.GroupVersionKind {
	if model == nil {
		return nil
	}
	raw, ok := model.Extensions["x-kubernetes-group-version-kind"]
	if !ok {
		return nil
	}
	var out []schema.GroupVersionKind
	appendGVK := func(group, version, kind string) {
		if version != "" && kind != "" {
			out = append(out, schema.GroupVersionKind{Group: group, Version: version, Kind: kind})
		}
	}
	switch values := raw.(type) {
	case []map[string]string:
		for _, value := range values {
			appendGVK(value["group"], value["version"], value["kind"])
		}
	case []interface{}:
		for _, item := range values {
			switch value := item.(type) {
			case map[string]interface{}:
				group, _ := value["group"].(string)
				version, _ := value["version"].(string)
				kind, _ := value["kind"].(string)
				appendGVK(group, version, kind)
			case map[interface{}]interface{}:
				group, _ := value["group"].(string)
				version, _ := value["version"].(string)
				kind, _ := value["kind"].(string)
				appendGVK(group, version, kind)
			}
		}
	}
	return out
}

// ResourceSchema contains the immutable schema machinery for one GVK.
type ResourceSchema struct {
	GVK           schema.GroupVersionKind
	Namespaced    bool
	HasStatus     bool
	mainManager   *managedfields.FieldManager
	statusManager *managedfields.FieldManager
}

func newResourceSchema(
	gvk schema.GroupVersionKind,
	namespaced bool,
	hasStatus bool,
	typeConverter managedfields.TypeConverter,
) (*ResourceSchema, error) {
	converter := unstructuredObjectConverter{}
	defaulter := unstructuredDefaulter{}
	creator := unstructuredCreator{}
	apiVersion := fieldpath.APIVersion(gvk.GroupVersion().String())

	mainReset := map[fieldpath.APIVersion]*fieldpath.Set{}
	if hasStatus {
		mainReset[apiVersion] = fieldpath.NewSet(fieldpath.MakePathOrDie("status"))
	}
	mainManager, err := managedfields.NewDefaultCRDFieldManager(
		typeConverter,
		converter,
		defaulter,
		creator,
		gvk,
		gvk.GroupVersion(),
		"",
		fieldpath.NewExcludeFilterSetMap(mainReset),
	)
	if err != nil {
		return nil, err
	}

	var statusManager *managedfields.FieldManager
	if hasStatus {
		statusReset := map[fieldpath.APIVersion]*fieldpath.Set{
			apiVersion: fieldpath.NewSet(fieldpath.MakePathOrDie("spec")),
		}
		statusManager, err = managedfields.NewDefaultCRDFieldManager(
			typeConverter,
			converter,
			defaulter,
			creator,
			gvk,
			gvk.GroupVersion(),
			"status",
			fieldpath.NewExcludeFilterSetMap(statusReset),
		)
		if err != nil {
			return nil, err
		}
	}

	return &ResourceSchema{
		GVK:           gvk,
		Namespaced:    namespaced,
		HasStatus:     hasStatus,
		mainManager:   mainManager,
		statusManager: statusManager,
	}, nil
}

type unstructuredCreator struct{}

func (unstructuredCreator) New(gvk schema.GroupVersionKind) (runtime.Object, error) {
	return newUnstructured(gvk), nil
}

type unstructuredDefaulter struct{}

func (unstructuredDefaulter) Default(runtime.Object) {}

type unstructuredObjectConverter struct{}

func (unstructuredObjectConverter) Convert(in, out, _ interface{}) error {
	src, ok := in.(runtime.Unstructured)
	if !ok {
		return fmt.Errorf("convert input %T is not unstructured", in)
	}
	dst, ok := out.(runtime.Unstructured)
	if !ok {
		return fmt.Errorf("convert output %T is not unstructured", out)
	}
	dst.SetUnstructuredContent(runtime.DeepCopyJSON(src.UnstructuredContent()))
	return nil
}

func (unstructuredObjectConverter) ConvertToVersion(in runtime.Object, target runtime.GroupVersioner) (runtime.Object, error) {
	src, ok := in.(runtime.Unstructured)
	if !ok {
		return nil, fmt.Errorf("convert input %T is not unstructured", in)
	}
	copy := newUnstructured(src.GetObjectKind().GroupVersionKind())
	copy.SetUnstructuredContent(runtime.DeepCopyJSON(src.UnstructuredContent()))
	if kind, ok := target.KindForGroupVersionKinds([]schema.GroupVersionKind{copy.GroupVersionKind()}); ok {
		copy.SetGroupVersionKind(kind)
	}
	return copy, nil
}

func (unstructuredObjectConverter) ConvertFieldLabel(schema.GroupVersionKind, string, string) (string, string, error) {
	return "", "", errors.New("field label conversion is not implemented")
}
