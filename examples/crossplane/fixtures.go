package main

import (
	_ "embed"
	"fmt"

	"github.com/crossplane/crossplane/v2/apis/apiextensions/v1"
	pkgv1 "github.com/crossplane/crossplane/v2/apis/pkg/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"
)

//go:embed fixtures/composition.yaml
var compositionYAML []byte

//go:embed fixtures/composite-resource.yaml
var compositeResourceYAML []byte

//go:embed fixtures/function-revision.yaml
var functionRevisionYAML []byte

func buildComposition() *v1.Composition {
	composition := &v1.Composition{}
	mustUnmarshalFixture(compositionYAML, composition)
	return composition
}

func buildCompositeResource() *unstructured.Unstructured {
	resource := &unstructured.Unstructured{}
	mustUnmarshalFixture(compositeResourceYAML, &resource.Object)
	return resource
}

func buildFunctionRevision() *pkgv1.FunctionRevision {
	revision := &pkgv1.FunctionRevision{}
	mustUnmarshalFixture(functionRevisionYAML, revision)
	return revision
}

func mustUnmarshalFixture(data []byte, target any) {
	if err := yaml.Unmarshal(data, target); err != nil {
		panic(fmt.Sprintf("unmarshal embedded harness fixture: %v", err))
	}
}
