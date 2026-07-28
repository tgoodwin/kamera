package main

import (
	_ "embed"
	"fmt"

	v1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"
)

//go:embed fixtures/application-rgd.yaml
var applicationRGDYAML []byte

//go:embed fixtures/application-instance.yaml
var applicationInstanceYAML []byte

func buildQuickstartApplicationRGDTyped() *v1alpha1.ResourceGraphDefinition {
	rgd := &v1alpha1.ResourceGraphDefinition{}
	mustUnmarshalFixture(applicationRGDYAML, rgd)
	return rgd
}

func buildQuickstartApplicationRGD() *unstructured.Unstructured {
	rgd := buildQuickstartApplicationRGDTyped()
	object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(rgd)
	if err != nil {
		panic(fmt.Sprintf("convert typed RGD to unstructured: %v", err))
	}
	result := &unstructured.Unstructured{Object: object}
	result.SetAPIVersion(applicationAPIVersion)
	result.SetKind(resourceGraphDefinitionKind)
	return result
}

func buildQuickstartApplicationInstance() *unstructured.Unstructured {
	instance := &unstructured.Unstructured{}
	mustUnmarshalFixture(applicationInstanceYAML, &instance.Object)
	return instance
}

func mustUnmarshalFixture(data []byte, target any) {
	if err := yaml.Unmarshal(data, target); err != nil {
		panic(fmt.Sprintf("unmarshal embedded harness fixture: %v", err))
	}
}
