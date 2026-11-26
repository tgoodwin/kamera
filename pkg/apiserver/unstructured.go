package apiserver

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func newUnstructured(gvk schema.GroupVersionKind) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
	obj.SetGroupVersionKind(gvk)
	return obj
}
