package replay

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestDynamicClientDelegatesNamespacedCRUD(t *testing.T) {
	gvk := schema.GroupVersionKind{Group: "example.org", Version: "v1", Kind: "Widget"}
	gvr := schema.GroupVersionResource{Group: "example.org", Version: "v1", Resource: "widgets"}
	scheme := runtime.NewScheme()
	scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(gvk.GroupVersion().WithKind("WidgetList"), &unstructured.UnstructuredList{})

	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{gvk.GroupVersion()})
	mapper.AddSpecific(gvk, gvr, gvr, meta.RESTScopeNamespace)

	resource := NewDynamicClient(
		fake.NewClientBuilder().WithScheme(scheme).Build(),
		mapper,
	).Resource(gvr).Namespace("default")

	object := &unstructured.Unstructured{}
	object.SetGroupVersionKind(gvk)
	object.SetName("sample")
	object.Object["spec"] = map[string]any{"value": "initial"}
	if _, err := resource.Create(context.Background(), object, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create object: %v", err)
	}

	got, err := resource.Get(context.Background(), "sample", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get object: %v", err)
	}
	if got.GetNamespace() != "default" {
		t.Fatalf("expected default namespace, got %q", got.GetNamespace())
	}

	list, err := resource.List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list objects: %v", err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("expected one object, got %d", len(list.Items))
	}

	if err := resource.Delete(context.Background(), "sample", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete object: %v", err)
	}
	if _, err := resource.Get(context.Background(), "sample", metav1.GetOptions{}); err == nil {
		t.Fatal("expected deleted object to be absent")
	}
}
