package replay

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestDefaultNamespaceClientDefaultsSelectedObjects(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	inner := fake.NewClientBuilder().WithScheme(scheme).Build()
	wrapped := NewDefaultNamespaceClient(inner, "default", func(obj client.Object) bool {
		_, ok := obj.(*corev1.ConfigMap)
		return ok
	})

	created := &corev1.ConfigMap{}
	created.Name = "sample"
	if err := wrapped.Create(context.Background(), created); err != nil {
		t.Fatalf("create object: %v", err)
	}
	if created.Namespace != "default" {
		t.Fatalf("expected namespace to be defaulted, got %q", created.Namespace)
	}

	got := &corev1.ConfigMap{}
	if err := wrapped.Get(context.Background(), client.ObjectKey{Name: "sample"}, got); err != nil {
		t.Fatalf("get object with defaulted key: %v", err)
	}
	if got.Namespace != "default" {
		t.Fatalf("expected fetched object in default namespace, got %q", got.Namespace)
	}
}
