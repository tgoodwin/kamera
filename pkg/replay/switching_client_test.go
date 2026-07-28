package replay

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestSwitchingClientUsesCurrentDelegate(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	delegate := func(value string) client.Client {
		return fake.NewClientBuilder().WithScheme(scheme).WithObjects(&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "sample"},
			Data:       map[string]string{"value": value},
		}).Build()
	}

	switching := NewSwitchingClient()
	for _, value := range []string{"first", "second"} {
		switching.Set(delegate(value))
		got := &corev1.ConfigMap{}
		if err := switching.Get(
			context.Background(),
			client.ObjectKey{Namespace: "default", Name: "sample"},
			got,
		); err != nil {
			t.Fatalf("get from %s delegate: %v", value, err)
		}
		if got.Data["value"] != value {
			t.Fatalf("expected %q delegate value, got %q", value, got.Data["value"])
		}
	}
}
