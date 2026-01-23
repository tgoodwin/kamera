package main

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

func TestNodeRegistrarCreatesNode(t *testing.T) {
	s := scheme.Scheme
	utilruntime.Must(corev1.AddToScheme(s))

	nc := &v1.NodeClaim{}
	nc.Name = "nc-1"
	nc.Status.ProviderID = "provider-1"

	cl := fake.NewClientBuilder().WithScheme(s).WithObjects(nc).Build()
	reg := nodeRegistrar{client: cl}

	_, err := reg.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(nc)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var node corev1.Node
	if err := cl.Get(context.Background(), client.ObjectKey{Name: "provider-1"}, &node); err != nil {
		t.Fatalf("expected node to exist: %v", err)
	}
}
