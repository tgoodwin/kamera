package main

import (
	"context"
	"testing"
	"time"

	"github.com/awslabs/operatorpkg/reconciler"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestProvisionerAdapterMapsResult(t *testing.T) {
	adapter := provisionerAdapter{
		reconcileFunc: func(ctx context.Context) (reconciler.Result, error) {
			return reconciler.Result{RequeueAfter: time.Second * 5}, nil
		},
	}
	res, err := adapter.Reconcile(context.Background(), reconcile.Request{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.RequeueAfter != 5*time.Second {
		t.Fatalf("expected 5s requeue, got %v", res.RequeueAfter)
	}
}

func TestNameGeneratingClientAssignsName(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	obj := &corev1.ConfigMap{}
	obj.GenerateName = "cm-"

	cl := fake.NewClientBuilder().WithScheme(scheme).Build()
	wrapped := &nameGeneratingClient{Client: cl}

	if err := wrapped.Create(context.Background(), obj); err != nil {
		t.Fatalf("create failed: %v", err)
	}
	if obj.Name == "" {
		t.Fatalf("expected generated name to be set")
	}
}
