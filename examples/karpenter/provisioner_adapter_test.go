package main

import (
	"context"
	"testing"
	"time"

	"github.com/awslabs/operatorpkg/reconciler"
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
