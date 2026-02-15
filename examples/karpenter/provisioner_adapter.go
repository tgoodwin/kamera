package main

import (
	"context"
	"time"

	"github.com/awslabs/operatorpkg/reconciler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// provisionerAdapter bridges operatorpkg's singleton reconciler to controller-runtime.
// NOTE: In real Karpenter, the provisioner is triggered by a singleton source + internal batcher.
// In Kamera we explicitly enqueue it (via PodController + async tick) to preserve semantics.
type provisionerAdapter struct {
	reconcileFunc func(context.Context) (reconciler.Result, error)
}

func (a provisionerAdapter) Reconcile(ctx context.Context, _ reconcile.Request) (reconcile.Result, error) {
	result, err := a.reconcileFunc(ctx)
	return reconcile.Result{
		Requeue:      result.Requeue,
		RequeueAfter: time.Duration(result.RequeueAfter),
	}, err
}
