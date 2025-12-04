package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/samber/lo"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/replay"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/tracegen"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type FinalizerReconciler struct {
	*tracegen.Client
	Recorder replay.EffectRecorder
}

// Reconcile handler for tracked objects
func (r *FinalizerReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	logger := log.FromContext(ctx)
	logger.V(2).Info("FinalizerReconciler Reconcile", "request", req)
	obj := &unstructured.Unstructured{}
	canonicalKind, ok := ctx.Value(tag.CleanupKindKey{}).(string)
	if !ok {
		return reconcile.Result{}, errors.New("no kind in context")
	}
	// The context stores canonical kind like "core/Pod" or "apps/Deployment".
	// We need to extract just the Kind part (after the /) for SetKind.
	kind := canonicalKind
	if idx := strings.LastIndex(canonicalKind, "/"); idx >= 0 {
		kind = canonicalKind[idx+1:]
	}
	obj.SetKind(kind)
	if err := r.Get(ctx, req.NamespacedName, obj); err != nil {
		// If the object is not found, it's already been deleted - nothing to clean up
		if client.IgnoreNotFound(err) == nil {
			logger.V(1).Info("object already deleted, nothing to clean up")
			return reconcile.Result{}, nil
		}
		logger.Error(err, "failed to get object")
		return reconcile.Result{}, fmt.Errorf("failed to get object: %w", err)
	}

	logger.WithValues(
		"kind", kind,
		"namespaceName", req.NamespacedName,
	).V(1).Info("processing object marked for deletion")

	// get the Kind out of the context
	if obj.GetDeletionTimestamp() != nil {
		// Object is being deleted

		// in simulation/replay, need to remove the object from state
		if r.Recorder != nil {
			// simulation mode
			logger.V(1).Info("recording REMOVE effect")
			if err := r.Recorder.RecordEffect(ctx, obj, event.REMOVE, nil); err != nil {
				logger.Error(err, "recording effect")
				return reconcile.Result{}, fmt.Errorf("recording effect: %w", err)
			}
			// emit the event
			r.LogOperation(ctx, obj, event.REMOVE)
			return reconcile.Result{}, nil
		}

		// in production mode, we use a custom finalizer to ensure the below LogOperation
		// event runs as close to the actual deletion as possible.
		if len(obj.GetFinalizers()) == 1 && obj.GetFinalizers()[0] == tag.SleeveFinalizer {
			// Remove our finalizer to allow actual deletion by the APIServer
			obj.SetFinalizers(lo.Without(obj.GetFinalizers(), tag.SleeveFinalizer))
			// Update object to remove finalizer and trigger removal
			if err := r.Update(ctx, obj); err != nil {
				logger.Error(err, "failed to update object")
				return reconcile.Result{}, fmt.Errorf("failed to update object: %w", err)
			}
			logger.V(2).Info("Emitting REMOVE event")
			r.Client.LogOperation(ctx, obj, event.REMOVE)
			logger.V(1).Info("Finalizer removed")
		}
	}

	return reconcile.Result{}, nil
}
