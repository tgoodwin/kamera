package controller

import (
	"context"
	"errors"
	"fmt"

	"github.com/samber/lo"
	tracegen "github.com/tgoodwin/sleeve/pkg/client"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/replay"
	"github.com/tgoodwin/sleeve/pkg/tag"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
	obj := &unstructured.Unstructured{}
	kind, ok := ctx.Value(tag.CleanupKindKey{}).(string)
	if !ok {
		return reconcile.Result{}, errors.New("no kind in context")
	}
	obj.SetKind(kind)
	if err := r.Get(ctx, req.NamespacedName, obj); err != nil {
		logger.Error(err, "failed to get object")
		return reconcile.Result{}, fmt.Errorf("failed to get object: %w", err)
	}

	logger.WithValues("kind", kind, "namespaceName", req.NamespacedName, "deletionTimestamp", obj.GetDeletionTimestamp()).
		Info("got deleted object")

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
			return reconcile.Result{}, nil
		}

		// Only emit REMOVE event if this is our last finalizer
		if len(obj.GetFinalizers()) == 1 && obj.GetFinalizers()[0] == tag.SleeveFinalizer {
			logger.V(2).Info("Emitting REMOVE event")
			r.Client.LogOperation(ctx, obj, event.REMOVE) // Your event logging mechanism
		}

		// Remove our finalizer to allow actual deletion
		obj.SetFinalizers(lo.Without(obj.GetFinalizers(), tag.SleeveFinalizer))
		// Update object to remove finalizer
	}

	return reconcile.Result{}, nil
}
