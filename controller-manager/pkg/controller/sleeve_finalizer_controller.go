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
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type FinalizerReconciler struct {
	*tracegen.Client
	Recorder replay.EffectRecorder
}

// Reconcile handler for tracked objects
func (r *FinalizerReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	obj := &unstructured.Unstructured{}
	kind, ok := ctx.Value(tag.CleanupKindKey{}).(string)
	if !ok {
		fmt.Println("no kind in context, returning")
		return reconcile.Result{}, errors.New("no kind in context")
	}
	obj.SetKind(kind)
	// ... fetch object
	if err := r.Get(ctx, req.NamespacedName, obj); err != nil {
		panic(fmt.Sprintf("failed to get object: %s, error: %v", req.NamespacedName, err))
		return reconcile.Result{}, fmt.Errorf("failed to get object: %w", err)
	}

	fmt.Printf("got object %s/%s deleted at %v\n", kind, req.NamespacedName, obj.GetDeletionTimestamp())

	// get the Kind out of the context
	if obj.GetDeletionTimestamp() != nil {
		// Object is being deleted

		// in simulation/replay, need to remove the object from state
		if r.Recorder != nil {
			// simulation mode
			fmt.Println("emitting REMOVE effect")
			if err := r.Recorder.RecordEffect(ctx, obj, event.REMOVE, nil); err != nil {
				fmt.Printf("failed to record effect: %v\n", err)
				return reconcile.Result{}, fmt.Errorf("failed to record effect: %w", err)
			}
			return reconcile.Result{}, nil
		}

		// Only emit REMOVE event if this is our last finalizer
		if len(obj.GetFinalizers()) == 1 && obj.GetFinalizers()[0] == tag.SleeveFinalizer {
			fmt.Println("Logging REMOVE operation")
			r.Client.LogOperation(ctx, obj, event.REMOVE) // Your event logging mechanism
		}

		// Remove our finalizer to allow actual deletion
		obj.SetFinalizers(lo.Without(obj.GetFinalizers(), tag.SleeveFinalizer))
		// Update object to remove finalizer
	}

	return reconcile.Result{}, nil
}
