package controller

import (
	"context"

	"github.com/samber/lo"
	tracegen "github.com/tgoodwin/sleeve/pkg/client"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/tag"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type FinalizerReconciler struct {
	tracegen.Client
}

// Reconcile handler for tracked objects
func (r *FinalizerReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	obj := &unstructured.Unstructured{}
	// ... fetch object

	if obj.GetDeletionTimestamp() != nil {
		// Object is being deleted

		// Only emit REMOVE event if this is our last finalizer
		if len(obj.GetFinalizers()) == 1 && obj.GetFinalizers()[0] == tag.SleeveFinalizer {
			// Log a REMOVE operation event for tracing
			// e := event.Event{
			// 	ID:           uuid.New().String(),
			// 	Timestamp:    event.FormatTimeStr(time.Now()),
			// 	ReconcileID:  getReconcileID(ctx),
			// 	ControllerID: "system-finalizer",
			// 	OpType:       string(event.REMOVE),
			// 	Kind:         obj.GetKind(),
			// 	ObjectID:     string(obj.GetUID()),
			// 	Version:      obj.GetResourceVersion(),
			// 	Labels:       obj.GetLabels(),
			// }
			r.Client.LogOperation(ctx, obj, event.REMOVE) // Your event logging mechanism
		}

		// Remove our finalizer to allow actual deletion
		obj.SetFinalizers(lo.Without(obj.GetFinalizers(), tag.SleeveFinalizer))
		// Update object to remove finalizer
	}

	return reconcile.Result{}, nil
}
