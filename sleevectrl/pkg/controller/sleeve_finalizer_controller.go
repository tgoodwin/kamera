package controller

import (
	"context"
	"errors"
	"fmt"

	"github.com/samber/lo"
	"github.com/tgoodwin/kamera/pkg/replay"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type FinalizerReconciler struct {
	Client   client.Client
	Recorder replay.EffectRecorder
}

func cleanupEligibleForRemoval(obj *unstructured.Unstructured) bool {
	deletionTS := obj.GetDeletionTimestamp()
	if deletionTS == nil || deletionTS.IsZero() {
		return false
	}
	return len(obj.GetFinalizers()) == 0
}

func setObjectTypeMetaFromCanonicalKind(obj *unstructured.Unstructured, canonicalKind string) error {
	gk := util.ParseGroupKind(canonicalKind)
	if gk.Kind == "" {
		return fmt.Errorf("canonical kind %q has empty Kind", canonicalKind)
	}
	obj.SetKind(gk.Kind)
	if gk.Group == "" {
		obj.SetAPIVersion("v1")
		return nil
	}
	obj.SetAPIVersion(gk.Group + "/v1")
	return nil
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
	if err := setObjectTypeMetaFromCanonicalKind(obj, canonicalKind); err != nil {
		return reconcile.Result{}, err
	}
	kind := obj.GetKind()
	if err := r.Client.Get(ctx, req.NamespacedName, obj); err != nil {
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
			if !cleanupEligibleForRemoval(obj) {
				logger.V(1).Info("object still blocked by finalizers, skipping REMOVE")
				return reconcile.Result{}, nil
			}
			// simulation mode
			logger.V(1).Info("recording REMOVE effect")
			remover, ok := r.Client.(interface {
				Remove(context.Context, client.Object) error
			})
			if !ok {
				return reconcile.Result{}, fmt.Errorf("cleanup client does not support Remove")
			}
			if err := remover.Remove(ctx, obj); err != nil {
				logger.Error(err, "recording effect")
				return reconcile.Result{}, fmt.Errorf("recording effect: %w", err)
			}
			return reconcile.Result{}, nil
		}

		// in production mode, we use a custom finalizer to ensure the below LogOperation
		// event runs as close to the actual deletion as possible.
		if len(obj.GetFinalizers()) == 1 && obj.GetFinalizers()[0] == tag.SleeveFinalizer {
			// Remove our finalizer to allow actual deletion by the APIServer
			obj.SetFinalizers(lo.Without(obj.GetFinalizers(), tag.SleeveFinalizer))
			// Update object to remove finalizer and trigger removal
			if err := r.Client.Update(ctx, obj); err != nil {
				logger.Error(err, "failed to update object")
				return reconcile.Result{}, fmt.Errorf("failed to update object: %w", err)
			}
			logger.V(1).Info("Finalizer removed")
		}
	}

	return reconcile.Result{}, nil
}
