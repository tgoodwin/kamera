package tracecheck

import (
	"context"
	"testing"

	"github.com/tgoodwin/kamera/pkg/replay"
	"github.com/tgoodwin/kamera/pkg/tag"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type cleanupKindCaptureReconciler struct {
	gotKind any
}

func (r *cleanupKindCaptureReconciler) Reconcile(ctx context.Context, _ reconcile.Request) (reconcile.Result, error) {
	r.gotKind = ctx.Value(tag.CleanupKindKey{})
	return reconcile.Result{}, nil
}

type noopEffects struct{}

func (noopEffects) GetEffects(_ context.Context) (Changes, error) { return Changes{}, nil }

func TestControllerRuntimeStrategy_DoesNotInjectCleanupKind(t *testing.T) {
	fm := replay.NewFrameManager(nil)
	strategy := &ControllerRuntimeStrategy{
		Reconciler:    &cleanupKindCaptureReconciler{},
		frameInserter: fm,
		effectReader:  noopEffects{},
		name:          cleanupReconcilerID,
		scheme:        runtime.NewScheme(),
	}

	frameID := "frame-1"
	reqName := types.NamespacedName{Namespace: "default", Name: "obj-1"}
	fm.InsertCacheFrame(frameID, replay.CacheFrame{
		"apps/Deployment": {
			reqName: &unstructured.Unstructured{},
		},
	})

	ctx := replay.WithFrameID(context.Background(), frameID)
	_, err := strategy.ReconcileAtState(ctx, reqName)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	r := strategy.Reconciler.(*cleanupKindCaptureReconciler)
	if r.gotKind != nil {
		t.Fatalf("expected no cleanup kind to be injected by base strategy, got %v", r.gotKind)
	}
}

func TestCleanupRuntimeStrategy_InjectsCleanupKind(t *testing.T) {
	fm := replay.NewFrameManager(nil)
	base := &ControllerRuntimeStrategy{
		Reconciler:    &cleanupKindCaptureReconciler{},
		frameInserter: fm,
		effectReader:  noopEffects{},
		name:          cleanupReconcilerID,
		scheme:        runtime.NewScheme(),
	}
	strategy := newCleanupRuntimeStrategy(base, fm)

	frameID := "frame-2"
	reqName := types.NamespacedName{Namespace: "default", Name: "obj-2"}
	fm.InsertCacheFrame(frameID, replay.CacheFrame{
		"apps/Deployment": {
			reqName: &unstructured.Unstructured{},
		},
	})

	ctx := replay.WithFrameID(context.Background(), frameID)
	_, err := strategy.ReconcileAtState(ctx, reqName)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	r := base.Reconciler.(*cleanupKindCaptureReconciler)
	if r.gotKind != "apps/Deployment" {
		t.Fatalf("expected cleanup kind apps/Deployment, got %v", r.gotKind)
	}
}
