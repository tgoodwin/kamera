package tracecheck

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/tgoodwin/kamera/pkg/replay"
	"github.com/tgoodwin/kamera/pkg/snapshot"
)

type noopStrategy struct{}

func (noopStrategy) PrepareState(ctx context.Context, _ []runtime.Object) (context.Context, func(), error) {
	return ctx, func() {}, nil
}

func (noopStrategy) ReconcileAtState(_ context.Context, _ types.NamespacedName) (reconcile.Result, error) {
	return reconcile.Result{}, nil
}

type noopEffectReader struct{}

func (noopEffectReader) GetEffects(_ context.Context) (Changes, error) {
	return Changes{}, nil
}

type noopVersionManager struct{}

func (noopVersionManager) Resolve(_ snapshot.VersionHash) *unstructured.Unstructured { return nil }
func (noopVersionManager) Publish(_ *unstructured.Unstructured) snapshot.VersionHash { return snapshot.VersionHash{} }
func (noopVersionManager) Diff(_, _ *snapshot.VersionHash) string { return "" }
func (noopVersionManager) Lookup(_ string, _ snapshot.HashStrategy) (snapshot.VersionHash, bool) {
	return snapshot.VersionHash{}, false
}
func (noopVersionManager) DebugKey(_ string) {}

func TestReconcileAtState_AllowsClusterScopedRequests(t *testing.T) {
	e := &Explorer{
		reconcilers: map[ReconcilerID]*ReconcilerContainer{
			"cluster": {
				Name:           "cluster",
				Strategy:       noopStrategy{},
				effectReader:   noopEffectReader{},
				versionManager: noopVersionManager{},
			},
		},
	}

	pr := PendingReconcile{
		ReconcilerID: "cluster",
		Request: reconcile.Request{
			NamespacedName: types.NamespacedName{Name: "widget-composition"},
		},
	}

	ctx := replay.WithFrameID(context.Background(), "frame-1")
	_, err := e.reconcileAtState(ctx, ObjectVersions{}, pr)
	require.NoError(t, err)
}
