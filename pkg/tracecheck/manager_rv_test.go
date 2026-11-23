package tracecheck

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/replay"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/util"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// newTestManager creates a manager wired for RV conflict testing.
func newTestManager(rvLookup func(snapshot.VersionHash) int64) *manager {
	store := snapshot.NewStore()
	return &manager{
		versionStore:   NewVersionStore(store, nil),
		effects:        make(map[string]reconcileEffects),
		scheme:         runtime.NewScheme(),
		effectRKeys:    make(map[string]util.Set[string]),
		effectIKeys:    make(map[string]util.Set[snapshot.IdentityKey]),
		effectRVs:      make(map[string]map[string]int64),
		effectVersions: make(map[string]map[string]snapshot.VersionHash),
		effectNextRVs:  make(map[string]int64),
		rvLookup:       rvLookup,
	}
}

func makeObj(name, rv string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion("v1")
	obj.SetKind("ConfigMap")
	obj.SetNamespace("default")
	obj.SetName(name)
	if rv != "" {
		obj.SetResourceVersion(rv)
	}
	return obj
}

func ctxWithFrame(frameID string) context.Context {
	return replay.WithFrameID(context.Background(), frameID)
}

// Test 1: Conflict on stale write
func TestResourceVersionConflict_StaleWrite(t *testing.T) {
	rvMap := make(map[snapshot.VersionHash]int64)
	mgr := newTestManager(func(vh snapshot.VersionHash) int64 { return rvMap[vh] })

	// Create an object and publish it with RV=5
	obj := makeObj("test-cm", "")
	hash := mgr.Publish(obj)
	rvMap[hash] = 5

	key := snapshot.NewCompositeKeyWithGroup("", "ConfigMap", "default", "test-cm", "obj1")
	ov := ObjectVersions{key: hash}

	ctx := ctxWithFrame("frame-1")
	require.NoError(t, mgr.PrepareEffectContext(ctx, ov))

	// Record a GET (controller reads the object, gets RV=5)
	objWithRV := makeObj("test-cm", "5")
	err := mgr.RecordEffect(ctx, objWithRV, event.GET, nil, nil)
	require.NoError(t, err)

	// Simulate another writer advancing the RV to 6
	mgr.effectRVs["frame-1"]["core/ConfigMap/default/test-cm"] = 6

	// Controller tries to UPDATE with RV=5 (stale) — should get 409
	// (RV conflict check only applies to Update, not Patch/MergePatch)
	staleObj := makeObj("test-cm", "5")
	staleObj.Object["data"] = map[string]any{"key": "value"}
	err = mgr.RecordEffect(ctx, staleObj, event.UPDATE, nil, nil)
	require.Error(t, err)
	require.True(t, apierrors.IsConflict(err), "expected Conflict error, got: %v", err)
}

// Test 2: Success on current write
func TestResourceVersionConflict_CurrentWriteSucceeds(t *testing.T) {
	rvMap := make(map[snapshot.VersionHash]int64)
	mgr := newTestManager(func(vh snapshot.VersionHash) int64 { return rvMap[vh] })

	obj := makeObj("test-cm", "")
	hash := mgr.Publish(obj)
	rvMap[hash] = 5

	key := snapshot.NewCompositeKeyWithGroup("", "ConfigMap", "default", "test-cm", "obj1")
	ov := ObjectVersions{key: hash}

	ctx := ctxWithFrame("frame-2")
	require.NoError(t, mgr.PrepareEffectContext(ctx, ov))

	// Record GET
	objWithRV := makeObj("test-cm", "5")
	err := mgr.RecordEffect(ctx, objWithRV, event.GET, nil, nil)
	require.NoError(t, err)

	// UPDATE with current RV=5 — should succeed
	updateObj := makeObj("test-cm", "5")
	updateObj.Object["data"] = map[string]any{"key": "value"}
	err = mgr.RecordEffect(ctx, updateObj, event.UPDATE, nil, nil)
	require.NoError(t, err)
}

// Test 3: Successful writes advance RV within the reconcile.
func TestResourceVersionConflict_MultiWriteSameReconcile(t *testing.T) {
	rvMap := make(map[snapshot.VersionHash]int64)
	mgr := newTestManager(func(vh snapshot.VersionHash) int64 { return rvMap[vh] })

	obj := makeObj("test-cm", "")
	hash := mgr.Publish(obj)
	rvMap[hash] = 5

	key := snapshot.NewCompositeKeyWithGroup("", "ConfigMap", "default", "test-cm", "obj1")
	ov := ObjectVersions{key: hash}

	ctx := ctxWithFrame("frame-3")
	require.NoError(t, mgr.PrepareEffectContext(ctx, ov))

	// Record GET (RV=5)
	objWithRV := makeObj("test-cm", "5")
	err := mgr.RecordEffect(ctx, objWithRV, event.GET, nil, nil)
	require.NoError(t, err)

	// First UPDATE with RV=5 succeeds and receives the API server's next RV.
	currentObj := makeObj("test-cm", "5")
	currentObj.Object["data"] = map[string]any{"step": "1"}
	err = mgr.RecordEffect(ctx, currentObj, event.UPDATE, nil, nil)
	require.NoError(t, err)
	require.Equal(t, "6", currentObj.GetResourceVersion())

	// Reusing the returned object carries RV=6 and succeeds again.
	currentObj.Object["data"] = map[string]any{"step": "2"}
	err = mgr.RecordEffect(ctx, currentObj, event.UPDATE, nil, nil)
	require.NoError(t, err)
	require.Equal(t, "7", currentObj.GetResourceVersion())

	changes, err := mgr.GetEffects(ctx)
	require.NoError(t, err)
	require.Len(t, changes.Effects, 2)
	require.Equal(t, int64(6), changes.Effects[0].ResourceVersion)
	require.Equal(t, int64(7), changes.Effects[1].ResourceVersion)

	// A separate copy retaining the original RV is now stale.
	staleObj := makeObj("test-cm", "5")
	staleObj.Object["data"] = map[string]any{"step": "3"}
	err = mgr.RecordEffect(ctx, staleObj, event.UPDATE, nil, nil)
	require.Error(t, err)
	require.True(t, apierrors.IsConflict(err), "expected Conflict for stale RV, got: %v", err)
}

func TestResourceVersionConflict_NoOpPatchDoesNotStaleRetainedObject(t *testing.T) {
	rvMap := make(map[snapshot.VersionHash]int64)
	mgr := newTestManager(func(vh snapshot.VersionHash) int64 { return rvMap[vh] })

	current := makeObj("test-cm", "")
	tag.EnsureDeterministicIdentity(current)
	currentHash := mgr.Publish(current)
	rvMap[currentHash] = 5
	key := snapshot.NewCompositeKeyWithGroup("", "ConfigMap", "default", "test-cm", "obj1")
	ctx := ctxWithFrame("frame-no-op-patch")
	require.NoError(t, mgr.PrepareEffectContext(ctx, ObjectVersions{key: currentHash}))

	retained := current.DeepCopy()
	retained.SetResourceVersion("5")
	require.NoError(t, mgr.RecordEffect(ctx, retained.DeepCopy(), event.PATCH, nil, nil))
	require.Equal(t, "5", retained.GetResourceVersion(), "no-op patch must not advance the retained object's RV")

	retained.Object["data"] = map[string]any{"updated": "true"}
	require.NoError(t, mgr.RecordEffect(ctx, retained, event.UPDATE, nil, nil))
	require.Equal(t, "6", retained.GetResourceVersion())

	changes, err := mgr.GetEffects(ctx)
	require.NoError(t, err)
	require.Len(t, changes.Effects, 1, "no-op patch must not materialize a write effect")
	require.Equal(t, event.UPDATE, changes.Effects[0].OpType)
}

func TestResourceVersionSequenceUsesBranchFrontierAfterDeletion(t *testing.T) {
	mgr := newTestManager(nil)
	ctx := withResourceVersionBase(ctxWithFrame("frame-after-delete"), 10)
	require.NoError(t, mgr.PrepareEffectContext(ctx, ObjectVersions{}))

	created := makeObj("new-cm", "")
	require.NoError(t, mgr.RecordEffect(ctx, created, event.CREATE, nil, nil))
	require.Equal(t, "11", created.GetResourceVersion())

	changes, err := mgr.GetEffects(ctx)
	require.NoError(t, err)
	require.Len(t, changes.Effects, 1)
	require.Equal(t, int64(11), changes.Effects[0].ResourceVersion)
}

func TestResourceVersionSequenceIncludesRemoval(t *testing.T) {
	mgr := newTestManager(nil)
	current := makeObj("test-cm", "")
	currentHash := mgr.Publish(current)
	key := snapshot.NewCompositeKeyWithGroup("", "ConfigMap", "default", "test-cm", "obj1")
	ctx := withResourceVersionBase(ctxWithFrame("frame-remove-create"), 5)
	require.NoError(t, mgr.PrepareEffectContext(ctx, ObjectVersions{key: currentHash}))

	require.NoError(t, mgr.RecordEffect(ctx, current.DeepCopy(), event.MARK_FOR_DELETION, nil, nil))
	require.NoError(t, mgr.RecordEffect(ctx, current.DeepCopy(), event.REMOVE, nil, nil))
	created := makeObj("test-cm", "")
	require.NoError(t, mgr.RecordEffect(ctx, created, event.CREATE, nil, nil))

	changes, err := mgr.GetEffects(ctx)
	require.NoError(t, err)
	require.Len(t, changes.Effects, 3)
	require.Equal(t, int64(6), changes.Effects[0].ResourceVersion)
	require.Equal(t, int64(7), changes.Effects[1].ResourceVersion)
	require.Equal(t, int64(8), changes.Effects[2].ResourceVersion)
	require.Equal(t, "8", created.GetResourceVersion())
}

func TestResourceVersionConflict_UpdateWithoutRVIsAccepted(t *testing.T) {
	rvMap := make(map[snapshot.VersionHash]int64)
	mgr := newTestManager(func(vh snapshot.VersionHash) int64 { return rvMap[vh] })

	obj := makeObj("test-cm", "")
	hash := mgr.Publish(obj)
	rvMap[hash] = 5

	key := snapshot.NewCompositeKeyWithGroup("", "ConfigMap", "default", "test-cm", "obj1")
	ctx := ctxWithFrame("frame-update-without-rv")
	require.NoError(t, mgr.PrepareEffectContext(ctx, ObjectVersions{key: hash}))

	err := mgr.RecordEffect(ctx, makeObj("test-cm", ""), event.UPDATE, nil, nil)
	require.NoError(t, err)
}

func TestResourceVersionConflict_AllowedKindsAcceptEmptyAndZeroRV(t *testing.T) {
	for name, rv := range map[string]string{"omitted": "", "zero": "0"} {
		t.Run(name, func(t *testing.T) {
			mgr := newTestManager(func(snapshot.VersionHash) int64 { return 5 })
			stored := makeObj("test", "")
			stored.SetKind("ReplicationController")
			hash := mgr.Publish(stored)
			key := snapshot.NewCompositeKeyWithGroup("", "ReplicationController", "default", "test", "obj")
			ctx := ctxWithFrame("frame-allowed")
			require.NoError(t, mgr.PrepareEffectContext(ctx, ObjectVersions{key: hash}))
			update := stored.DeepCopy()
			update.SetLabels(map[string]string{"changed": "true"})
			update.SetResourceVersion(rv)
			require.NoError(t, mgr.RecordEffect(ctx, update, event.UPDATE, nil, nil))
			require.Equal(t, "6", update.GetResourceVersion())
		})
	}
}

func TestResourceVersionConflict_CustomKindRequiresRV(t *testing.T) {
	mgr := newTestManager(nil)
	obj := makeObj("test", "")
	obj.SetAPIVersion("example.com/v1")
	obj.SetKind("Widget")
	hash := mgr.Publish(obj)
	key := snapshot.NewCompositeKeyWithGroup("example.com", "Widget", "default", "test", "obj")
	ctx := ctxWithFrame("frame-custom-no-rv")
	require.NoError(t, mgr.PrepareEffectContext(ctx, ObjectVersions{key: hash}))
	err := mgr.RecordEffect(ctx, obj.DeepCopy(), event.UPDATE, nil, nil)
	require.True(t, apierrors.IsInvalid(err), "expected Invalid, got %v", err)
}

// Test 5: No-RV write succeeds
func TestResourceVersionConflict_NoRVWriteSucceeds(t *testing.T) {
	rvMap := make(map[snapshot.VersionHash]int64)
	mgr := newTestManager(func(vh snapshot.VersionHash) int64 { return rvMap[vh] })

	obj := makeObj("test-cm", "")
	hash := mgr.Publish(obj)
	rvMap[hash] = 5

	key := snapshot.NewCompositeKeyWithGroup("", "ConfigMap", "default", "test-cm", "obj1")
	ov := ObjectVersions{key: hash}

	ctx := ctxWithFrame("frame-4")
	require.NoError(t, mgr.PrepareEffectContext(ctx, ov))

	// PATCH with empty resourceVersion — should succeed regardless
	patchObj := makeObj("test-cm", "")
	patchObj.Object["data"] = map[string]any{"key": "value"}
	err := mgr.RecordEffect(ctx, patchObj, event.PATCH, nil, nil)
	require.NoError(t, err)
}

// Test 5: APPLY skips RV check (SSA semantics)
func TestResourceVersionConflict_ApplySkipsRVCheck(t *testing.T) {
	rvMap := make(map[snapshot.VersionHash]int64)
	mgr := newTestManager(func(vh snapshot.VersionHash) int64 { return rvMap[vh] })

	obj := makeObj("test-cm", "")
	hash := mgr.Publish(obj)
	rvMap[hash] = 5

	key := snapshot.NewCompositeKeyWithGroup("", "ConfigMap", "default", "test-cm", "obj1")
	ov := ObjectVersions{key: hash}

	ctx := ctxWithFrame("frame-5")
	require.NoError(t, mgr.PrepareEffectContext(ctx, ov))

	// APPLY with stale RV — should succeed (SSA doesn't use RV)
	applyObj := makeObj("test-cm", "1") // deliberately stale
	applyObj.Object["data"] = map[string]any{"key": "value"}
	err := mgr.RecordEffect(ctx, applyObj, event.APPLY, nil, nil)
	require.NoError(t, err)
}

type customRVStrategy struct {
	objects  []runtime.Object
	recorder replay.EffectRecorder
}

func (s *customRVStrategy) PrepareState(ctx context.Context, objects []runtime.Object) (context.Context, func(), error) {
	s.objects = objects
	return ctx, func() {}, nil
}

func (s *customRVStrategy) ReconcileAtState(ctx context.Context, _ types.NamespacedName) (reconcile.Result, error) {
	for _, obj := range s.objects {
		u := obj.(*unstructured.Unstructured)
		if u.GetKind() == "Widget" {
			u.SetLabels(map[string]string{"updated": "true"})
			return reconcile.Result{}, s.recorder.RecordEffect(ctx, u, event.UPDATE, nil, nil)
		}
	}
	return reconcile.Result{}, nil
}

func TestCustomStrategyCanUpdateObservedResource(t *testing.T) {
	rvs := make(map[snapshot.VersionHash]int64)
	lookup := func(hash snapshot.VersionHash) int64 { return rvs[hash] }
	mgr := newTestManager(lookup)
	widget := makeObj("shared", "")
	widget.SetAPIVersion("example.com/v1")
	widget.SetKind("Widget")
	widgetHash := mgr.Publish(widget)
	cmHash := mgr.Publish(makeObj("shared", ""))
	rvs[widgetHash], rvs[cmHash] = 41, 73
	widgetKey := snapshot.NewCompositeKeyWithGroup("example.com", "Widget", "default", "shared", "widget-id")
	cmKey := snapshot.NewCompositeKeyWithGroup("", "ConfigMap", "default", "shared", "cm-id")
	state := ObjectVersions{widgetKey: widgetHash, cmKey: cmHash}
	ctx := ctxWithFrame("custom-strategy")
	require.NoError(t, mgr.PrepareEffectContext(ctx, state))
	strategy := &customRVStrategy{recorder: mgr}
	container := &ReconcilerContainer{Strategy: strategy, versionManager: mgr, effectReader: mgr, rvLookup: lookup}

	result, err := container.doReconcile(ctx, state, reconcile.Request{})
	require.NoError(t, err)
	require.Len(t, result.Changes.Effects, 1)
	effect := result.Changes.Effects[0]
	require.Equal(t, widgetKey.ResourceKey, effect.Key.ResourceKey)
	require.Equal(t, int64(74), effect.ResourceVersion)
	require.Equal(t, "true", mgr.Resolve(effect.Version).GetLabels()["updated"])
	require.Empty(t, mgr.Resolve(widgetHash).GetResourceVersion(), "must not stamp the stored object")
}
