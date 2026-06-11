package tracecheck

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/replay"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/util"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

// newTestManager creates a manager wired for RV conflict testing.
func newTestManager(rvLookup func(snapshot.VersionHash) int64) *manager {
	store := snapshot.NewStore()
	return &manager{
		versionStore: NewVersionStore(store, nil),
		effects:      make(map[string]reconcileEffects),
		scheme:       runtime.NewScheme(),
		effectRKeys:  make(map[string]util.Set[string]),
		effectIKeys:  make(map[string]util.Set[snapshot.IdentityKey]),
		effectRVs:    make(map[string]map[string]int64),
		rvLookup:     rvLookup,
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

// Test 3: Multi-write within same reconcile — all succeed against same baseline
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

	// First UPDATE with RV=5 — succeeds
	patch1 := makeObj("test-cm", "5")
	patch1.Object["data"] = map[string]any{"step": "1"}
	err = mgr.RecordEffect(ctx, patch1, event.UPDATE, nil, nil)
	require.NoError(t, err)

	// Second UPDATE also with RV=5 — succeeds (RV doesn't advance within a frame;
	// all writes in the same reconcile are checked against the same ground truth baseline)
	patch2 := makeObj("test-cm", "5")
	patch2.Object["data"] = map[string]any{"step": "2"}
	err = mgr.RecordEffect(ctx, patch2, event.UPDATE, nil, nil)
	require.NoError(t, err)

	// UPDATE with wrong RV=99 — should fail
	patch3 := makeObj("test-cm", "99")
	patch3.Object["data"] = map[string]any{"step": "3"}
	err = mgr.RecordEffect(ctx, patch3, event.UPDATE, nil, nil)
	require.Error(t, err)
	require.True(t, apierrors.IsConflict(err), "expected Conflict for wrong RV, got: %v", err)
}

// Test 4: No-RV write succeeds
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

// Test 6: RV stamping in doReconcile
func TestResourceVersionStamping_DoReconcile(t *testing.T) {
	store := snapshot.NewStore()
	vs := NewVersionStore(store, nil)

	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion("v1")
	obj.SetKind("ConfigMap")
	obj.SetNamespace("default")
	obj.SetName("test-cm")
	hash := vs.Publish(obj)

	rvMap := map[snapshot.VersionHash]int64{hash: 42}

	container := &ReconcilerContainer{
		Name:           "test",
		versionManager: vs,
		rvLookup: func(vh snapshot.VersionHash) int64 {
			return rvMap[vh]
		},
	}

	key := snapshot.NewCompositeKeyWithGroup("", "ConfigMap", "default", "test-cm", "obj1")
	ov := ObjectVersions{key: hash}

	// Resolve objects as doReconcile would
	var objects []runtime.Object
	for _, h := range ov {
		resolved := container.versionManager.Resolve(h)
		require.NotNil(t, resolved)
		if container.rvLookup != nil {
			if rv := container.rvLookup(h); rv > 0 {
				resolved.SetResourceVersion(strconv.FormatInt(rv, 10))
			}
		}
		objects = append(objects, resolved)
	}

	require.Len(t, objects, 1)
	u := objects[0].(*unstructured.Unstructured)
	require.Equal(t, "42", u.GetResourceVersion(), "expected RV=42 stamped on served object")
}
