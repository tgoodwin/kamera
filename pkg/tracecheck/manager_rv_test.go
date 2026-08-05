package tracecheck

import (
	"context"
	"strconv"
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

func TestResourceVersionConflict_UpdateRequiresRV(t *testing.T) {
	rvMap := make(map[snapshot.VersionHash]int64)
	mgr := newTestManager(func(vh snapshot.VersionHash) int64 { return rvMap[vh] })

	obj := makeObj("test-cm", "")
	hash := mgr.Publish(obj)
	rvMap[hash] = 5

	key := snapshot.NewCompositeKeyWithGroup("", "ConfigMap", "default", "test-cm", "obj1")
	ctx := ctxWithFrame("frame-update-without-rv")
	require.NoError(t, mgr.PrepareEffectContext(ctx, ObjectVersions{key: hash}))

	err := mgr.RecordEffect(ctx, makeObj("test-cm", ""), event.UPDATE, nil, nil)
	require.Error(t, err)
	require.True(t, apierrors.IsInvalid(err), "expected Invalid for Update without RV, got: %v", err)
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

func TestResourceVersionStamping_DistinguishesKindsWithSameName(t *testing.T) {
	nn := types.NamespacedName{Namespace: "default", Name: "shared"}
	configMap := makeObj("shared", "")
	secret := makeObj("shared", "")
	secret.SetKind("Secret")

	configMapHash := snapshot.VersionHash{Value: "config-map"}
	secretHash := snapshot.VersionHash{Value: "secret"}
	versions := map[snapshot.VersionHash]int64{
		configMapHash: 41,
		secretHash:    73,
	}
	observable := ObjectVersions{
		snapshot.NewCompositeKeyWithGroup("", "ConfigMap", nn.Namespace, nn.Name, "cm-id"):  configMapHash,
		snapshot.NewCompositeKeyWithGroup("", "Secret", nn.Namespace, nn.Name, "secret-id"): secretHash,
	}
	frame := replay.CacheFrame{
		"core/ConfigMap": {nn: configMap},
		"core/Secret":    {nn: secret},
	}

	stampCacheFrameResourceVersions(frame, observable, func(hash snapshot.VersionHash) int64 {
		return versions[hash]
	})

	require.Equal(t, "41", configMap.GetResourceVersion())
	require.Equal(t, "73", secret.GetResourceVersion())
}
