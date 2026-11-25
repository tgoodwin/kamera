package tracecheck

import (
	"testing"

	"github.com/go-logr/logr"

	"github.com/stretchr/testify/require"
	"github.com/tgoodwin/kamera/pkg/apiserver"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/replay"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/util"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var schemaWriteTestGVK = schema.GroupVersionKind{Group: "testing.kamera.io", Version: "v1", Kind: "Widget"}

func schemaWriteTestRegistry(t *testing.T) *apiserver.Registry {
	t.Helper()
	registry := apiserver.NewRegistry()
	require.NoError(t, registry.RegisterResourceSchema(schemaWriteTestGVK, true, true, &apiextensionsv1.JSONSchemaProps{
		Type: "object",
		Properties: map[string]apiextensionsv1.JSONSchemaProps{
			"spec": {
				Type: "object",
				Properties: map[string]apiextensionsv1.JSONSchemaProps{
					"first":  {Type: "string"},
					"second": {Type: "string"},
				},
			},
			"status": {
				Type: "object",
				Properties: map[string]apiextensionsv1.JSONSchemaProps{
					"phase": {Type: "string"},
				},
			},
		},
	}))
	return registry
}

func configMapTestRegistry(t *testing.T) *apiserver.Registry {
	t.Helper()
	registry := apiserver.NewRegistry()
	require.NoError(t, registry.RegisterResourceSchema(
		corev1.SchemeGroupVersion.WithKind("ConfigMap"), true, false,
		&apiextensionsv1.JSONSchemaProps{
			Type: "object",
			Properties: map[string]apiextensionsv1.JSONSchemaProps{
				"data": {
					Type: "object",
					AdditionalProperties: &apiextensionsv1.JSONSchemaPropsOrBool{
						Allows: true,
						Schema: &apiextensionsv1.JSONSchemaProps{Type: "string"},
					},
				},
			},
		},
	))
	return registry
}

func schemaWriteWidget(fields map[string]interface{}) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": schemaWriteTestGVK.GroupVersion().String(),
		"kind":       schemaWriteTestGVK.Kind,
		"metadata": map[string]interface{}{
			"name":      "example",
			"namespace": "default",
		},
	}}
	for key, value := range fields {
		obj.Object[key] = value
	}
	return obj
}

func TestManagerMaterializesRegisteredApplySynchronously(t *testing.T) {
	registry := schemaWriteTestRegistry(t)
	resourceSchema, found := registry.Lookup(schemaWriteTestGVK)
	require.True(t, found)
	live, err := resourceSchema.Create(schemaWriteWidget(map[string]interface{}{
		"spec": map[string]interface{}{"first": "one"},
	}), "initial-manager")
	require.NoError(t, err)

	store := snapshot.NewStore()
	mgr := &manager{
		versionStore: NewVersionStore(store, nil),
		effects:      make(map[string]reconcileEffects),
		scheme:       runtime.NewScheme(),
		effectRKeys:  make(map[string]util.Set[string]),
		effectIKeys:  make(map[string]util.Set[snapshot.IdentityKey]),
		effectRVs:    make(map[string]map[string]int64),

		schemaRegistry: registry,
	}
	hash := mgr.Publish(live)
	mgr.rvLookup = func(candidate snapshot.VersionHash) int64 {
		if candidate == hash {
			return 5
		}
		return 0
	}
	key := snapshot.NewCompositeKeyWithGroup(
		schemaWriteTestGVK.Group, schemaWriteTestGVK.Kind, "default", "example", tag.GetSleeveObjectID(live),
	)
	ctx := ctxWithFrame("schema-apply")
	require.NoError(t, mgr.PrepareEffectContext(ctx, ObjectVersions{key: hash}))

	stale := schemaWriteWidget(map[string]interface{}{
		"spec": map[string]interface{}{"first": "stale"},
	})
	stale.SetResourceVersion("4")
	err = mgr.RecordEffect(ctx, stale, event.UPDATE, &replay.PreconditionInfo{FieldManager: "stale-manager"}, nil)
	require.True(t, apierrors.IsConflict(err), "stale schema-backed update must conflict: %v", err)

	staleApply := schemaWriteWidget(map[string]interface{}{
		"spec": map[string]interface{}{"second": "stale"},
	})
	staleApply.SetResourceVersion("4")
	err = mgr.RecordEffect(ctx, staleApply, event.APPLY, &replay.PreconditionInfo{FieldManager: "second-manager"}, nil)
	require.True(t, apierrors.IsConflict(err), "stale schema-backed apply must conflict: %v", err)

	intent := schemaWriteWidget(map[string]interface{}{
		"spec": map[string]interface{}{"second": "two"},
	})
	err = mgr.RecordEffect(ctx, intent, event.APPLY, &replay.PreconditionInfo{FieldManager: "second-manager"}, nil)
	require.NoError(t, err)
	require.Equal(t, "one", intent.Object["spec"].(map[string]interface{})["first"])
	require.Equal(t, "two", intent.Object["spec"].(map[string]interface{})["second"])
	require.Equal(t, "6", intent.GetResourceVersion())

	changes, err := mgr.GetEffects(ctx)
	require.NoError(t, err)
	require.Len(t, changes.Effects, 1)
	require.True(t, changes.Effects[0].Materialized)
	materialized := mgr.Resolve(changes.Effects[0].Version)
	require.Equal(t, "one", materialized.Object["spec"].(map[string]interface{})["first"])
	require.Equal(t, "two", materialized.Object["spec"].(map[string]interface{})["second"])
	initialStored := mgr.Resolve(hash)
	require.Equal(t, "one", initialStored.Object["spec"].(map[string]interface{})["first"])
	require.NotContains(t, initialStored.Object["spec"].(map[string]interface{}), "second")
	require.Equal(t, int64(6), changes.Effects[0].ResourceVersion)
	require.Empty(t, materialized.GetResourceVersion(), "stored objects must not contain resourceVersion")
}

func TestManagerTreatsRepeatedApplyAcrossFramesAsNoOp(t *testing.T) {
	registry := configMapTestRegistry(t)
	mgr := newTestManager(nil)
	mgr.schemaRegistry = registry
	firstCtx := ctxWithFrame("config-create")
	require.NoError(t, mgr.PrepareEffectContext(firstCtx, ObjectVersions{}))
	first := makeObj("example", "")
	first.Object["data"] = map[string]interface{}{"key": "value"}
	require.NoError(t, mgr.RecordEffect(firstCtx, first, event.APPLY, &replay.PreconditionInfo{FieldManager: "manager"}, nil))
	firstChanges, err := mgr.GetEffects(firstCtx)
	require.NoError(t, err)
	require.Len(t, firstChanges.Effects, 1)
	firstEffect := firstChanges.Effects[0]
	mgr.CleanupEffectContext(firstCtx)

	mgr.rvLookup = func(candidate snapshot.VersionHash) int64 {
		if candidate == firstEffect.Version {
			return 1
		}
		return 0
	}
	key := snapshot.NewCompositeKeyWithGroup("", "ConfigMap", "default", "example", tag.GetSleeveObjectID(first))
	secondCtx := ctxWithFrame("config-no-op")
	require.NoError(t, mgr.PrepareEffectContext(secondCtx, ObjectVersions{key: firstEffect.Version}))
	second := makeObj("example", "")
	second.Object["data"] = map[string]interface{}{"key": "value"}
	require.NoError(t, mgr.RecordEffect(secondCtx, second, event.APPLY, &replay.PreconditionInfo{FieldManager: "manager"}, nil))
	secondChanges, err := mgr.GetEffects(secondCtx)
	require.NoError(t, err)
	require.Empty(t, secondChanges.Effects)
	require.Equal(t, first.GetResourceVersion(), second.GetResourceVersion())
	require.Equal(t, int64(1), second.GetGeneration())
}

func TestManagerKeepsDistinctSchemaVersionsForNormalizedTimestampChanges(t *testing.T) {
	mgr := newTestManager(nil)
	mgr.schemaRegistry = configMapTestRegistry(t)

	firstCtx := ctxWithFrame("timestamp-first")
	require.NoError(t, mgr.PrepareEffectContext(firstCtx, ObjectVersions{}))
	first := makeObj("example", "")
	first.Object["data"] = map[string]interface{}{"lastRun": "2026-11-23T10:00:00Z"}
	require.NoError(t, mgr.RecordEffect(firstCtx, first, event.APPLY, &replay.PreconditionInfo{FieldManager: "manager"}, nil))
	firstChanges, err := mgr.GetEffects(firstCtx)
	require.NoError(t, err)
	require.Len(t, firstChanges.Effects, 1)
	firstVersion := firstChanges.Effects[0].Version
	mgr.CleanupEffectContext(firstCtx)

	mgr.rvLookup = func(candidate snapshot.VersionHash) int64 {
		if candidate == firstVersion {
			return 1
		}
		return 0
	}

	secondCtx := ctxWithFrame("timestamp-second")
	key := firstChanges.Effects[0].Key
	require.NoError(t, mgr.PrepareEffectContext(secondCtx, ObjectVersions{key: firstVersion}))
	second := makeObj("example", "")
	second.Object["data"] = map[string]interface{}{"lastRun": "2026-11-23T10:01:00Z"}
	require.NoError(t, mgr.RecordEffect(secondCtx, second, event.APPLY, &replay.PreconditionInfo{FieldManager: "manager"}, nil))
	secondChanges, err := mgr.GetEffects(secondCtx)
	require.NoError(t, err)
	require.Len(t, secondChanges.Effects, 1)
	require.NotEqual(t, firstVersion, secondChanges.Effects[0].Version)

	firstStored := mgr.Resolve(firstVersion)
	require.Equal(t, "2026-11-23T10:00:00Z", firstStored.Object["data"].(map[string]interface{})["lastRun"])
}

func TestRegisteredMergePatchUsesFrameLiveObject(t *testing.T) {
	mgr := newTestManager(nil)
	mgr.schemaRegistry = configMapTestRegistry(t)
	ctx := ctxWithFrame("merge-patch-live")
	require.NoError(t, mgr.PrepareEffectContext(ctx, ObjectVersions{}))

	apply := makeObj("example", "")
	apply.Object["data"] = map[string]interface{}{"key": "value"}
	require.NoError(t, mgr.RecordEffect(ctx, apply, event.APPLY, &replay.PreconditionInfo{FieldManager: "manager"}, nil))
	firstChanges, err := mgr.GetEffects(ctx)
	require.NoError(t, err)
	require.Len(t, firstChanges.Effects, 1)

	patch := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]interface{}{
			"name":      "example",
			"namespace": "default",
			"labels":    map[string]interface{}{"patched": "true"},
		},
	}}
	require.NoError(t, mgr.RecordEffect(ctx, patch, event.PATCH, nil, &replay.EffectOptions{PatchType: types.MergePatchType}))
	changes, err := mgr.GetEffects(ctx)
	require.NoError(t, err)
	require.Len(t, changes.Effects, 2)
	patched := mgr.Resolve(changes.Effects[1].Version)
	require.Equal(t, "value", patched.Object["data"].(map[string]interface{})["key"])
	require.Equal(t, "true", patched.GetLabels()["patched"])
}

func TestMixedWritesShareResourceVersionSequence(t *testing.T) {
	mgr := newTestManager(nil)
	mgr.schemaRegistry = configMapTestRegistry(t)
	ctx := ctxWithFrame("mixed-writes")
	require.NoError(t, mgr.PrepareEffectContext(ctx, ObjectVersions{}))

	apply := func() *unstructured.Unstructured {
		obj := makeObj("example", "")
		obj.Object["data"] = map[string]any{"key": "value"}
		require.NoError(t, mgr.RecordEffect(ctx, obj, event.APPLY, &replay.PreconditionInfo{FieldManager: "apply-manager"}, nil))
		return obj
	}
	first := apply()
	require.Equal(t, "1", first.GetResourceVersion())

	patch := first.DeepCopy()
	patch.SetLabels(map[string]string{"added-by-patch": "true"})
	require.NoError(t, mgr.RecordEffect(ctx, patch, event.PATCH, nil, nil))
	require.Equal(t, "2", patch.GetResourceVersion())

	repeated := apply()
	require.Equal(t, "2", repeated.GetResourceVersion(), "unchanged apply must return the patch's version")
	require.Equal(t, "true", repeated.GetLabels()["added-by-patch"])

	changes, err := mgr.GetEffects(ctx)
	require.NoError(t, err)
	require.Len(t, changes.Effects, 2)
	explorer := &Explorer{versionManager: mgr}
	start := StateNode{Contents: NewStateSnapshot(ObjectVersions{}, KindSequences{}, nil)}
	next, _, events := explorer.applyEffects(logr.Discard(), start, &ReconcileResult{Changes: changes})
	require.Len(t, events, 2)
	require.Equal(t, int64(1), events[0].Sequence)
	require.Equal(t, int64(2), events[1].Sequence)
	require.Equal(t, changes.Effects[1].Version, next[changes.Effects[1].Key])
}

func TestApplyKeepsIdentityForUIDDelete(t *testing.T) {
	mgr := newTestManager(nil)
	mgr.schemaRegistry = configMapTestRegistry(t)
	ctx := ctxWithFrame("legacy-apply")
	require.NoError(t, mgr.PrepareEffectContext(ctx, ObjectVersions{}))

	applied := makeObj("example", "")
	require.NoError(t, mgr.RecordEffect(ctx, applied, event.APPLY, &replay.PreconditionInfo{FieldManager: "manager"}, nil))
	changes, err := mgr.GetEffects(ctx)
	require.NoError(t, err)
	require.Len(t, changes.Effects, 1)
	require.NotEmpty(t, applied.GetUID())
	require.Equal(t, string(applied.GetUID()), changes.Effects[0].Key.ObjectID)

	mgr.CleanupEffectContext(ctx)
	mgr.rvLookup = func(snapshot.VersionHash) int64 { return 1 }
	deleteCtx := ctxWithFrame("legacy-delete")
	require.NoError(t, mgr.PrepareEffectContext(deleteCtx, ObjectVersions{changes.Effects[0].Key: changes.Effects[0].Version}))
	uid := applied.GetUID()
	toDelete := makeObj("example", "1")
	toDelete.SetUID(uid)
	require.NoError(t, mgr.RecordEffect(deleteCtx, toDelete, event.MARK_FOR_DELETION, &replay.PreconditionInfo{UID: &uid}, nil))
}

func TestManagerStrictModeRejectsUnregisteredWrites(t *testing.T) {
	mgr := newTestManager(nil)
	mgr.schemaRegistry = apiserver.NewRegistry()
	mgr.requireSchemas = true
	ctx := ctxWithFrame("strict-schema")
	require.NoError(t, mgr.PrepareEffectContext(ctx, ObjectVersions{}))

	err := mgr.RecordEffect(ctx, makeObj("example", ""), event.APPLY, &replay.PreconditionInfo{FieldManager: "manager"}, nil)
	require.ErrorContains(t, err, "schema-backed APPLY by reconciler \"unknown\" requires a registered schema for /v1, Kind=ConfigMap")
}

func TestManagerRejectsForcedApplyWithSecondControllerOwner(t *testing.T) {
	registry := schemaWriteTestRegistry(t)
	resourceSchema, found := registry.Lookup(schemaWriteTestGVK)
	require.True(t, found)
	controller := true
	ownerA := metav1.OwnerReference{
		APIVersion: "testing.kamera.io/v1", Kind: "Owner", Name: "a", UID: types.UID("owner-a"), Controller: &controller,
	}
	ownerB := metav1.OwnerReference{
		APIVersion: "testing.kamera.io/v1", Kind: "Owner", Name: "b", UID: types.UID("owner-b"), Controller: &controller,
	}
	initial := schemaWriteWidget(nil)
	initial.SetOwnerReferences([]metav1.OwnerReference{ownerA})
	live, err := resourceSchema.Create(initial, "owner-a-manager")
	require.NoError(t, err)

	mgr := newTestManager(nil)
	mgr.schemaRegistry = registry
	hash := mgr.Publish(live)
	key := snapshot.NewCompositeKeyWithGroup(
		schemaWriteTestGVK.Group, schemaWriteTestGVK.Kind, "default", "example", tag.GetSleeveObjectID(live),
	)
	ctx := ctxWithFrame("invalid-owner-apply")
	require.NoError(t, mgr.PrepareEffectContext(ctx, ObjectVersions{key: hash}))
	second := schemaWriteWidget(nil)
	second.SetOwnerReferences([]metav1.OwnerReference{ownerB})

	err = mgr.RecordEffect(ctx, second, event.APPLY, &replay.PreconditionInfo{
		FieldManager: "owner-b-manager",
		Force:        true,
	}, nil)
	require.True(t, apierrors.IsInvalid(err), "force must not bypass merged ObjectMeta validation: %v", err)
	changes, getErr := mgr.GetEffects(ctx)
	require.NoError(t, getErr)
	require.Empty(t, changes.Effects, "invalid write must not record a durable effect")
}

func TestBuildStartStateLeavesUnregisteredFixtureWithoutOwnership(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	builder := NewExplorerBuilder(scheme).RequireSchemas()
	obj := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"}}

	_, err := builder.BuildStartStateFromObjects([]client.Object{obj}, nil)
	require.NoError(t, err)
	require.Empty(t, obj.GetManagedFields())
}

func TestBuildStartStateSeedsOwnershipAndForkRetainsSchemas(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	builder := NewExplorerBuilder(scheme).
		RequireSchemas().
		WithResourceSchema(corev1.SchemeGroupVersion.WithKind("ConfigMap"), true, false,
			&apiextensionsv1.JSONSchemaProps{Type: "object", Properties: map[string]apiextensionsv1.JSONSchemaProps{
				"data": {Type: "object", AdditionalProperties: &apiextensionsv1.JSONSchemaPropsOrBool{
					Allows: true, Schema: &apiextensionsv1.JSONSchemaProps{Type: "string"},
				}},
			}})
	fork := builder.Fork()
	obj := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"},
		Data:       map[string]string{"key": "value"},
	}

	state, err := fork.BuildStartStateFromObjects([]client.Object{obj}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, obj.GetManagedFields())
	require.Equal(t, initialFieldManager, obj.GetManagedFields()[0].Manager)
	require.Len(t, state.Objects(), 1)

	stateBuilderObj := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "event-builder", Namespace: "default"},
		Data:       map[string]string{"key": "value"},
	}
	stateBuilderState := builder.NewStateEventBuilder().AddTopLevelObject(stateBuilderObj)
	require.NotEmpty(t, stateBuilderObj.GetManagedFields())
	require.Len(t, stateBuilderState.Objects(), 1)
}
