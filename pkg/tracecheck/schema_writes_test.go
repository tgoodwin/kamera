package tracecheck

import (
	"testing"

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
		versionStore:   NewVersionStore(store, nil),
		effects:        make(map[string]reconcileEffects),
		scheme:         runtime.NewScheme(),
		effectRKeys:    make(map[string]util.Set[string]),
		effectIKeys:    make(map[string]util.Set[snapshot.IdentityKey]),
		effectRVs:      make(map[string]map[string]int64),
		effectObjects:  make(map[string]map[string]*unstructured.Unstructured),
		effectNextRV:   make(map[string]int64),
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
	require.Equal(t, "6", materialized.GetResourceVersion())
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
	require.Len(t, secondChanges.Effects, 1)
	require.Equal(t, firstEffect.Version, secondChanges.Effects[0].Version)
	require.Equal(t, int64(1), second.GetGeneration())
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

func TestBuildStartStateSeedsOwnershipAndStrictlyRequiresSchemas(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	builder := NewExplorerBuilder(scheme).RequireSchemas()
	obj := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"}}

	_, err := builder.BuildStartStateFromObjects([]client.Object{obj}, nil)
	require.ErrorContains(t, err, "requires a registered schema for /v1, Kind=ConfigMap")
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
