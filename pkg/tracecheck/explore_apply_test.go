package tracecheck

import (
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestApplyEffects_ApplyCreatesObject(t *testing.T) {
	store := snapshot.NewStore()
	vs := NewVersionStore(store, nil)
	explorer := &Explorer{versionManager: vs}

	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion("v1")
	obj.SetKind("ConfigMap")
	obj.SetNamespace("default")
	obj.SetName("example")
	obj.Object["spec"] = map[string]any{"message": "hello"}

	key := snapshot.NewCompositeKeyWithGroup("", "ConfigMap", "default", "example", "obj1")
	hash := vs.Publish(obj)

	stepResult := &ReconcileResult{
		Changes: Changes{
			ObjectVersions: ObjectVersions{key: hash},
			Effects: []Effect{
				{
					OpType:  event.APPLY,
					Key:     key,
					Version: hash,
				},
			},
		},
	}

	state := StateNode{
		Contents: NewStateSnapshot(ObjectVersions{}, KindSequences{}, nil),
	}

	nextState, _, _ := explorer.applyEffects(logr.Discard(), state, stepResult)

	updated, ok := nextState[key]
	require.True(t, ok, "expected applied object to exist in next state")

	applied := vs.Resolve(updated)
	require.NotNil(t, applied, "expected applied object to resolve")
	require.Equal(t, int64(1), applied.GetGeneration(), "expected generation to be set on apply create")
}

func TestApplyEffects_UpdateIncrementsGenerationWhenKeyIdentityChanges(t *testing.T) {
	store := snapshot.NewStore()
	vs := NewVersionStore(store, nil)
	explorer := &Explorer{versionManager: vs}

	oldObj := &unstructured.Unstructured{}
	oldObj.SetAPIVersion("v1")
	oldObj.SetKind("ConfigMap")
	oldObj.SetNamespace("default")
	oldObj.SetName("example")
	oldObj.SetGeneration(3)
	oldObj.Object["spec"] = map[string]any{"message": "old"}

	oldKey := snapshot.NewCompositeKeyWithGroup("", "ConfigMap", "default", "example", "obj-old")
	oldHash := vs.Publish(oldObj)

	newObj := oldObj.DeepCopy()
	newObj.SetGeneration(0)
	newObj.Object["spec"] = map[string]any{"message": "new"}

	newKey := snapshot.NewCompositeKeyWithGroup("", "ConfigMap", "default", "example", "obj-new")
	newHash := vs.Publish(newObj)

	stepResult := &ReconcileResult{
		Changes: Changes{
			ObjectVersions: ObjectVersions{newKey: newHash},
			Effects: []Effect{
				{
					OpType:  event.UPDATE,
					Key:     newKey,
					Version: newHash,
				},
			},
		},
	}

	state := StateNode{
		Contents: NewStateSnapshot(
			ObjectVersions{oldKey: oldHash},
			KindSequences{oldKey.CanonicalGroupKind(): 1},
			nil,
		),
	}

	nextState, _, _ := explorer.applyEffects(logr.Discard(), state, stepResult)

	_, oldStillPresent := nextState[oldKey]
	require.False(t, oldStillPresent, "expected old key to be replaced")

	updatedHash, ok := nextState[newKey]
	require.True(t, ok, "expected updated object to exist under new key")
	updated := vs.Resolve(updatedHash)
	require.NotNil(t, updated, "expected updated object to resolve")
	require.Equal(t, int64(4), updated.GetGeneration(), "expected generation increment from old object")
	require.Equal(t, "new", updated.Object["spec"].(map[string]any)["message"])
}

func TestApplyEffects_UpdateIncrementsGenerationFromNumericMetadataField(t *testing.T) {
	store := snapshot.NewStore()
	vs := NewVersionStore(store, nil)
	explorer := &Explorer{versionManager: vs}

	key := snapshot.NewCompositeKeyWithGroup("", "ConfigMap", "default", "example", "obj1")

	oldObj := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"namespace":  "default",
				"name":       "example",
				"generation": float64(5),
			},
			"spec": map[string]any{"message": "old"},
		},
	}
	oldHash := vs.Publish(oldObj)

	newObj := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"namespace": "default",
				"name":      "example",
			},
			"spec": map[string]any{"message": "new"},
		},
	}
	newHash := vs.Publish(newObj)

	stepResult := &ReconcileResult{
		Changes: Changes{
			ObjectVersions: ObjectVersions{key: newHash},
			Effects: []Effect{
				{
					OpType:  event.UPDATE,
					Key:     key,
					Version: newHash,
				},
			},
		},
	}

	state := StateNode{
		Contents: NewStateSnapshot(
			ObjectVersions{key: oldHash},
			KindSequences{key.CanonicalGroupKind(): 1},
			nil,
		),
	}

	nextState, _, _ := explorer.applyEffects(logr.Discard(), state, stepResult)

	updatedHash, ok := nextState[key]
	require.True(t, ok, "expected updated object to exist")
	updated := vs.Resolve(updatedHash)
	require.NotNil(t, updated, "expected updated object to resolve")
	require.Equal(t, int64(6), updated.GetGeneration(), "expected generation increment from numeric metadata field")
}

func TestApplyEffects_ApplyIncrementsGenerationWhenKeyIdentityChanges(t *testing.T) {
	store := snapshot.NewStore()
	vs := NewVersionStore(store, nil)
	explorer := &Explorer{versionManager: vs}

	oldObj := &unstructured.Unstructured{}
	oldObj.SetAPIVersion("v1")
	oldObj.SetKind("ConfigMap")
	oldObj.SetNamespace("default")
	oldObj.SetName("example")
	oldObj.SetGeneration(3)
	oldObj.Object["spec"] = map[string]any{"message": "old"}

	oldKey := snapshot.NewCompositeKeyWithGroup("", "ConfigMap", "default", "example", "obj-old")
	oldHash := vs.Publish(oldObj)

	newObj := oldObj.DeepCopy()
	newObj.SetGeneration(0)
	newObj.Object["spec"] = map[string]any{"message": "new"}

	newKey := snapshot.NewCompositeKeyWithGroup("", "ConfigMap", "default", "example", "obj-new")
	newHash := vs.Publish(newObj)

	stepResult := &ReconcileResult{
		Changes: Changes{
			ObjectVersions: ObjectVersions{newKey: newHash},
			Effects: []Effect{
				{
					OpType:  event.APPLY,
					Key:     newKey,
					Version: newHash,
				},
			},
		},
	}

	state := StateNode{
		Contents: NewStateSnapshot(
			ObjectVersions{oldKey: oldHash},
			KindSequences{oldKey.CanonicalGroupKind(): 1},
			nil,
		),
	}

	nextState, _, _ := explorer.applyEffects(logr.Discard(), state, stepResult)

	_, oldStillPresent := nextState[oldKey]
	require.False(t, oldStillPresent, "expected old key to be replaced")

	updatedHash, ok := nextState[newKey]
	require.True(t, ok, "expected applied object to exist under new key")
	updated := vs.Resolve(updatedHash)
	require.NotNil(t, updated, "expected applied object to resolve")
	require.Equal(t, int64(4), updated.GetGeneration(), "expected generation increment from old object")
	require.Equal(t, "new", updated.Object["spec"].(map[string]any)["message"])
}
