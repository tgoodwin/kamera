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

func TestApplyEffects_StatusUpdatePreservesSpecAndGeneration(t *testing.T) {
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
				"generation": int64(7),
			},
			"spec":   map[string]any{"message": "keep-me"},
			"status": map[string]any{"phase": "old"},
		},
	}
	oldHash := vs.Publish(oldObj)

	statusOnlyObj := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"namespace": "default",
				"name":      "example",
			},
			"status": map[string]any{"phase": "new"},
		},
	}
	statusOnlyHash := vs.Publish(statusOnlyObj)

	stepResult := &ReconcileResult{
		Changes: Changes{
			ObjectVersions: ObjectVersions{key: statusOnlyHash},
			Effects: []Effect{
				{
					OpType:      event.UPDATE,
					Key:         key,
					Version:     statusOnlyHash,
					Subresource: "status",
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
	require.Equal(t, int64(7), updated.GetGeneration(), "status update must not bump generation")
	require.Equal(t, "keep-me", updated.Object["spec"].(map[string]any)["message"], "status update must preserve spec")
	require.Equal(t, "new", updated.Object["status"].(map[string]any)["phase"], "status update must replace status")
}

func TestApplyEffects_StatusApplyPreservesSpec(t *testing.T) {
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
				"generation": int64(3),
			},
			"spec":   map[string]any{"message": "keep-me"},
			"status": map[string]any{"phase": "old"},
		},
	}
	oldHash := vs.Publish(oldObj)

	statusOnlyObj := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"namespace": "default",
				"name":      "example",
			},
			"status": map[string]any{"phase": "applied"},
		},
	}
	statusOnlyHash := vs.Publish(statusOnlyObj)

	stepResult := &ReconcileResult{
		Changes: Changes{
			ObjectVersions: ObjectVersions{key: statusOnlyHash},
			Effects: []Effect{
				{
					OpType:      event.APPLY,
					Key:         key,
					Version:     statusOnlyHash,
					Subresource: "status",
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
	require.Equal(t, "keep-me", updated.Object["spec"].(map[string]any)["message"], "status apply must preserve spec")
	require.Equal(t, "applied", updated.Object["status"].(map[string]any)["phase"], "status apply must replace status")
	require.Equal(t, int64(3), updated.GetGeneration(), "status apply must not bump generation")
}

func TestApplyEffects_StatusUpdateClearsStatusWhenIncomingObjectOmitsIt(t *testing.T) {
	store := snapshot.NewStore()
	vs := NewVersionStore(store, nil)
	explorer := &Explorer{versionManager: vs}

	key := snapshot.NewCompositeKeyWithGroup("", "ConfigMap", "default", "example", "obj1")

	oldObj := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"namespace": "default",
				"name":      "example",
			},
			"spec":   map[string]any{"message": "keep-me"},
			"status": map[string]any{"phase": "old"},
		},
	}
	oldHash := vs.Publish(oldObj)

	statuslessObj := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"namespace": "default",
				"name":      "example",
			},
		},
	}
	statuslessHash := vs.Publish(statuslessObj)

	stepResult := &ReconcileResult{
		Changes: Changes{
			ObjectVersions: ObjectVersions{key: statuslessHash},
			Effects: []Effect{
				{
					OpType:      event.UPDATE,
					Key:         key,
					Version:     statuslessHash,
					Subresource: "status",
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
	_, hasStatus := updated.Object["status"]
	require.False(t, hasStatus, "status update without status must clear stored status")
	require.Equal(t, "keep-me", updated.Object["spec"].(map[string]any)["message"], "status update must preserve spec")
}

// TestApplyEffects_StateEventHashMatchesContents_Create verifies that after
// applyEffects processes a CREATE with generation 0 (which triggers a
// generation bump and re-publish), the stateEvent records the post-modification
// hash -- the same hash that ends up in Contents.contents.
//
// This is a regression test for a bug where stateEvents recorded the original
// pre-modification effect hash, causing ObserveAt replay to produce hashes
// that did not match Contents.contents or resourceVersions.
func TestApplyEffects_StateEventHashMatchesContents_Create(t *testing.T) {
	store := snapshot.NewStore()
	vs := NewVersionStore(store, nil)
	explorer := &Explorer{
		versionManager:   vs,
		resourceVersions: make(map[snapshot.VersionHash]int64),
	}

	// Build an object with generation 0. applyEffects will bump it to 1
	// and re-publish, producing a different hash than the original.
	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion("v1")
	obj.SetKind("ConfigMap")
	obj.SetNamespace("default")
	obj.SetName("cm-create")
	obj.SetGeneration(0)
	obj.Object["spec"] = map[string]any{"data": "value"}

	key := snapshot.NewCompositeKeyWithGroup("", "ConfigMap", "default", "cm-create", "obj-create")
	originalHash := vs.Publish(obj)

	stepResult := &ReconcileResult{
		FrameID: "frame-create-1",
		Changes: Changes{
			ObjectVersions: ObjectVersions{key: originalHash},
			Effects: []Effect{
				{
					OpType:  event.CREATE,
					Key:     key,
					Version: originalHash,
				},
			},
		},
	}

	state := StateNode{
		Contents: NewStateSnapshot(ObjectVersions{}, KindSequences{}, nil),
	}

	nextState, _, stateEvents := explorer.applyEffects(logr.Discard(), state, stepResult)

	// The generation bump should cause re-publish with a new hash.
	contentsHash, ok := nextState[key]
	require.True(t, ok, "expected created object to exist in next state")
	require.NotEqual(t, originalHash, contentsHash,
		"expected generation bump to produce a different hash than the original")

	// Core invariant: the stateEvent hash must match the contents hash.
	require.Len(t, stateEvents, 1, "expected exactly one stateEvent for one effect")
	eventHash := stateEvents[0].Effect.Version
	require.Equal(t, contentsHash, eventHash,
		"stateEvent hash must match the hash stored in Contents for the same key")

	// The post-modification hash must also be registered in resourceVersions.
	rv, rvPresent := explorer.resourceVersions[contentsHash]
	require.True(t, rvPresent, "expected post-modification hash to be registered in resourceVersions")
	require.Equal(t, stateEvents[0].Sequence, rv,
		"resourceVersion for the hash should equal the stateEvent sequence")
}

// TestApplyEffects_StateEventHashMatchesContents_Update verifies the same
// invariant as above but for UPDATE effects that trigger a generation bump.
func TestApplyEffects_StateEventHashMatchesContents_Update(t *testing.T) {
	store := snapshot.NewStore()
	vs := NewVersionStore(store, nil)
	explorer := &Explorer{
		versionManager:   vs,
		resourceVersions: make(map[snapshot.VersionHash]int64),
	}

	// Existing object in state with generation 0 and a spec field.
	oldObj := &unstructured.Unstructured{}
	oldObj.SetAPIVersion("v1")
	oldObj.SetKind("ConfigMap")
	oldObj.SetNamespace("default")
	oldObj.SetName("cm-update")
	oldObj.SetGeneration(0)
	oldObj.Object["spec"] = map[string]any{"version": "v1"}

	key := snapshot.NewCompositeKeyWithGroup("", "ConfigMap", "default", "cm-update", "obj-update")
	oldHash := vs.Publish(oldObj)

	// New object changes the spec (triggers generation bump).
	newObj := oldObj.DeepCopy()
	newObj.Object["spec"] = map[string]any{"version": "v2"}
	newObj.SetGeneration(0) // controller doesn't set generation; applyEffects should set it to 1
	newHash := vs.Publish(newObj)

	stepResult := &ReconcileResult{
		FrameID: "frame-update-1",
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

	nextState, _, stateEvents := explorer.applyEffects(logr.Discard(), state, stepResult)

	contentsHash, ok := nextState[key]
	require.True(t, ok, "expected updated object to exist in next state")
	require.NotEqual(t, newHash, contentsHash,
		"expected generation bump to re-publish with a different hash")

	// Core invariant: stateEvent hash == contents hash.
	require.Len(t, stateEvents, 1)
	require.Equal(t, contentsHash, stateEvents[0].Effect.Version,
		"stateEvent hash must match the hash stored in Contents for UPDATE")

	// Verify resourceVersions registration.
	rv, rvPresent := explorer.resourceVersions[contentsHash]
	require.True(t, rvPresent, "expected post-modification hash in resourceVersions")
	require.Equal(t, stateEvents[0].Sequence, rv)
}

// TestApplyEffects_StateEventHashMatchesContents_Apply verifies the invariant
// for APPLY effects (server-side apply with upsert semantics) when the object
// does not yet exist (create path).
func TestApplyEffects_StateEventHashMatchesContents_Apply(t *testing.T) {
	store := snapshot.NewStore()
	vs := NewVersionStore(store, nil)
	explorer := &Explorer{
		versionManager:   vs,
		resourceVersions: make(map[snapshot.VersionHash]int64),
	}

	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion("v1")
	obj.SetKind("Secret")
	obj.SetNamespace("default")
	obj.SetName("sec-apply")
	obj.SetGeneration(0)
	obj.Object["spec"] = map[string]any{"key": "secret-value"}

	key := snapshot.NewCompositeKeyWithGroup("", "Secret", "default", "sec-apply", "obj-apply")
	originalHash := vs.Publish(obj)

	stepResult := &ReconcileResult{
		FrameID: "frame-apply-1",
		Changes: Changes{
			ObjectVersions: ObjectVersions{key: originalHash},
			Effects: []Effect{
				{
					OpType:  event.APPLY,
					Key:     key,
					Version: originalHash,
				},
			},
		},
	}

	state := StateNode{
		Contents: NewStateSnapshot(ObjectVersions{}, KindSequences{}, nil),
	}

	nextState, _, stateEvents := explorer.applyEffects(logr.Discard(), state, stepResult)

	contentsHash := nextState[key]
	require.NotEqual(t, originalHash, contentsHash,
		"expected generation bump on APPLY create to re-publish")

	require.Len(t, stateEvents, 1)
	require.Equal(t, contentsHash, stateEvents[0].Effect.Version,
		"stateEvent hash must match the hash stored in Contents for APPLY")

	rv, rvPresent := explorer.resourceVersions[contentsHash]
	require.True(t, rvPresent)
	require.Equal(t, stateEvents[0].Sequence, rv)
}

// TestApplyEffects_StateEventHashMatchesContents_MultipleEffects verifies the
// invariant holds when applyEffects processes multiple effects in a single step,
// each of which triggers a generation bump.
func TestApplyEffects_StateEventHashMatchesContents_MultipleEffects(t *testing.T) {
	store := snapshot.NewStore()
	vs := NewVersionStore(store, nil)
	explorer := &Explorer{
		versionManager:   vs,
		resourceVersions: make(map[snapshot.VersionHash]int64),
	}

	// Two objects, both with generation 0, both being created.
	objA := &unstructured.Unstructured{}
	objA.SetAPIVersion("v1")
	objA.SetKind("ConfigMap")
	objA.SetNamespace("default")
	objA.SetName("cm-a")
	objA.SetGeneration(0)
	objA.Object["spec"] = map[string]any{"id": "a"}

	objB := &unstructured.Unstructured{}
	objB.SetAPIVersion("v1")
	objB.SetKind("ConfigMap")
	objB.SetNamespace("default")
	objB.SetName("cm-b")
	objB.SetGeneration(0)
	objB.Object["spec"] = map[string]any{"id": "b"}

	keyA := snapshot.NewCompositeKeyWithGroup("", "ConfigMap", "default", "cm-a", "obj-a")
	keyB := snapshot.NewCompositeKeyWithGroup("", "ConfigMap", "default", "cm-b", "obj-b")
	hashA := vs.Publish(objA)
	hashB := vs.Publish(objB)

	stepResult := &ReconcileResult{
		FrameID: "frame-multi-1",
		Changes: Changes{
			ObjectVersions: ObjectVersions{keyA: hashA, keyB: hashB},
			Effects: []Effect{
				{OpType: event.CREATE, Key: keyA, Version: hashA},
				{OpType: event.CREATE, Key: keyB, Version: hashB},
			},
		},
	}

	state := StateNode{
		Contents: NewStateSnapshot(ObjectVersions{}, KindSequences{}, nil),
	}

	nextState, _, stateEvents := explorer.applyEffects(logr.Discard(), state, stepResult)

	require.Len(t, stateEvents, 2, "expected two stateEvents for two effects")

	for _, se := range stateEvents {
		contentsHash, ok := nextState[se.Effect.Key]
		require.True(t, ok, "stateEvent key %s must exist in next state", se.Effect.Key)
		require.Equal(t, contentsHash, se.Effect.Version,
			"stateEvent hash must match Contents hash for key %s", se.Effect.Key)

		rv, rvPresent := explorer.resourceVersions[contentsHash]
		require.True(t, rvPresent,
			"post-modification hash must be in resourceVersions for key %s", se.Effect.Key)
		require.Equal(t, se.Sequence, rv,
			"resourceVersion must match stateEvent sequence for key %s", se.Effect.Key)
	}
}
