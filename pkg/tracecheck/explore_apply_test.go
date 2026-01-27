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
