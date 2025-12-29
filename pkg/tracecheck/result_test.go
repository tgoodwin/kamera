package tracecheck

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestExplorerObjects(t *testing.T) {
	store := snapshot.NewStore()
	resolver := NewVersionStore(store, runtime.NewScheme())
	explorer := &Explorer{versionManager: resolver}

	makePod := func(name, objID string) (*unstructured.Unstructured, snapshot.CompositeKey, snapshot.VersionHash) {
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(schema.GroupVersionKind{Version: "v1", Kind: "Pod"})
		obj.SetNamespace("default")
		obj.SetName(name)
		obj.SetLabels(map[string]string{"tracey-uid": objID})
		vHash := resolver.Publish(obj.DeepCopy())
		key := snapshot.NewCompositeKeyWithGroup("", obj.GetKind(), obj.GetNamespace(), obj.GetName(), objID)
		return obj, key, vHash
	}

	podA, keyA, hashA := makePod("pod-a", "a-uid")
	podB, keyB, hashB := makePod("pod-b", "b-uid")

	contents := ObjectVersions{
		keyA: hashA,
		keyB: hashB,
	}
	kindSeq := KindSequences{
		util.CanonicalGroupKind("", "Pod"): 2,
	}

	state := StateNode{
		Contents: NewStateSnapshot(contents, kindSeq, nil),
	}
	resultState := ResultState{State: state}

	objects := explorer.Objects(resultState)
	names := make(map[string]struct{})
	for _, obj := range objects {
		names[obj.GetName()] = struct{}{}
	}

	require.Len(t, names, 2)
	_, hasA := names[podA.GetName()]
	_, hasB := names[podB.GetName()]
	require.True(t, hasA, "expected pod-a in results")
	require.True(t, hasB, "expected pod-b in results")
}

func TestExplorerObjectsNilResolver(t *testing.T) {
	explorer := &Explorer{}
	resultState := ResultState{}

	objects := explorer.Objects(resultState)
	require.Empty(t, objects)
}
