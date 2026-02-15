package tracecheck

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"k8s.io/apimachinery/pkg/types"
)

func TestMergeStateNodes(t *testing.T) {
	newCompositeKey := func(kind, namespace, name string) snapshot.CompositeKey {
		return snapshot.CompositeKey{
			IdentityKey: snapshot.IdentityKey{
				Kind:     kind,
				ObjectID: name,
			},
			ResourceKey: snapshot.ResourceKey{
				Kind:      kind,
				Namespace: namespace,
				Name:      name,
			},
		}
	}

	newPending := func(id, namespace, name string) PendingReconcile {
		return PendingReconcile{
			ReconcilerID: ReconcilerID(id),
			Request: reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: namespace,
					Name:      name,
				},
			},
		}
	}

	keyA := newCompositeKey("KindA", "default", "obj-a")
	keyB := newCompositeKey("KindB", "default", "obj-b")

	hashA1 := snapshot.VersionHash{Value: "hash-a1"}
	hashA2 := snapshot.VersionHash{Value: "hash-a2"}
	hashB1 := snapshot.VersionHash{Value: "hash-b1"}

	primary := StateNode{
		Contents: StateSnapshot{
			contents: ObjectVersions{
				keyA: hashA1,
			},
			KindSequences: KindSequences{
				keyA.CanonicalGroupKind(): 5,
			},
		},
		PendingReconciles: []PendingReconcile{
			newPending("reconciler-1", "default", "one"),
			newPending("reconciler-2", "default", "two"),
		},
	}

	secondary := StateNode{
		Contents: StateSnapshot{
			contents: ObjectVersions{
				keyA: hashA2,
				keyB: hashB1,
			},
			KindSequences: KindSequences{
				keyA.CanonicalGroupKind(): 6,
				keyB.CanonicalGroupKind(): 7,
			},
		},
		PendingReconciles: []PendingReconcile{
			newPending("reconciler-1", "default", "one"),
			newPending("reconciler-3", "default", "three"),
		},
	}

	merged := MergeStateNodes(primary, secondary)

	require.Equal(t, hashA2, merged.Objects()[keyA])
	require.Equal(t, hashB1, merged.Objects()[keyB])
	assert.ElementsMatch(t, []PendingReconcile{
		newPending("reconciler-1", "default", "one"),
		newPending("reconciler-2", "default", "two"),
		newPending("reconciler-3", "default", "three"),
	}, merged.PendingReconciles)
	assert.Equal(t, KindSequences{
		keyA.CanonicalGroupKind(): 5,
		keyB.CanonicalGroupKind(): 7,
	}, merged.Contents.KindSequences)
}
