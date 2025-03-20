package test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/test/integration/controller"
	"github.com/tgoodwin/sleeve/pkg/tracecheck"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// CreateZookeeperObject creates a ZookeeperCluster unstructured object
func CreateZookeeperObject(name, namespace, uid string, size int64, deletionTimestamp *metav1.Time) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "zookeeper.pravega.io",
		Version: "v1beta1",
		Kind:    "ZookeeperCluster",
	})
	obj.SetName(name)
	obj.SetNamespace(namespace)
	obj.SetUID(types.UID(uid))
	obj.SetLabels(map[string]string{
		"tracey-uid":                       uid,
		"discrete.events/sleeve-object-id": uid,
	})
	obj.SetDeletionTimestamp(deletionTimestamp)

	_ = unstructured.SetNestedField(obj.Object, size, "spec", "size")

	return obj
}

// CreatePVCObject creates a PVC unstructured object
func CreatePVCObject(name, namespace, uid string, zkName string, ownerRef []metav1.OwnerReference) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "",
		Version: "v1",
		Kind:    "PersistentVolumeClaim",
	})
	obj.SetName(name)
	obj.SetNamespace(namespace)
	obj.SetUID(types.UID(uid))
	obj.SetLabels(map[string]string{
		"app":                              zkName, // This is the key label that connects to ZK
		"tracey-uid":                       uid,
		"discrete.events/sleeve-object-id": uid,
	})

	if ownerRef != nil {
		obj.SetOwnerReferences(ownerRef)
	}

	_ = unstructured.SetNestedField(obj.Object, "10Gi", "spec", "resources", "requests", "storage")
	_ = unstructured.SetNestedField(obj.Object, "standard", "spec", "storageClassName")

	return obj
}

// TestZookeeperControllerStalenessIssue tests the scenario described in the GitHub issue
func TestZookeeperControllerStalenessIssue(t *testing.T) {
	// Set up scheme with our test types
	scheme := runtime.NewScheme()

	eb := tracecheck.NewExplorerBuilder(scheme)
	eb.WithReconciler("ZookeeperReconciler", func(c tracecheck.Client) tracecheck.Reconciler {
		return &controller.ZookeeperReconciler{
			Client: c,
			Scheme: scheme,
		}
	})

	eb.WithResourceDep("ZookeeperCluster", "ZookeeperReconciler")
	eb.WithResourceDep("PersistentVolumeClaim", "ZookeeperReconciler")
	eb.AssignReconcilerToKind("ZookeeperReconciler", "ZookeeperCluster")

	emitter := event.NewInMemoryEmitter()
	eb.WithEmitter(emitter)

	stateBuilder := eb.NewStateEventBuilder()

	// Create a state builder to model our sequence of events

	// 1. First ZookeeperCluster is created
	zk1 := CreateZookeeperObject("zk-cluster", "default", "zk-old-uid", 3, nil)
	zk1OwnerRef := metav1.OwnerReference{
		APIVersion: "zookeeper.pravega.io/v1beta1",
		Kind:       "ZookeeperCluster",
		Name:       "zk-cluster",
		UID:        zk1.GetUID(),
	}
	stateBuilder.AddStateEvent("ZookeeperCluster", "zk-old-uid", zk1, event.CREATE, "ZookeeperReconciler")

	// 2. PVCs are created for the first ZK
	pvc1 := CreatePVCObject("zk-cluster-pvc-0", "default", "pvc-uid-1", "zk-cluster", []metav1.OwnerReference{zk1OwnerRef})
	stateBuilder.AddStateEvent("PersistentVolumeClaim", "pvc-uid-1", pvc1, event.CREATE, "ZookeeperReconciler")

	pvc2 := CreatePVCObject("zk-cluster-pvc-1", "default", "pvc-uid-2", "zk-cluster", []metav1.OwnerReference{zk1OwnerRef})
	stateBuilder.AddStateEvent("PersistentVolumeClaim", "pvc-uid-2", pvc2, event.CREATE, "ZookeeperReconciler")

	pvc3 := CreatePVCObject("zk-cluster-pvc-2", "default", "pvc-uid-3", "zk-cluster", []metav1.OwnerReference{zk1OwnerRef})
	stateBuilder.AddStateEvent("PersistentVolumeClaim", "pvc-uid-3", pvc3, event.CREATE, "ZookeeperReconciler")

	// 3. First ZK is marked for deletion
	deletionTime := metav1.NewTime(time.Now())
	zk1WithDeletion := CreateZookeeperObject("zk-cluster", "default", "zk-old-uid", 3, &deletionTime)
	stateBuilder.AddStateEvent("ZookeeperCluster", "zk-old-uid", zk1WithDeletion, event.UPDATE, "ZookeeperReconciler")

	// 4. PVCs are deleted during deletion process
	stateBuilder.AddStateEvent("PersistentVolumeClaim", "pvc-uid-1", pvc1, event.DELETE, "ZookeeperReconciler")
	stateBuilder.AddStateEvent("PersistentVolumeClaim", "pvc-uid-2", pvc2, event.DELETE, "ZookeeperReconciler")
	stateBuilder.AddStateEvent("PersistentVolumeClaim", "pvc-uid-3", pvc3, event.DELETE, "ZookeeperReconciler")

	// 5. First ZK is fully deleted
	stateBuilder.AddStateEvent("ZookeeperCluster", "zk-old-uid", zk1, event.DELETE, "ZookeeperReconciler")

	// 6. New ZK with same name but different UID is created
	zk2 := CreateZookeeperObject("zk-cluster", "default", "zk-new-uid", 3, nil)
	zk2OwnerRef := metav1.OwnerReference{
		APIVersion: "zookeeper.pravega.io/v1beta1",
		Kind:       "ZookeeperCluster",
		Name:       "zk-cluster",
		UID:        zk2.GetUID(),
	}
	stateBuilder.AddStateEvent("ZookeeperCluster", "zk-new-uid", zk2, event.CREATE, "ZookeeperReconciler")

	// 7. New PVCs are created for the new ZK
	pvc4 := CreatePVCObject("zk-cluster-pvc-0", "default", "pvc-uid-4", "zk-cluster", []metav1.OwnerReference{zk2OwnerRef})
	stateBuilder.AddStateEvent("PersistentVolumeClaim", "pvc-uid-4", pvc4, event.CREATE, "ZookeeperReconciler")

	pvc5 := CreatePVCObject("zk-cluster-pvc-1", "default", "pvc-uid-5", "zk-cluster", []metav1.OwnerReference{zk2OwnerRef})
	stateBuilder.AddStateEvent("PersistentVolumeClaim", "pvc-uid-5", pvc5, event.CREATE, "ZookeeperReconciler")

	pvc6 := CreatePVCObject("zk-cluster-pvc-2", "default", "pvc-uid-6", "zk-cluster", []metav1.OwnerReference{zk2OwnerRef})
	stateBuilder.AddStateEvent("PersistentVolumeClaim", "pvc-uid-6", pvc6, event.CREATE, "ZookeeperReconciler")
	eb.WithMaxDepth(2) // tuned this experimentally

	check := func(res *tracecheck.Result) bool {
		// Check if any of the converged states show the bug (PVCs deleted incorrectly)
		bugDetected := false
		for _, state := range res.ConvergedStates {
			// Count objects by kind
			pvcCount := 0
			hasZookeeper := false

			for key := range state.State.Objects() {
				if key.IdentityKey.Kind == "ZookeeperCluster" {
					hasZookeeper = true
				}
				if key.IdentityKey.Kind == "PersistentVolumeClaim" {
					pvcCount++
				}
			}

			// The bug is observed when Zookeeper exists but PVCs are gone
			if hasZookeeper && pvcCount == 0 {
				bugDetected = true
				t.Logf("Bug detected: ZookeeperCluster exists but all PVCs were incorrectly deleted")
				break
			}
		}
		return bugDetected
	}

	t.Run("Bug manifests under stale reads", func(t *testing.T) {
		eb.ExploreStaleStates() // Enable staleness exploration
		eb.WithKindBounds("ZookeeperReconciler", tracecheck.KindBounds{
			"ZookeeperCluster": 3,
		})

		explorer, err := eb.Build("standalone")
		if err != nil {
			t.Fatal(err)
		}

		// Build the state events
		initialState := stateBuilder.Build()
		initialState.PendingReconciles = []tracecheck.PendingReconcile{
			{
				ReconcilerID: "ZookeeperReconciler",
				Request: reconcile.Request{
					NamespacedName: types.NamespacedName{
						Namespace: "default",
						Name:      "zk-cluster",
					},
				},
			},
		}

		// Explore all possible execution paths
		result := explorer.Explore(context.Background(), initialState)

		// Verify results
		assert.NotEmpty(t, result.ConvergedStates, "Expected at least one converged state")

		// Check if any of the converged states show the bug (PVCs deleted incorrectly)
		bugDetected := check(result)
		assert.True(t, bugDetected, "Bug not detected: should have found a state where the ZookeeperCluster exists but its PVCs were incorrectly deleted")
	})
	t.Run("Bug does not manifest if staleness doesnt go back far enough", func(t *testing.T) {
		eb.ExploreStaleStates() // default
		eb.WithKindBounds("ZookeeperReconciler", tracecheck.KindBounds{
			"ZookeeperCluster": 1, // using staleness but not going back far enough (showing only most recent version)
		})

		explorer, err := eb.Build("standalone")
		if err != nil {
			t.Fatal(err)
		}

		// Build the state events
		initialState := stateBuilder.Build()
		initialState.PendingReconciles = []tracecheck.PendingReconcile{
			{
				ReconcilerID: "ZookeeperReconciler",
				Request: reconcile.Request{
					NamespacedName: types.NamespacedName{
						Namespace: "default",
						Name:      "zk-cluster",
					},
				},
			},
		}

		// Explore all possible execution paths
		result := explorer.Explore(context.Background(), initialState)

		// Verify results
		assert.NotEmpty(t, result.ConvergedStates, "Expected at least one converged state")

		bugDetected := check(result)
		assert.False(t, bugDetected, "Bug should not have been detected")
	})
}
