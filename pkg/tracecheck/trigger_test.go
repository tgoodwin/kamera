package tracecheck

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"github.com/tgoodwin/sleeve/pkg/util"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// Mock hash resolver for testing
type mockHashResolver struct {
	objects map[snapshot.VersionHash]*unstructured.Unstructured
}

func (m *mockHashResolver) GetByHash(hash snapshot.VersionHash, strategy snapshot.HashStrategy) (*unstructured.Unstructured, bool) {
	obj, exists := m.objects[hash]
	return obj, exists
}

// Helper function to create test unstructured objects
func createTestObject(kind, namespace, name string, ownerRefs []metav1.OwnerReference) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetKind(kind)
	obj.SetNamespace(namespace)
	obj.SetName(name)
	if len(ownerRefs) > 0 {
		obj.SetOwnerReferences(ownerRefs)
	}
	return obj
}

// Helper to sort PendingReconciles for stable comparison
func sortPendingReconciles(reconciles []PendingReconcile) []PendingReconcile {
	sort.Slice(reconciles, func(i, j int) bool {
		if reconciles[i].ReconcilerID != reconciles[j].ReconcilerID {
			return reconciles[i].ReconcilerID < reconciles[j].ReconcilerID
		}
		if reconciles[i].Request.Namespace != reconciles[j].Request.Namespace {
			return reconciles[i].Request.Namespace < reconciles[j].Request.Namespace
		}
		return reconciles[i].Request.Name < reconciles[j].Request.Name
	})
	return reconciles
}

func TestGetTriggeredBasicCase(t *testing.T) {
	// Set up dependencies
	deps := ResourceDeps{
		"Pod":       util.NewSet("podController"),
		"Node":      util.NewSet("nodeController"),
		"Namespace": util.NewSet("nsController"),
	}

	owners := OwnerByKind{
		"Pod":       "podController",
		"Node":      "nodeController",
		"Namespace": "nsController",
	}

	// Create test objects
	podObj := createTestObject("Pod", "default", "test-pod", nil)

	// Create mock resolver
	podHash := snapshot.NewDefaultHash("hash1")
	resolver := &mockHashResolver{
		objects: map[snapshot.VersionHash]*unstructured.Unstructured{
			podHash: podObj,
		},
	}

	// Create trigger manager
	tm := NewTriggerManager(deps, owners, resolver)

	// Create test change set
	changes := ObjectVersions{
		snapshot.IdentityKey{Kind: "Pod", ObjectID: "default/test-pod"}: podHash,
	}

	// Get triggered reconciles
	triggered, err := tm.getTriggered(changes)
	assert.Equal(t, nil, err)

	// Expected result
	expected := []PendingReconcile{
		{
			ReconcilerID: "podController",
			Request: reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: "default", Name: "test-pod"},
			},
		},
	}

	// Verify results
	assert.Equal(t, expected, triggered)
}

func TestGetTriggeredWithOwnerReferences(t *testing.T) {
	// Set up dependencies
	deps := ResourceDeps{
		"Pod":        util.NewSet("podController"),
		"Deployment": util.NewSet("deploymentController"),
		"ReplicaSet": util.NewSet("replicaSetController"),
	}

	owners := OwnerByKind{
		"Pod":        "podController",
		"Deployment": "deploymentController",
		"ReplicaSet": "replicaSetController",
	}

	// Create owner references
	rsOwnerRef := metav1.OwnerReference{
		APIVersion: "apps/v1",
		Kind:       "ReplicaSet",
		Name:       "test-rs",
		UID:        "rs-123",
	}

	deployOwnerRef := metav1.OwnerReference{
		APIVersion: "apps/v1",
		Kind:       "Deployment",
		Name:       "test-deploy",
		UID:        "deploy-123",
	}

	// Create test objects with owner references
	podObj := createTestObject("Pod", "default", "test-pod", []metav1.OwnerReference{rsOwnerRef})
	rsObj := createTestObject("ReplicaSet", "default", "test-rs", []metav1.OwnerReference{deployOwnerRef})

	// Create mock resolver
	podHash := snapshot.NewDefaultHash("pod-hash")
	rsHash := snapshot.NewDefaultHash("rs-hash")
	resolver := &mockHashResolver{
		objects: map[snapshot.VersionHash]*unstructured.Unstructured{
			podHash: podObj,
			rsHash:  rsObj,
		},
	}

	// Create trigger manager
	tm := NewTriggerManager(deps, owners, resolver)

	// Test Case 1: Pod with ReplicaSet owner
	changes := ObjectVersions{
		snapshot.IdentityKey{Kind: "Pod", ObjectID: "default/test-pod"}: podHash,
	}

	triggered, err := tm.getTriggered(changes)
	assert.Equal(t, nil, err)

	expected := []PendingReconcile{
		{
			ReconcilerID: "podController",
			Request: reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: "default", Name: "test-pod"},
			},
		},
		{
			ReconcilerID: "replicaSetController",
			Request: reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: "default", Name: "test-rs"},
			},
		},
	}

	// Sort for stable comparison
	triggered = sortPendingReconciles(triggered)
	expected = sortPendingReconciles(expected)
	assert.Equal(t, expected, triggered)

	// Test Case 2: ReplicaSet with Deployment owner
	changes = ObjectVersions{
		snapshot.IdentityKey{Kind: "ReplicaSet", ObjectID: "default/test-rs"}: rsHash,
	}

	triggered, err = tm.getTriggered(changes)
	assert.Equal(t, nil, err)

	expected = []PendingReconcile{
		{
			ReconcilerID: "replicaSetController",
			Request: reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: "default", Name: "test-rs"},
			},
		},
		{
			ReconcilerID: "deploymentController",
			Request: reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: "default", Name: "test-deploy"},
			},
		},
	}

	// Sort for stable comparison
	triggered = sortPendingReconciles(triggered)
	expected = sortPendingReconciles(expected)
	assert.Equal(t, expected, triggered)
}

func TestGetTriggeredMultipleObjects(t *testing.T) {
	// Set up dependencies
	deps := ResourceDeps{
		"Pod":     util.NewSet("podController"),
		"Service": util.NewSet("serviceController"),
	}

	owners := OwnerByKind{
		"Pod":     "podController",
		"Service": "serviceController",
	}

	// Create test objects
	pod1 := createTestObject("Pod", "default", "pod-1", nil)
	pod2 := createTestObject("Pod", "default", "pod-2", nil)
	svc := createTestObject("Service", "default", "svc-1", nil)

	// Create mock resolver
	pod1Hash := snapshot.NewDefaultHash("pod1-hash")
	pod2Hash := snapshot.NewDefaultHash("pod2-hash")
	svcHash := snapshot.NewDefaultHash("svc-hash")
	resolver := &mockHashResolver{
		objects: map[snapshot.VersionHash]*unstructured.Unstructured{
			pod1Hash: pod1,
			pod2Hash: pod2,
			svcHash:  svc,
		},
	}

	// Create trigger manager
	tm := NewTriggerManager(deps, owners, resolver)

	// Create test change set with multiple objects
	changes := ObjectVersions{
		snapshot.IdentityKey{Kind: "Pod", ObjectID: "default/pod-1"}:     pod1Hash,
		snapshot.IdentityKey{Kind: "Pod", ObjectID: "default/pod-2"}:     pod2Hash,
		snapshot.IdentityKey{Kind: "Service", ObjectID: "default/svc-1"}: svcHash,
	}

	// Get triggered reconciles
	triggered, err := tm.getTriggered(changes)
	assert.Equal(t, nil, err)

	// Expected result (not concerned with order)
	expected := []PendingReconcile{
		{
			ReconcilerID: "podController",
			Request: reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: "default", Name: "pod-1"},
			},
		},
		{
			ReconcilerID: "podController",
			Request: reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: "default", Name: "pod-2"},
			},
		},
		{
			ReconcilerID: "serviceController",
			Request: reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: "default", Name: "svc-1"},
			},
		},
	}

	// Sort both slices for comparison
	triggered = sortPendingReconciles(triggered)
	expected = sortPendingReconciles(expected)
	assert.Equal(t, expected, triggered)
}

func TestGetTriggeredMissingPrimaryReconciler(t *testing.T) {
	// Set up dependencies with missing reconciler for Job
	deps := ResourceDeps{
		"Pod": util.NewSet("podController"),
		// No entry for "Job"
	}

	owners := OwnerByKind{
		"Pod": "podController",
		"Job": "jobController", // This exists in owners but not in deps
	}

	// Create test objects
	jobObj := createTestObject("Job", "default", "test-job", nil)

	// Create mock resolver
	jobHash := snapshot.NewDefaultHash("job-hash")
	resolver := &mockHashResolver{
		objects: map[snapshot.VersionHash]*unstructured.Unstructured{
			jobHash: jobObj,
		},
	}

	// Create trigger manager
	tm := NewTriggerManager(deps, owners, resolver)

	// Create test change set
	changes := ObjectVersions{
		snapshot.IdentityKey{Kind: "Job", ObjectID: "default/test-job"}: jobHash,
	}

	// Get triggered reconciles
	triggered, err := tm.getTriggered(changes)
	assert.Equal(t, nil, err)

	// Expected result - the primary reconciler is still triggered because it's in owners
	expected := []PendingReconcile{
		{
			ReconcilerID: "jobController",
			Request: reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: "default", Name: "test-job"},
			},
		},
	}

	assert.Equal(t, expected, triggered)
}

func TestGetTriggeredMissingOwnerReconciler(t *testing.T) {
	// Set up dependencies
	deps := ResourceDeps{
		"Pod": util.NewSet("podController"),
		// No entry for "CustomResource"
	}

	owners := OwnerByKind{
		"Pod": "podController",
		// No entry for "CustomResource"
	}

	// Create owner reference to a custom resource type without a registered controller
	customOwnerRef := metav1.OwnerReference{
		APIVersion: "custom.io/v1",
		Kind:       "CustomResource",
		Name:       "custom-1",
		UID:        "custom-123",
	}

	// Create test object
	podObj := createTestObject("Pod", "default", "test-pod", []metav1.OwnerReference{customOwnerRef})

	// Create mock resolver
	podHash := snapshot.NewDefaultHash("pod-hash")
	resolver := &mockHashResolver{
		objects: map[snapshot.VersionHash]*unstructured.Unstructured{
			podHash: podObj,
		},
	}

	// Create trigger manager
	tm := NewTriggerManager(deps, owners, resolver)

	// Create test change set
	changes := ObjectVersions{
		snapshot.IdentityKey{Kind: "Pod", ObjectID: "default/test-pod"}: podHash,
	}

	// Get triggered reconciles
	triggered, err := tm.getTriggered(changes)
	assert.Equal(t, nil, err)

	// Expected result - only the pod controller is triggered, the custom resource's controller isn't
	expected := []PendingReconcile{
		{
			ReconcilerID: "podController",
			Request: reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: "default", Name: "test-pod"},
			},
		},
	}

	assert.Equal(t, expected, triggered)
}

func TestGetTriggeredWithHashResolutionFailure(t *testing.T) {
	// Set up dependencies
	deps := ResourceDeps{
		"Pod": util.NewSet("podController"),
	}

	owners := OwnerByKind{
		"Pod": "podController",
	}

	// Create mock resolver with empty objects map
	resolver := &mockHashResolver{
		objects: map[snapshot.VersionHash]*unstructured.Unstructured{},
	}

	// Create trigger manager
	tm := NewTriggerManager(deps, owners, resolver)

	// Create test change set with a hash that doesn't exist
	changes := ObjectVersions{
		snapshot.IdentityKey{Kind: "Pod", ObjectID: "default/test-pod"}: snapshot.NewDefaultHash("non-existent-hash"),
	}

	_, err := tm.getTriggered(changes)
	assert.NotNil(t, err)
}
