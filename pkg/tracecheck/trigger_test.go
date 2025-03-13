package tracecheck

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tgoodwin/sleeve/pkg/event"
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

	owners := PrimariesByKind{
		"Pod":       util.NewSet("podController"),
		"Node":      util.NewSet("nodeController"),
		"Namespace": util.NewSet("nsController"),
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
	tm := &TriggerManager{
		deps:     deps,
		owners:   owners,
		resolver: resolver,
	}

	changes := Changes{
		Effects: []effect{
			{
				OpType:  event.CREATE,
				Key:     snapshot.NewCompositeKey("Pod", "default", "test-pod", "test-pod"),
				Version: podHash,
			},
		},
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

	owners := PrimariesByKind{
		"Pod":        util.NewSet("podController"),
		"Deployment": util.NewSet("deploymentController"),
		"ReplicaSet": util.NewSet("replicaSetController"),
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
	tm := &TriggerManager{
		deps:     deps,
		owners:   owners,
		resolver: resolver,
	}

	// Test Case 1: Pod with ReplicaSet owner
	changes := Changes{
		Effects: []effect{
			{
				OpType:  event.CREATE,
				Key:     snapshot.NewCompositeKey("Pod", "default", "test-pod", "test-pod"),
				Version: podHash,
			},
		},
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
	changes = Changes{
		Effects: []effect{
			{
				OpType:  event.CREATE,
				Key:     snapshot.NewCompositeKey("ReplicaSet", "default", "test-rs", "test-rs"),
				Version: rsHash,
			},
		},
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

	owners := PrimariesByKind{
		"Pod":     util.NewSet("podController"),
		"Service": util.NewSet("serviceController"),
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
	tm := &TriggerManager{
		deps:     deps,
		owners:   owners,
		resolver: resolver,
	}

	changes := Changes{
		Effects: []effect{
			{
				OpType:  event.CREATE,
				Key:     snapshot.NewCompositeKey("Pod", "default", "pod-1", "pod-1"),
				Version: pod1Hash,
			},
			{
				OpType:  event.CREATE,
				Key:     snapshot.NewCompositeKey("Pod", "default", "pod-2", "pod-2"),
				Version: pod2Hash,
			},
			{
				OpType:  event.CREATE,
				Key:     snapshot.NewCompositeKey("Service", "default", "svc-1", "svc-1"),
				Version: svcHash,
			},
		},
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

func TestGetTriggeredThroughOwnerRefs(t *testing.T) {
	deps := ResourceDeps{
		"Pod":         util.NewSet("podController"),
		"RouteConfig": util.NewSet("routeConfigController", "podController"),
	}

	owners := PrimariesByKind{
		"Pod":         util.NewSet("podController"),
		"RouteConfig": util.NewSet("routeConfigController"),
	}

	// Create owner references
	podOwnerRef := metav1.OwnerReference{
		APIVersion: "v1",
		Kind:       "Pod",
		Name:       "test-pod",
		UID:        "pod-123",
	}

	// create route config object with owner reference to pod
	routeConfigObj := createTestObject("RouteConfig", "default", "route-config-1", []metav1.OwnerReference{podOwnerRef})
	rcHash := snapshot.NewDefaultHash("rc-hash")
	resolver := &mockHashResolver{
		objects: map[snapshot.VersionHash]*unstructured.Unstructured{
			rcHash: routeConfigObj,
		},
	}
	tm := &TriggerManager{deps, owners, resolver}

	changes := Changes{
		Effects: []effect{
			{
				OpType:  event.CREATE,
				Key:     snapshot.NewCompositeKey("RouteConfig", "default", "route-config-1", "route-config-1"),
				Version: rcHash,
			},
		},
	}
	expected := []PendingReconcile{
		{
			ReconcilerID: "podController",
			Request: reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: "default", Name: "test-pod"},
			},
		},
		{
			ReconcilerID: "routeConfigController",
			Request: reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: "default", Name: "route-config-1"},
			},
		},
	}
	actual, err := tm.getTriggered(changes)
	assert.Nil(t, err)
	assert.Equal(t, expected, actual)
}

func TestGetTriggeredMissingPrimaryReconciler(t *testing.T) {
	// Set up dependencies with missing reconciler for Job
	deps := ResourceDeps{
		"Pod": util.NewSet("podController"),
		// No entry for "Job"
	}

	owners := PrimariesByKind{
		"Pod": util.NewSet("podController"),
		"Job": util.NewSet("jobController"), // This exists in owners but not in deps
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
	tm := &TriggerManager{
		deps:     deps,
		owners:   owners,
		resolver: resolver,
	}

	// Create test change set
	changes := Changes{
		Effects: []effect{
			{
				OpType:  event.CREATE,
				Key:     snapshot.NewCompositeKey("Job", "default", "test-job", "test-job"),
				Version: jobHash,
			},
		},
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

	owners := PrimariesByKind{
		"Pod": util.NewSet("podController"),
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
	tm := &TriggerManager{deps, owners, resolver}

	// Create test change set
	changes := Changes{
		Effects: []effect{
			{
				OpType:  event.CREATE,
				Key:     snapshot.NewCompositeKey("Pod", "default", "test-pod", "test-pod"),
				Version: podHash,
			},
		},
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

	owners := PrimariesByKind{
		"Pod": util.NewSet("podController"),
	}

	// Create mock resolver with empty objects map
	resolver := &mockHashResolver{
		objects: map[snapshot.VersionHash]*unstructured.Unstructured{},
	}

	// Create trigger manager
	tm := &TriggerManager{deps, owners, resolver}

	// Create test change set with a hash that doesn't exist
	changes := Changes{
		Effects: []effect{
			{
				OpType:  event.CREATE,
				Key:     snapshot.NewCompositeKey("Pod", "default", "test-pod", "test-pod"),
				Version: snapshot.NewDefaultHash("non-existent-hash"),
			},
		},
	}

	_, err := tm.getTriggered(changes)
	assert.NotNil(t, err)
}
