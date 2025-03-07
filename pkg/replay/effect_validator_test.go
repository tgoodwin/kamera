package replay

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tgoodwin/sleeve/pkg/event"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// ValidateOperation checks if an operation would result in a conflict
// and automatically updates the internal tracking state.
// Helper function to create test objects
func createTestObject(name, namespace string, gvk schema.GroupVersionKind) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetName(name)
	obj.SetNamespace(namespace)
	obj.SetGroupVersionKind(gvk)
	return obj
}

func TestResourceConflictValidator_Create(t *testing.T) {
	validator := NewResourceConflictValidator()

	// Define GVK for test
	gvk := schema.GroupVersionKind{
		Group:   "apps",
		Version: "v1",
		Kind:    "Deployment",
	}

	// Test case 1: Create when object doesn't exist (should succeed)
	obj1 := createTestObject("test-deployment", "default", gvk)
	err := validator.ValidateOperation(event.CREATE, obj1)
	assert.NoError(t, err, "Create should succeed when object doesn't exist")

	// Test case 2: Create when object already exists (should fail with AlreadyExists)
	obj2 := createTestObject("test-deployment", "default", gvk)
	err = validator.ValidateOperation(event.CREATE, obj2)
	assert.Error(t, err, "Create should fail when object already exists")
	assert.True(t, apierrors.IsAlreadyExists(err), "Error should be AlreadyExists")

	// Test case 3: Create different object with same name in different namespace (should succeed)
	obj3 := createTestObject("test-deployment", "other-namespace", gvk)
	err = validator.ValidateOperation(event.CREATE, obj3)
	assert.NoError(t, err, "Create should succeed for different namespace")

	// Test case 4: Create different object with different name (should succeed)
	obj4 := createTestObject("other-deployment", "default", gvk)
	err = validator.ValidateOperation(event.CREATE, obj4)
	assert.NoError(t, err, "Create should succeed for different name")
}

func TestResourceConflictValidator_Get(t *testing.T) {
	validator := NewResourceConflictValidator()

	// Define GVK for test
	gvk := schema.GroupVersionKind{
		Group:   "apps",
		Version: "v1",
		Kind:    "Deployment",
	}

	// Create test object first
	obj := createTestObject("test-deployment", "default", gvk)
	_ = validator.ValidateOperation(event.CREATE, obj)

	// Test case 1: Get existing object (should succeed)
	err := validator.ValidateOperation(event.GET, obj)
	assert.NoError(t, err, "Get should succeed for existing object")

	// Test case 2: Get non-existent object (should fail with NotFound)
	objNotFound := createTestObject("nonexistent", "default", gvk)
	err = validator.ValidateOperation(event.GET, objNotFound)
	assert.Error(t, err, "Get should fail for non-existent object")
	assert.True(t, apierrors.IsNotFound(err), "Error should be NotFound")
}

func TestResourceConflictValidator_List(t *testing.T) {
	validator := NewResourceConflictValidator()

	// Define GVK for test
	gvk := schema.GroupVersionKind{
		Group:   "apps",
		Version: "v1",
		Kind:    "Deployment",
	}

	// Add some objects
	obj1 := createTestObject("test-1", "default", gvk)
	obj2 := createTestObject("test-2", "default", gvk)
	_ = validator.ValidateOperation(event.CREATE, obj1)
	_ = validator.ValidateOperation(event.CREATE, obj2)

	// Test case: List operation always succeeds
	dummyObj := createTestObject("dummy", "default", gvk)
	err := validator.ValidateOperation(event.LIST, dummyObj)
	assert.NoError(t, err, "List should always succeed")
}

func TestResourceConflictValidator_Update(t *testing.T) {
	validator := NewResourceConflictValidator()

	// Define GVK for test
	gvk := schema.GroupVersionKind{
		Group:   "apps",
		Version: "v1",
		Kind:    "Deployment",
	}

	// Create test object first
	obj := createTestObject("test-deployment", "default", gvk)
	_ = validator.ValidateOperation(event.CREATE, obj)

	// Test case 1: Update existing object (should succeed)
	err := validator.ValidateOperation(event.UPDATE, obj)
	assert.NoError(t, err, "Update should succeed for existing object")

	// Test case 2: Update non-existent object (should fail with NotFound)
	objNotFound := createTestObject("nonexistent", "default", gvk)
	err = validator.ValidateOperation(event.UPDATE, objNotFound)
	assert.Error(t, err, "Update should fail for non-existent object")
	assert.True(t, apierrors.IsNotFound(err), "Error should be NotFound")
}

func TestResourceConflictValidator_Delete(t *testing.T) {
	validator := NewResourceConflictValidator()

	// Define GVK for test
	gvk := schema.GroupVersionKind{
		Group:   "apps",
		Version: "v1",
		Kind:    "Deployment",
	}

	// Create test object first
	obj := createTestObject("test-deployment", "default", gvk)
	_ = validator.ValidateOperation(event.CREATE, obj)

	// Test case 1: Delete existing object (should succeed)
	err := validator.ValidateOperation(event.DELETE, obj)
	assert.NoError(t, err, "Delete should succeed for existing object")

	// Verify the object was removed from tracking
	err = validator.ValidateOperation(event.GET, obj)
	assert.True(t, apierrors.IsNotFound(err), "Object should be removed after delete")

	// Test case 2: Delete non-existent object (should fail with NotFound)
	err = validator.ValidateOperation(event.DELETE, obj)
	assert.Error(t, err, "Delete should fail for non-existent object")
	assert.True(t, apierrors.IsNotFound(err), "Error should be NotFound")
}

func TestResourceConflictValidator_Patch(t *testing.T) {
	validator := NewResourceConflictValidator()

	// Define GVK for test
	gvk := schema.GroupVersionKind{
		Group:   "apps",
		Version: "v1",
		Kind:    "Deployment",
	}

	// Create test object first
	obj := createTestObject("test-deployment", "default", gvk)
	_ = validator.ValidateOperation(event.CREATE, obj)

	// Test case 1: Patch existing object (should succeed)
	err := validator.ValidateOperation(event.PATCH, obj)
	assert.NoError(t, err, "Patch should succeed for existing object")

	// Test case 2: Patch non-existent object (should fail with NotFound)
	objNotFound := createTestObject("nonexistent", "default", gvk)
	err = validator.ValidateOperation(event.PATCH, objNotFound)
	assert.Error(t, err, "Patch should fail for non-existent object")
	assert.True(t, apierrors.IsNotFound(err), "Error should be NotFound")
}

func TestResourceConflictValidator_Apply(t *testing.T) {
	validator := NewResourceConflictValidator()

	// Define GVK for test
	gvk := schema.GroupVersionKind{
		Group:   "apps",
		Version: "v1",
		Kind:    "Deployment",
	}

	// Test case 1: Apply when object doesn't exist (should succeed and create it)
	obj := createTestObject("test-deployment", "default", gvk)
	err := validator.ValidateOperation(event.APPLY, obj)
	assert.NoError(t, err, "Apply should succeed when object doesn't exist")

	// Verify object was created (GET should succeed)
	err = validator.ValidateOperation(event.GET, obj)
	assert.NoError(t, err, "Object should exist after Apply")

	// Test case 2: Apply when object already exists (should succeed and update it)
	err = validator.ValidateOperation(event.APPLY, obj)
	assert.NoError(t, err, "Apply should succeed when object already exists")
}

func TestResourceConflictValidator_DifferentObjectsWithSameName(t *testing.T) {
	validator := NewResourceConflictValidator()

	// Define different GVKs for test
	deploymentGVK := schema.GroupVersionKind{
		Group:   "apps",
		Version: "v1",
		Kind:    "Deployment",
	}

	serviceGVK := schema.GroupVersionKind{
		Group:   "",
		Version: "v1",
		Kind:    "Service",
	}

	// Create a Deployment
	deployment := createTestObject("test-app", "default", deploymentGVK)
	err := validator.ValidateOperation(event.CREATE, deployment)
	assert.NoError(t, err, "Should be able to create deployment")

	// Create a Service with the same name (should succeed because different Kind)
	service := createTestObject("test-app", "default", serviceGVK)
	err = validator.ValidateOperation(event.CREATE, service)
	assert.NoError(t, err, "Should be able to create service with same name as deployment")

	// Verify both exist
	err = validator.ValidateOperation(event.GET, deployment)
	assert.NoError(t, err, "Deployment should exist")

	err = validator.ValidateOperation(event.GET, service)
	assert.NoError(t, err, "Service should exist")
}

func TestResourceConflictValidator_ConcurrentOperations(t *testing.T) {
	validator := NewResourceConflictValidator()

	// Define GVK for test
	gvk := schema.GroupVersionKind{
		Group:   "apps",
		Version: "v1",
		Kind:    "Deployment",
	}

	// Run multiple operations concurrently to test thread safety
	const numGoroutines = 100
	var wg sync.WaitGroup

	// Create unique objects concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			obj := createTestObject(fmt.Sprintf("test-%d", index), "default", gvk)
			_ = validator.ValidateOperation(event.CREATE, obj)
		}(i)
	}

	wg.Wait()

	// Verify we have the expected number of objects
	count := 0
	validator.mu.RLock()
	count = len(validator.resources)
	validator.mu.RUnlock()

	assert.Equal(t, numGoroutines, count, "Should have created exactly numGoroutines objects")
}
