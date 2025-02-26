package snapshot

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Define mock hashers for testing
type MockDefaultHasher struct{}

func (h *MockDefaultHasher) Hash(obj *unstructured.Unstructured) (VersionHash, error) {
	// Simple mock implementation that uses name/namespace as hash
	return NewDefaultHash(fmt.Sprintf("default-%s-%s", obj.GetNamespace(), obj.GetName())), nil
}

type MockAnonymizedHasher struct{}

func (h *MockAnonymizedHasher) Hash(obj *unstructured.Unstructured) (VersionHash, error) {
	// Mock implementation that ignores namespace for "anonymization"
	return NewDefaultHash(fmt.Sprintf("anon-%s", obj.GetName())), nil
}

type ErrorHasher struct{}

func (h *ErrorHasher) Hash(obj *unstructured.Unstructured) (VersionHash, error) {
	return VersionHash{}, fmt.Errorf("simulated hash error")
}

// Helper function to create test objects
func createTestObject(namespace, name string, labels map[string]string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetNamespace(namespace)
	obj.SetName(name)
	obj.SetKind("TestKind")
	obj.SetAPIVersion("test/v1")
	obj.SetLabels(labels)
	return obj
}

func TestNewObjectStore(t *testing.T) {
	store := NewObjectStore()

	assert.NotNil(t, store.indices)
	assert.NotNil(t, store.objectHashes)
	assert.NotNil(t, store.hashGenerators)

	// Verify default indices are initialized
	assert.NotNil(t, store.indices[DefaultHash])
	assert.NotNil(t, store.indices[AnonymizedHash])
}

func TestRegisterHashGenerator(t *testing.T) {
	store := NewObjectStore()

	customStrategy := HashStrategy("custom")
	customHasher := &MockDefaultHasher{}

	store.RegisterHashGenerator(customStrategy, customHasher)

	assert.Equal(t, customHasher, store.hashGenerators[customStrategy])
}

func TestStoreObject(t *testing.T) {
	store := NewObjectStore()

	// Register our mock hashers
	store.RegisterHashGenerator(DefaultHash, &MockDefaultHasher{})
	store.RegisterHashGenerator(AnonymizedHash, &MockAnonymizedHasher{})

	// Create a test object
	obj := createTestObject("test-ns", "test-obj", map[string]string{"app": "test"})

	// Store the object
	err := store.StoreObject(obj)
	require.NoError(t, err)

	// Expected hash values
	expectedDefaultHash := NewDefaultHash("default-test-ns-test-obj")
	expectedAnonHash := NewDefaultHash("anon-test-obj")

	// Check that the object is stored in both indices
	storedObj, found := store.indices[DefaultHash][expectedDefaultHash]
	assert.True(t, found)
	assert.Equal(t, obj, storedObj)

	storedObj, found = store.indices[AnonymizedHash][expectedAnonHash]
	assert.True(t, found)
	assert.Equal(t, obj, storedObj)

	// Check that the hash mappings are stored correctly
	objKey := getObjectKey(obj)
	hashes, found := store.objectHashes[objKey]
	assert.True(t, found)
	assert.Equal(t, expectedDefaultHash, hashes[DefaultHash])
	assert.Equal(t, expectedAnonHash, hashes[AnonymizedHash])
}

func TestStoreObjectError(t *testing.T) {
	store := NewObjectStore()

	// Register a hasher that returns an error
	store.RegisterHashGenerator(DefaultHash, &ErrorHasher{})

	// Create a test object
	obj := createTestObject("test-ns", "test-obj", nil)

	// Attempt to store the object
	err := store.StoreObject(obj)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to generate")
}

func TestGetByHash(t *testing.T) {
	store := NewObjectStore()

	// Register our mock hashers
	store.RegisterHashGenerator(DefaultHash, &MockDefaultHasher{})
	store.RegisterHashGenerator(AnonymizedHash, &MockAnonymizedHasher{})

	// Create and store a test object
	obj1 := createTestObject("test-ns", "test-obj1", nil)
	err := store.StoreObject(obj1)
	require.NoError(t, err)

	// Create and store another test object
	obj2 := createTestObject("another-ns", "test-obj2", nil)
	err = store.StoreObject(obj2)
	require.NoError(t, err)

	// Test getting objects by their hashes
	retrievedObj, found := store.GetByHash(NewDefaultHash("default-test-ns-test-obj1"), DefaultHash)
	assert.True(t, found)
	assert.Equal(t, obj1, retrievedObj)

	retrievedObj, found = store.GetByHash(NewDefaultHash("anon-test-obj2"), AnonymizedHash)
	assert.True(t, found)
	assert.Equal(t, obj2, retrievedObj)

	// Test getting with non-existent hash
	retrievedObj, found = store.GetByHash(NewDefaultHash("non-existent"), DefaultHash)
	assert.False(t, found)
	assert.Nil(t, retrievedObj)

	// Test getting with non-existent strategy
	retrievedObj, found = store.GetByHash(NewDefaultHash("default-test-ns-test-obj1"), HashStrategy("non-existent"))
	assert.False(t, found)
	assert.Nil(t, retrievedObj)
}

func TestGetByHashWithMultipleObjects(t *testing.T) {
	store := NewObjectStore()

	// Register mock hashers where the anonymized hasher gives same hash for different objects
	store.RegisterHashGenerator(DefaultHash, &MockDefaultHasher{})

	// Custom hasher that always returns the same hash for a specific name pattern
	sameHasher := &MockAnonymizedHasher{}
	store.RegisterHashGenerator(AnonymizedHash, sameHasher)

	// Create and store two test objects that will have the same anonymized hash
	obj1 := createTestObject("ns1", "obj-common", nil)
	obj2 := createTestObject("ns2", "obj-common", nil)

	err := store.StoreObject(obj1)
	require.NoError(t, err)

	err = store.StoreObject(obj2)
	require.NoError(t, err)

	// The second object should overwrite the first one in the anonymized index
	// since they'd have the same anonymized hash
	retrievedObj, found := store.GetByHash(NewDefaultHash("anon-obj-common"), AnonymizedHash)
	assert.True(t, found)
	assert.Equal(t, obj2, retrievedObj) // Last one wins

	// But they should still be separate in the default index
	obj1ByDefault, found := store.GetByHash(NewDefaultHash("default-ns1-obj-common"), DefaultHash)
	assert.True(t, found)
	assert.Equal(t, obj1, obj1ByDefault)

	obj2ByDefault, found := store.GetByHash(NewDefaultHash("default-ns2-obj-common"), DefaultHash)
	assert.True(t, found)
	assert.Equal(t, obj2, obj2ByDefault)
}

func TestConvertHash(t *testing.T) {
	store := NewObjectStore()

	// Register our mock hashers
	store.RegisterHashGenerator(DefaultHash, &MockDefaultHasher{})
	store.RegisterHashGenerator(AnonymizedHash, &MockAnonymizedHasher{})

	// Create and store a test object
	obj := createTestObject("test-ns", "test-obj", nil)
	err := store.StoreObject(obj)
	require.NoError(t, err)

	// Convert from default to anonymized hash
	anonHash, found := store.ConvertHash(NewDefaultHash("default-test-ns-test-obj"), DefaultHash, AnonymizedHash)
	assert.True(t, found)
	assert.Equal(t, NewDefaultHash("anon-test-obj"), anonHash)

	// Convert from anonymized to default hash
	defaultHash, found := store.ConvertHash(NewDefaultHash("anon-test-obj"), AnonymizedHash, DefaultHash)
	assert.True(t, found)
	assert.Equal(t, NewDefaultHash("default-test-ns-test-obj"), defaultHash)

	// Test conversion with non-existent hash
	_, found = store.ConvertHash(NewDefaultHash("non-existent"), DefaultHash, AnonymizedHash)
	assert.False(t, found)

	// Test conversion with non-existent strategy
	_, found = store.ConvertHash(NewDefaultHash("default-test-ns-test-obj"), DefaultHash, HashStrategy("non-existent"))
	assert.False(t, found)
}

func TestUpdateObject(t *testing.T) {
	store := NewObjectStore()

	// Register our mock hashers
	store.RegisterHashGenerator(DefaultHash, &MockDefaultHasher{})
	store.RegisterHashGenerator(AnonymizedHash, &MockAnonymizedHasher{})

	// Create and store a test object
	obj := createTestObject("test-ns", "test-obj", map[string]string{"version": "v1"})
	err := store.StoreObject(obj)
	require.NoError(t, err)

	// Create an updated version of the object
	updatedObj := createTestObject("test-ns", "test-obj", map[string]string{"version": "v2"})
	err = store.StoreObject(updatedObj)
	require.NoError(t, err)

	// The hashes should be the same since our mock hashers don't use labels
	defaultHash := NewDefaultHash("default-test-ns-test-obj")
	anonHash := NewDefaultHash("anon-test-obj")

	// Verify the object was updated in both indices
	storedObj, found := store.GetByHash(defaultHash, DefaultHash)
	assert.True(t, found)
	assert.Equal(t, "v2", storedObj.GetLabels()["version"])

	storedObj, found = store.GetByHash(anonHash, AnonymizedHash)
	assert.True(t, found)
	assert.Equal(t, "v2", storedObj.GetLabels()["version"])
}

func TestConcurrentHashClash(t *testing.T) {
	// This test simulates the scenario where two different objects get the same hash
	// in one strategy but different hashes in another strategy

	store := NewObjectStore()

	// Define a custom hasher that gives the same hash for objects with names that start with "clash-"
	clashHasher := &MockAnonymizedHasher{}
	store.RegisterHashGenerator(AnonymizedHash, clashHasher)
	store.RegisterHashGenerator(DefaultHash, &MockDefaultHasher{})

	// Create and store two objects that will clash in the anonymized index
	obj1 := createTestObject("ns1", "clash-1", nil)
	obj2 := createTestObject("ns2", "clash-2", nil)

	err := store.StoreObject(obj1)
	require.NoError(t, err)

	err = store.StoreObject(obj2)
	require.NoError(t, err)

	// Both should be retrievable by their default hashes
	retrieved1, found := store.GetByHash(NewDefaultHash("default-ns1-clash-1"), DefaultHash)
	assert.True(t, found)
	assert.Equal(t, obj1, retrieved1)

	retrieved2, found := store.GetByHash(NewDefaultHash("default-ns2-clash-2"), DefaultHash)
	assert.True(t, found)
	assert.Equal(t, obj2, retrieved2)

	// The last stored object should win in the anonymized index
	retrievedAnon, found := store.GetByHash(NewDefaultHash("anon-clash-2"), AnonymizedHash)
	assert.True(t, found)
	assert.Equal(t, obj2, retrievedAnon)

	// Convert from default-1 to anonymized (should succeed, but might point to obj2!)
	anonHash, found := store.ConvertHash(NewDefaultHash("default-ns1-clash-1"), DefaultHash, AnonymizedHash)
	assert.True(t, found)

	// Get the object with this anonymized hash
	retrievedObj, found := store.GetByHash(anonHash, AnonymizedHash)
	assert.True(t, found)

	// This might be obj2, not obj1, because of the hash collision in the anonymized strategy
	if retrievedObj.GetNamespace() == "ns2" {
		assert.Equal(t, obj2, retrievedObj, "Collision detected - anonymized hash for obj1 points to obj2")
	}
}
