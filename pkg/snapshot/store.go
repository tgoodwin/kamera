package snapshot

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type HashStrategy string

const (
	DefaultHash    HashStrategy = "default"
	AnonymizedHash HashStrategy = "anonymized"
)

type Hasher interface {
	Hash(obj *unstructured.Unstructured) (VersionHash, error)
}

type ObjectStore struct {
	// Maps hash strategy -> object hash -> object
	indices map[HashStrategy]map[VersionHash]*unstructured.Unstructured

	// Maps object identity key -> map of hash strategy -> hash value
	// This allows reverse lookups and cross-referencing between hash strategies
	objectHashes map[string]map[HashStrategy]VersionHash

	// Hash generator functions
	hashGenerators map[HashStrategy]Hasher
}

func NewObjectStore() *ObjectStore {
	store := &ObjectStore{
		indices:        make(map[HashStrategy]map[VersionHash]*unstructured.Unstructured),
		objectHashes:   make(map[string]map[HashStrategy]VersionHash),
		hashGenerators: make(map[HashStrategy]Hasher),
	}

	// Initialize indices for each hash strategy
	store.indices[DefaultHash] = make(map[VersionHash]*unstructured.Unstructured)
	store.indices[AnonymizedHash] = make(map[VersionHash]*unstructured.Unstructured)

	// Register hash generators
	store.RegisterHashGenerator(DefaultHash, NewDefaultHasher())
	store.RegisterHashGenerator(AnonymizedHash, NewAnonymizingHasher(DefaultLabelReplacements))

	return store
}

func (s *ObjectStore) RegisterHashGenerator(strategy HashStrategy, generator Hasher) {
	s.hashGenerators[strategy] = generator
}

// Get object identity key (could be namespace/name or another unique identifier)
func getObjectKey(obj *unstructured.Unstructured) string {
	return fmt.Sprintf("%s/%s", obj.GetNamespace(), obj.GetName())
}

// Store an object with all registered hash strategies
func (s *ObjectStore) StoreObject(obj *unstructured.Unstructured) error {
	objKey := getObjectKey(obj)

	// Initialize hash map for this object if it doesn't exist
	if _, exists := s.objectHashes[objKey]; !exists {
		s.objectHashes[objKey] = make(map[HashStrategy]VersionHash)
	}

	// Calculate and store all hash strategies for this object
	for strategy, generator := range s.hashGenerators {
		hash, err := generator.Hash(obj)
		if err != nil {
			return fmt.Errorf("failed to generate %s hash: %w", strategy, err)
		}

		// Store in indices
		s.indices[strategy][hash] = obj

		// Record hash value for this object and strategy
		s.objectHashes[objKey][strategy] = hash
	}

	return nil
}

// Get object by hash value and strategy
func (s *ObjectStore) GetByHash(hash VersionHash, strategy HashStrategy) (*unstructured.Unstructured, bool) {
	if idx, exists := s.indices[strategy]; exists {
		obj, found := idx[hash]
		return obj, found
	}
	return nil, false
}

// Convert between hash strategies
func (s *ObjectStore) ConvertHash(hash VersionHash, fromStrategy, toStrategy HashStrategy) (VersionHash, bool) {
	// First find the object using the source hash strategy
	obj, found := s.GetByHash(hash, fromStrategy)
	if !found {
		return "", false
	}

	// Find the object's key
	objKey := getObjectKey(obj)

	// Look up the target hash strategy
	if hashes, exists := s.objectHashes[objKey]; exists {
		if targetHash, hasTargetStrategy := hashes[toStrategy]; hasTargetStrategy {
			return targetHash, true
		}
	}

	return "", false
}
