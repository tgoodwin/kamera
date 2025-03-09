package snapshot

import (
	"fmt"

	"github.com/samber/lo"
	"github.com/tgoodwin/sleeve/pkg/tag"
	"github.com/tgoodwin/sleeve/pkg/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type HashStrategy string

const (
	DefaultHash    HashStrategy = "default"
	AnonymizedHash HashStrategy = "anonymized"
)

// ResourceKey represents the granularity at which
// we can uniquely identify a resource in the store
type ResourceKey struct {
	Kind      string
	Namespace string
	Name      string
}

type CompositeKey struct {
	IdentityKey
	ResourceKey
}

func NewCompositeKey(kind, namespace, name, uid string) CompositeKey {
	return CompositeKey{
		IdentityKey: IdentityKey{
			Kind:     kind,
			ObjectID: uid,
		},
		ResourceKey: ResourceKey{
			Kind:      kind,
			Namespace: namespace,
			Name:      name,
		},
	}
}

type Hasher interface {
	Hash(obj *unstructured.Unstructured) (VersionHash, error)
}

type Store struct {
	// Maps hash strategy -> object hash -> object
	indices map[HashStrategy]map[VersionHash]*unstructured.Unstructured

	// Maps object identity key -> map of hash strategy -> hash value
	// This allows reverse lookups and cross-referencing between hash strategies
	objectHashes map[string]map[HashStrategy]VersionHash

	// Hash generator functions
	hashGenerators map[HashStrategy]Hasher

	// set of all resource keys, used to determine if a resource exists
	// during client operations so we can return appropriate errors
	// in the same way that a real k8s client would
	resourceKeys map[ResourceKey]struct{}

	idKeysToResourceKeys map[IdentityKey]ResourceKey
}

func NewStore() *Store {
	store := &Store{
		indices:        make(map[HashStrategy]map[VersionHash]*unstructured.Unstructured),
		objectHashes:   make(map[string]map[HashStrategy]VersionHash),
		hashGenerators: make(map[HashStrategy]Hasher),

		resourceKeys:         make(map[ResourceKey]struct{}),
		idKeysToResourceKeys: make(map[IdentityKey]ResourceKey),
	}

	// Initialize indices for each hash strategy
	store.indices[DefaultHash] = make(map[VersionHash]*unstructured.Unstructured)
	store.indices[AnonymizedHash] = make(map[VersionHash]*unstructured.Unstructured)

	// Register hash generators
	store.RegisterHashGenerator(DefaultHash, NewDefaultHasher())
	store.RegisterHashGenerator(AnonymizedHash, NewAnonymizingHasher(DefaultLabelReplacements))

	return store
}

func (s *Store) ResourceKeys() map[ResourceKey]struct{} {
	return s.resourceKeys
}

func (s *Store) GetVersionMap(strategy HashStrategy) map[VersionHash]*unstructured.Unstructured {
	return s.indices[strategy]
}

func (s *Store) RegisterHashGenerator(strategy HashStrategy, generator Hasher) {
	s.hashGenerators[strategy] = generator
}

// Get object identity key (could be namespace/name or another unique identifier)
func getObjectKey(obj *unstructured.Unstructured) string {
	kind := util.GetKind(obj)
	return fmt.Sprintf("%s/%s/%s", kind, obj.GetNamespace(), obj.GetName())
}

func getIdentityKey(obj *unstructured.Unstructured) IdentityKey {
	kind := util.GetKind(obj)
	return IdentityKey{
		Kind:     kind,
		ObjectID: tag.GetSleeveObjectID(obj),
	}
}

func getResourceKey(obj *unstructured.Unstructured) ResourceKey {
	kind := util.GetKind(obj)
	return ResourceKey{
		Kind:      kind,
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}
}

func (s *Store) indexObject(obj *unstructured.Unstructured, strategies ...HashStrategy) error {
	objKey := getObjectKey(obj)
	rKey := getResourceKey(obj)
	iKey := getIdentityKey(obj)

	s.resourceKeys[rKey] = struct{}{}
	s.idKeysToResourceKeys[iKey] = rKey

	if _, exists := s.objectHashes[objKey]; !exists {
		s.objectHashes[objKey] = make(map[HashStrategy]VersionHash)
	}

	for _, strategy := range strategies {
		hash, err := s.hashGenerators[strategy].Hash(obj)
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

// Store an object with all registered hash strategies
func (s *Store) StoreObject(obj *unstructured.Unstructured) error {
	strategies := lo.Keys(s.hashGenerators)
	return s.indexObject(obj, strategies...)
	// objKey := getObjectKey(obj)

	// rKey := getResourceKey(obj)
	// s.resourceKeys[rKey] = struct{}{}
	// fmt.Println("adding rkey", rKey)

	// iKey := getIdentityKey(obj)
	// s.idKeysToResourceKeys[iKey] = rKey

	// // Initialize hash map for this object if it doesn't exist
	// if _, exists := s.objectHashes[objKey]; !exists {
	// 	s.objectHashes[objKey] = make(map[HashStrategy]VersionHash)
	// }

	// // Calculate and store all hash strategies for this object
	// for strategy, generator := range s.hashGenerators {
	// 	hash, err := generator.Hash(obj)
	// 	if err != nil {
	// 		return fmt.Errorf("failed to generate %s hash: %w", strategy, err)
	// 	}

	// 	// Store in indices
	// 	s.indices[strategy][hash] = obj

	// 	// Record hash value for this object and strategy
	// 	s.objectHashes[objKey][strategy] = hash
	// }
	// return nil
}

func (s *Store) PublishWithStrategy(obj *unstructured.Unstructured, strategy HashStrategy) VersionHash {
	if err := s.indexObject(obj, strategy); err != nil {
		panic(fmt.Sprintf("error storing object: %v", err))
	}
	objKey := getObjectKey(obj)
	return s.objectHashes[objKey][strategy]

	// // Calculate hash
	// hash, err := s.hashGenerators[strategy].Hash(obj)
	// if err != nil {
	// 	panic(fmt.Sprintf("error hashing object: %v", err))
	// }

	// // Store in indices
	// s.indices[strategy][hash] = obj

	// // Record hash value for this object and strategy
	// objKey := getObjectKey(obj)
	// // Initialize hash map for this object if it doesn't exist
	// if _, exists := s.objectHashes[objKey]; !exists {
	// 	s.objectHashes[objKey] = make(map[HashStrategy]VersionHash)
	// }

	// rKey := getResourceKey(obj)
	// s.resourceKeys[rKey] = struct{}{}

	// s.objectHashes[objKey][strategy] = hash

	// return hash
}

func (s *Store) ResolveWithStrategy(hash VersionHash, strategy HashStrategy) *unstructured.Unstructured {
	if idx, exists := s.indices[strategy]; exists {
		obj, found := idx[hash]
		if found {
			return obj
		}
	}
	return nil
}

// Get object by hash value and strategy
func (s *Store) GetByHash(hash VersionHash, strategy HashStrategy) (*unstructured.Unstructured, bool) {
	if idx, exists := s.indices[strategy]; exists {
		obj, found := idx[hash]
		if !found {
			// dump all the objects in the store
			fmt.Println("lookup miss: store contents", &s)
			for h := range idx {
				fmt.Printf("hash: %s\n", h.Value)
			}
		}
		return obj, found
	}
	return nil, false
}

// Convert between hash strategies
func (s *Store) ConvertHash(hash VersionHash, fromStrategy, toStrategy HashStrategy) (VersionHash, bool) {
	// First find the object using the source hash strategy
	obj, found := s.GetByHash(hash, fromStrategy)
	if !found {
		return VersionHash{}, false
	}

	// Find the object's key
	objKey := getObjectKey(obj)

	// Look up the target hash strategy
	if hashes, exists := s.objectHashes[objKey]; exists {
		if targetHash, hasTargetStrategy := hashes[toStrategy]; hasTargetStrategy {
			return targetHash, true
		}
	}

	return VersionHash{}, false
}

// used to break ties in inferReconcileRequest
func (s *Store) Newest(candidates ...VersionHash) VersionHash {
	var newest VersionHash
	maxResourceVersion := ""
	for _, candidate := range candidates {
		obj := s.ResolveWithStrategy(candidate, candidate.Strategy)
		rv := obj.GetResourceVersion()
		if rv > maxResourceVersion {
			maxResourceVersion = rv
			newest = candidate
		}
	}
	return newest
}

func (s *Store) Oldest(candidates ...VersionHash) VersionHash {
	var oldest VersionHash
	minResourceVersion := candidates[0].Value
	for _, candidate := range candidates {
		obj := s.ResolveWithStrategy(candidate, candidate.Strategy)
		rv := obj.GetResourceVersion()
		if rv < minResourceVersion {
			minResourceVersion = rv
			oldest = candidate
		}
	}
	return oldest
}

func (s *Store) ResolveResourceKeys(keys ...IdentityKey) (util.Set[ResourceKey], error) {
	result := util.NewSet[ResourceKey]()
	for _, key := range keys {
		if rKey, exists := s.idKeysToResourceKeys[key]; exists {
			result.Add(rKey)
		}
	}
	if len(result) != len(keys) {
		return nil, fmt.Errorf("failed to resolve all keys")
	}
	return result, nil
}
