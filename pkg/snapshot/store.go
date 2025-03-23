package snapshot

import (
	"fmt"

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

func NewCompositeKey(kind, namespace, name, sleeveObjectID string) CompositeKey {
	return CompositeKey{
		IdentityKey: IdentityKey{
			Kind:     kind,
			ObjectID: sleeveObjectID,
		},
		ResourceKey: ResourceKey{
			Kind:      kind,
			Namespace: namespace,
			Name:      name,
		},
	}
}

func (ck CompositeKey) String() string {
	return fmt.Sprintf("{%s/%s/%s:%s}", ck.IdentityKey.Kind, ck.Namespace, ck.Name, util.Shorter(ck.ObjectID))
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

	// maps a raw hash value to a collection of all the hashes
	rawHashesToCollection map[string]map[HashStrategy]VersionHash

	// Hash generator functions
	hashGenerators map[HashStrategy]Hasher
}

func NewStore() *Store {
	store := &Store{
		indices:        make(map[HashStrategy]map[VersionHash]*unstructured.Unstructured),
		objectHashes:   make(map[string]map[HashStrategy]VersionHash),
		hashGenerators: make(map[HashStrategy]Hasher),

		rawHashesToCollection: make(map[string]map[HashStrategy]VersionHash),
	}

	// Initialize indices for each hash strategy
	store.indices[DefaultHash] = make(map[VersionHash]*unstructured.Unstructured)
	store.indices[AnonymizedHash] = make(map[VersionHash]*unstructured.Unstructured)

	// Register hash generators
	store.RegisterHashGenerator(DefaultHash, NewDefaultHasher())
	store.RegisterHashGenerator(AnonymizedHash, NewAnonymizingHasher(DefaultLabelReplacements))

	return store
}

func (s *Store) GetVersionMap(strategy HashStrategy) map[VersionHash]*unstructured.Unstructured {
	return s.indices[strategy]
}

func (s *Store) RegisterHashGenerator(strategy HashStrategy, generator Hasher) {
	s.hashGenerators[strategy] = generator
}

func (s *Store) DebugKey(key string) {
	fmt.Println("as object key", key)
	if _, exists := s.objectHashes[key]; !exists {
		fmt.Println("key not found")
		return
	}
	for strategy, hash := range s.objectHashes[key] {
		fmt.Printf("strategy: %s\nhash: %s\n", strategy, util.ShortenHash(hash.Value))
	}

	fmt.Println("everything:")
	for strategy, hashes := range s.indices {
		fmt.Printf("Strategy: %s\n", strategy)
		for hash := range hashes {
			fmt.Printf("  Hash: %s\n", util.ShortenHash(hash.Value))
		}
	}
}

// Get object identity key (could be namespace/name or another unique identifier)
func getObjectKey(obj *unstructured.Unstructured) string {
	kind := util.GetKind(obj)
	return fmt.Sprintf("%s/%s/%s", kind, obj.GetNamespace(), obj.GetName())
}

func (s *Store) indexObject(obj *unstructured.Unstructured) error {
	objKey := getObjectKey(obj)

	if _, exists := s.objectHashes[objKey]; !exists {
		s.objectHashes[objKey] = make(map[HashStrategy]VersionHash)
	}

	// store all the hashes for this object in the same map
	hashCollection := make(map[HashStrategy]VersionHash)

	// allow for reverse lookups

	for strategy, hasher := range s.hashGenerators {
		hash, err := hasher.Hash(obj)
		if err != nil {
			return fmt.Errorf("failed to generate %s hash: %w", strategy, err)
		}

		// put every hash "kind" into the same collection
		hashCollection[strategy] = hash
		// point every raw hash value to the collection
		s.rawHashesToCollection[hash.Value] = hashCollection

		// Store in indices
		s.indices[strategy][hash] = obj

		// this is used to look up the hash in PublishWithStrategy
		// TODO refactor
		s.objectHashes[objKey][strategy] = hash
	}
	return nil
}

// Store an object with all registered hash strategies
func (s *Store) StoreObject(obj *unstructured.Unstructured) error {
	return s.indexObject(obj)
}

func (s *Store) PublishWithStrategy(obj *unstructured.Unstructured, strategy HashStrategy) VersionHash {
	// internally, we hash with every registered strategy, so that we always have every hash type
	// for a given object. This is useful for cross-referencing between hash strategies.
	if err := s.indexObject(obj); err != nil {
		panic(fmt.Sprintf("error storing object: %v", err))
	}
	objKey := getObjectKey(obj)
	return s.objectHashes[objKey][strategy]
}

func (s *Store) ResolveWithStrategy(hash VersionHash, strategy HashStrategy) (*unstructured.Unstructured, bool) {
	if idx, exists := s.indices[strategy]; exists {
		obj, found := idx[hash]
		if found {
			return obj, true
		} else {
			shortHash := util.ShortenHash(hash.Value)
			fmt.Printf("lookup miss: hash %s strategy %s\n", shortHash, strategy)
			fmt.Println("curr contents under strategy", strategy)
			for h := range idx {
				fmt.Printf("hash: %s\n", util.ShortenHash(h.Value))
			}
		}
	}
	return nil, false
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

// Lookup a hash value by raw hash value and target hash strategy.
// If the raw hash value was produced with a different strategy than the target strategy,
// we translate the hash value to the target strategy.
func (s *Store) Lookup(rawHash string, targetStrategy HashStrategy) (VersionHash, bool) {
	if hashCollection, exists := s.rawHashesToCollection[rawHash]; exists {
		if hash, exists := hashCollection[targetStrategy]; exists {
			return hash, true
		}
	}
	return VersionHash{}, false
}

// Convert between hash strategies
func (s *Store) ConvertHash(hash VersionHash, fromStrategy, toStrategy HashStrategy) (VersionHash, bool) {
	// First find the object using the source hash strategy
	translated, ok := s.Lookup(hash.Value, toStrategy)
	if ok {
		return translated, true
	}
	return VersionHash{}, false
}

// used to break ties in inferReconcileRequest
func (s *Store) Newest(candidates ...VersionHash) VersionHash {
	var newest VersionHash
	maxResourceVersion := ""
	for _, candidate := range candidates {
		obj, _ := s.ResolveWithStrategy(candidate, candidate.Strategy)
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
		obj, _ := s.ResolveWithStrategy(candidate, candidate.Strategy)
		rv := obj.GetResourceVersion()
		if rv < minResourceVersion {
			minResourceVersion = rv
			oldest = candidate
		}
	}
	return oldest
}
