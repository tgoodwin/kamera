package snapshot

import (
	"encoding/json"
	"fmt"

	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type HashStrategy string

const (
	DefaultHash    HashStrategy = "default"
	AnonymizedHash HashStrategy = "anonymized" // removes sleeve UID info
	ShapeHash      HashStrategy = "shape"      // stronger than anonymized, removes all nondeterministic info
)

// ResourceKey represents the granularity at which
// we can uniquely identify a resource in the store
type ResourceKey struct {
	Group     string
	Kind      string
	Namespace string
	Name      string
}

type CompositeKey struct {
	IdentityKey
	ResourceKey
}

// compositeKeyJSON is the JSON representation of CompositeKey.
// We use explicit field names to avoid Go's embedded struct field collision issue.
type compositeKeyJSON struct {
	// Identity fields
	IdentityGroup string `json:"identityGroup,omitempty"`
	IdentityKind  string `json:"identityKind,omitempty"`
	ObjectID      string `json:"objectId,omitempty"`
	// Resource fields
	ResourceGroup string `json:"resourceGroup,omitempty"`
	ResourceKind  string `json:"resourceKind,omitempty"`
	Namespace     string `json:"namespace,omitempty"`
	Name          string `json:"name,omitempty"`
}

// MarshalJSON implements json.Marshaler for CompositeKey.
func (ck CompositeKey) MarshalJSON() ([]byte, error) {
	return json.Marshal(compositeKeyJSON{
		IdentityGroup: ck.IdentityKey.Group,
		IdentityKind:  ck.IdentityKey.Kind,
		ObjectID:      ck.IdentityKey.ObjectID,
		ResourceGroup: ck.ResourceKey.Group,
		ResourceKind:  ck.ResourceKey.Kind,
		Namespace:     ck.ResourceKey.Namespace,
		Name:          ck.ResourceKey.Name,
	})
}

// legacyCompositeKeyJSON is the old JSON format that had field collisions.
// We support reading it for backwards compatibility.
type legacyCompositeKeyJSON struct {
	ObjectID  string `json:"ObjectID"`
	Namespace string `json:"Namespace"`
	Name      string `json:"Name"`
}

// UnmarshalJSON implements json.Unmarshaler for CompositeKey.
// It supports both the new explicit format and the legacy format for backwards compatibility.
func (ck *CompositeKey) UnmarshalJSON(data []byte) error {
	// Try new format first
	var j compositeKeyJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return err
	}

	// If we got the new format fields, use them
	if j.ResourceKind != "" || j.IdentityKind != "" {
		ck.IdentityKey = IdentityKey{
			Group:    j.IdentityGroup,
			Kind:     j.IdentityKind,
			ObjectID: j.ObjectID,
		}
		ck.ResourceKey = ResourceKey{
			Group:     j.ResourceGroup,
			Kind:      j.ResourceKind,
			Namespace: j.Namespace,
			Name:      j.Name,
		}
		return nil
	}

	// Fall back to legacy format (ObjectID, Namespace, Name with capital letters)
	var legacy legacyCompositeKeyJSON
	if err := json.Unmarshal(data, &legacy); err != nil {
		return err
	}
	ck.IdentityKey = IdentityKey{
		ObjectID: legacy.ObjectID,
	}
	ck.ResourceKey = ResourceKey{
		Namespace: legacy.Namespace,
		Name:      legacy.Name,
	}
	return nil
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

func NewCompositeKeyWithGroup(group, kind, namespace, name, sleeveObjectID string) CompositeKey {
	ck := NewCompositeKey(kind, namespace, name, sleeveObjectID)
	ck.IdentityKey.Group = group
	ck.ResourceKey.Group = group
	return ck
}

func (ck CompositeKey) String() string {
	groupPart := ck.IdentityKey.Group
	if groupPart == "" {
		groupPart = "core"
	}
	return fmt.Sprintf("{%s/%s/%s/%s:%s}", groupPart, ck.IdentityKey.Kind, ck.Namespace, ck.Name, ck.ObjectID)
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
	crossRefIndex map[string]map[HashStrategy]VersionHash

	// Hash generator functions
	hashGenerators map[HashStrategy]Hasher
}

func NewStore() *Store {
	store := &Store{
		indices:        make(map[HashStrategy]map[VersionHash]*unstructured.Unstructured),
		objectHashes:   make(map[string]map[HashStrategy]VersionHash),
		hashGenerators: make(map[HashStrategy]Hasher),

		// maps raw hash value to a collection of all the hashes by strategy
		crossRefIndex: make(map[string]map[HashStrategy]VersionHash),
	}

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
	// initialize the index for this strategy
	s.indices[strategy] = make(map[VersionHash]*unstructured.Unstructured)
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
	gvk := util.GetGroupVersionKind(obj)
	group := gvk.Group
	if group == "" {
		group = "core"
	}
	kind := gvk.Kind
	return fmt.Sprintf("%s/%s/%s/%s", group, kind, obj.GetNamespace(), obj.GetName())
}

func (s *Store) indexObject(obj *unstructured.Unstructured) error {
	objCopy := obj.DeepCopy()
	tag.EnsureDeterministicIdentity(objCopy)
	objKey := getObjectKey(objCopy)

	if _, exists := s.objectHashes[objKey]; !exists {
		s.objectHashes[objKey] = make(map[HashStrategy]VersionHash)
	}

	// store all the hashes for this object in the same map
	hashCollection := make(map[HashStrategy]VersionHash)

	// allow for reverse lookups

	for strategy, hasher := range s.hashGenerators {
		hash, err := hasher.Hash(objCopy)
		if err != nil {
			return fmt.Errorf("failed to generate %s hash: %w", strategy, err)
		}

		// put every hash "kind" into the same collection
		hashCollection[strategy] = hash
		// point every raw hash value to the collection
		s.crossRefIndex[hash.Value] = hashCollection

		// Store in indices
		s.indices[strategy][hash] = objCopy

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
			return obj.DeepCopy(), true
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
		return obj.DeepCopy(), found
	}
	return nil, false
}

// Lookup a hash value by raw hash value and target hash strategy.
// If the raw hash value was produced with a different strategy than the target strategy,
// we translate the hash value to the target strategy.
func (s *Store) Lookup(rawHash string, targetStrategy HashStrategy) (VersionHash, bool) {
	if hashCollection, exists := s.crossRefIndex[rawHash]; exists {
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
