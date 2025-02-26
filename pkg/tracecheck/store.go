package tracecheck

import (
	"fmt"
	"sync"

	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type Store map[snapshot.VersionHash]*unstructured.Unstructured

type versionStore struct {
	store              Store
	keyToObj           map[event.CausalKey]*unstructured.Unstructured
	causalKeyToVersion map[event.CausalKey]snapshot.VersionHash
	hasher             snapshot.Hasher

	mu sync.RWMutex
}

var _ VersionManager = (*versionStore)(nil)

func newVersionStore() *versionStore {
	return &versionStore{
		store:              make(Store),
		causalKeyToVersion: make(map[event.CausalKey]snapshot.VersionHash),
		keyToObj:           make(map[event.CausalKey]*unstructured.Unstructured),
		hasher: snapshot.NewAnonymizingHasher(
			snapshot.DefaultLabelReplacements,
		),
	}
}

func (vs *versionStore) Resolve(anonymizedHash snapshot.VersionHash) *unstructured.Unstructured {
	res, ok := vs.store[anonymizedHash]
	if !ok {
		fmt.Printf("Miss for key\n%v\n", anonymizedHash)
		fmt.Println("There was a lookup miss: Here's the store contents")
		for hash, v := range vs.store {
			ckey, err := event.GetCausalKey(v)
			fmt.Println("causal key", ckey)
			fmt.Printf("%v\n", hash)
			if err != nil {
				logger.Error(err, "error getting causal key")
			}
		}
		// logger.Error(nil, "resolving version Hash for key", hash)
	}
	return res

}

func (vs *versionStore) Publish(obj *unstructured.Unstructured) snapshot.VersionHash {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	objCopy := obj.DeepCopy()
	hash := vs.hasher.Hash(objCopy)
	vs.store[hash] = objCopy

	// TODO ensure that all objects being mutated are still instrumented with Sleeve labels
	ckey, err := event.GetCausalKey(objCopy)
	// vs.keyToObj[ckey] = objCopy
	if err != nil {
		panic("object does not have causal key")
	}
	vs.causalKeyToVersion[ckey] = hash

	return hash
}

func (vs *versionStore) Diff(prev, curr *snapshot.VersionHash) string {
	var prevObj, currObj *unstructured.Unstructured
	if prev != nil {
		prevObj = vs.Resolve(*prev)
		if prevObj == nil {
			panic("could not resolve previous object")
		}
	}
	if curr != nil {
		currObj = vs.Resolve(*curr)
		if currObj == nil {
			panic("could not resolve current object")
		}
	}
	return snapshot.ComputeDelta(prevObj, currObj)
}
