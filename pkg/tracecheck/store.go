package tracecheck

import (
	"sync"

	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type Store map[snapshot.VersionHash]*unstructured.Unstructured

type versionStore struct {
	store              Store
	causalKeyToVersion map[event.CausalKey]snapshot.VersionHash
	hasher             snapshot.Hasher

	mu sync.RWMutex
}

var _ VersionManager = (*versionStore)(nil)

func newVersionStore() *versionStore {
	return &versionStore{
		store:              make(Store),
		causalKeyToVersion: make(map[event.CausalKey]snapshot.VersionHash),
		hasher: snapshot.NewAnonymizingHasher(
			snapshot.DefaultLabelReplacements,
		),
	}
}

func (vs *versionStore) Resolve(key snapshot.VersionHash) *unstructured.Unstructured {
	res, ok := vs.store[key]
	if !ok {
		panic("could not resolve key in versionStore")
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
	}
	if curr != nil {
		currObj = vs.Resolve(*curr)
	}
	return snapshot.ComputeDelta(prevObj, currObj)
}
