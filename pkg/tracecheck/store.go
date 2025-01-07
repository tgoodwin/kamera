package tracecheck

import (
	"sync"

	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/replay"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type Store map[snapshot.VersionHash]*unstructured.Unstructured

type versionStore struct {
	store              Store
	causalKeyToVersion map[event.CausalKey]snapshot.VersionHash
	hasher             snapshot.JSONHasher

	mu sync.RWMutex
}

var _ VersionManager = (*versionStore)(nil)

func fromReplayStore(rs replay.Store, hasher snapshot.JSONHasher) Store {
	out := make(Store)
	for _, elem := range rs {
		hash := hasher.Hash(elem)
		out[hash] = elem
	}
	return out
}

func (vs *versionStore) Resolve(key snapshot.VersionHash) *unstructured.Unstructured {
	return vs.store[key]
}

func (vs *versionStore) Publish(obj *unstructured.Unstructured) snapshot.VersionHash {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	objCopy := obj.DeepCopy()
	hash := vs.hasher.Hash(objCopy)
	vs.store[hash] = objCopy

	// TODO ensure that all objects being mutated are still instrumented with Sleeve labels
	ckey, _ := event.GetCausalKey(objCopy)
	vs.causalKeyToVersion[ckey] = hash

	return hash
}
