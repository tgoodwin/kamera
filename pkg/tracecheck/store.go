package tracecheck

import (
	"sync"

	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type versionStore struct {
	*snapshot.Store
	mu sync.RWMutex
}

var _ VersionManager = (*versionStore)(nil)

func newVersionStore(store *snapshot.Store) *versionStore {
	return &versionStore{
		Store: store,
	}
}

func (vs *versionStore) DebugKey(key string) {
	vs.Store.DebugKey(key)
}

func (vs *versionStore) Resolve(hash snapshot.VersionHash) *unstructured.Unstructured {
	res, ok := vs.ResolveWithStrategy(hash, hash.Strategy)
	if !ok {
		return nil
	}
	return res
}

func (vs *versionStore) Publish(obj *unstructured.Unstructured) snapshot.VersionHash {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	return vs.PublishWithStrategy(obj, snapshot.AnonymizedHash)
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
