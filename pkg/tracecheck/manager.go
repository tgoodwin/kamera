package tracecheck

import (
	"context"
	"sync"

	sleeveclient "github.com/tgoodwin/sleeve/pkg/client"
	"github.com/tgoodwin/sleeve/pkg/replay"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"github.com/tgoodwin/sleeve/pkg/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type VersionManager interface {
	Resolve(key snapshot.VersionHash) *unstructured.Unstructured
	Publish(obj *unstructured.Unstructured) snapshot.VersionHash
}

type effect struct {
	ObjectKey
	version snapshot.VersionHash
}

type reconcileEffects struct {
	reads  []effect
	writes []effect
}

func newEffect(kind, uid string, version snapshot.VersionHash) effect {
	return effect{
		ObjectKey: ObjectKey{
			Kind:     kind,
			ObjectID: uid,
		},
		version: version,
	}
}

type manager struct {
	*versionStore // maps hashes to full object values
	// (Kind+objectID) -> []versionHash
	*snapshot.LifecycleContainer // for each Object IdentityKey, store all value hashes and the reconciles that produced them

	effects map[string]reconcileEffects

	mu sync.RWMutex
}

var _ VersionManager = (*manager)(nil)

var _ effectReader = (*manager)(nil)

var DefaultHasher = snapshot.JSONHasher{}

func FromBuilder(b *replay.Builder) *manager {
	store := b.Store()
	// vStore := fromReplayStore(store, DefaultHasher)
	vStore := &versionStore{
		store:  make(Store),
		hasher: DefaultHasher,
	}

	events := b.Events()
	for _, event := range events {
		ckey := event.CausalKey()
		versionValue := store[ckey]
		vStore.Publish(versionValue)

		// now hydrate lifecycle info
	}

	return &manager{
		versionStore: vStore,
	}
}

func (m *manager) RecordEffect(ctx context.Context, obj client.Object, opType sleeveclient.OperationType) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	frameID := replay.FrameIDFromContext(ctx)
	u, err := util.ConvertToUnstructured(obj)
	if err != nil {
		return err
	}
	// publish the object version
	version := m.Publish(u)
	ikey := snapshot.IdentityKey{
		Kind:     u.GetKind(),
		ObjectID: string(u.GetUID()),
	}
	// add the version to the object's lifecycle
	m.InsertSynthesized(ikey, version, frameID)

	// now manifest an event and record it as an effect
	reffects, ok := m.effects[frameID]
	if !ok {
		reffects = reconcileEffects{
			reads:  make([]effect, 0),
			writes: make([]effect, 0),
		}
	}

	eff := newEffect(u.GetKind(), string(u.GetUID()), version)
	if opType == sleeveclient.GET || opType == sleeveclient.LIST {
		reffects.reads = append(reffects.reads, eff)
	} else {
		reffects.writes = append(reffects.writes, eff)
	}

	return nil
}

func (m *manager) retrieveEffects(frameID string) (ObjectVersions, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	effects := m.effects[frameID]
	out := make(ObjectVersions)
	for _, eff := range effects.writes {
		out[eff.ObjectKey] = eff.version
	}
	return out, nil
}
