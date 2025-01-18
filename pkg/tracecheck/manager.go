package tracecheck

import (
	"context"
	"fmt"
	"sync"

	sleeveclient "github.com/tgoodwin/sleeve/pkg/client"
	"github.com/tgoodwin/sleeve/pkg/replay"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"github.com/tgoodwin/sleeve/pkg/tag"
	"github.com/tgoodwin/sleeve/pkg/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type converter interface {
	getStateAtReconcile(reconcileID string) ObjectVersions
}

// 1. need to map from reconcileID to the object versions that were read at that reconcile

type VersionManager interface {
	Resolve(key snapshot.VersionHash) *unstructured.Unstructured
	Publish(obj *unstructured.Unstructured) snapshot.VersionHash
}

type effect struct {
	ObjectKey snapshot.IdentityKey
	version   snapshot.VersionHash
}

type reconcileEffects struct {
	reads  []effect
	writes []effect
}

func newEffect(kind, uid string, version snapshot.VersionHash) effect {
	return effect{
		ObjectKey: snapshot.IdentityKey{
			Kind:     kind,
			ObjectID: uid,
		},
		version: version,
	}
}

// manager is the "database" for the tracecheck package. It handles
// all state management responsibilities for tracechecking.
type manager struct {
	*versionStore // maps hashes to full object values
	// (Kind+objectID) -> []versionHash
	*snapshot.LifecycleContainer // for each Object IdentityKey, store all value hashes and the reconciles that produced them

	// need to add frame data to the manager as well for reconciler reads
	*converterImpl

	// populated by RecordEffect
	effects map[string]reconcileEffects

	mu sync.RWMutex
}

func (m *manager) Summary() {
	store := m.versionStore.store
	for k, v := range store {
		fmt.Printf("Key: %s, Value: %s\n", k, v)
	}
}

// ensure that manager implements the necessary interfaces
var _ VersionManager = (*manager)(nil)
var _ effectReader = (*manager)(nil)
var _ replay.EffectRecorder = (*manager)(nil)

var DefaultHasher = snapshot.JSONHasher{}

func (m *manager) RecordEffect(ctx context.Context, obj client.Object, opType sleeveclient.OperationType) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	logger := log.FromContext(ctx)
	logger.V(2).Info("recording effect", "opType", opType, "kind", util.GetKind(obj))

	kind := util.GetKind(obj)
	objectID := tag.GetSleeveObjectID(obj)
	if objectID == "" {
		return fmt.Errorf("object does not have a sleeve object ID")
	}

	frameID := replay.FrameIDFromContext(ctx)
	u, err := util.ConvertToUnstructured(obj)
	if err != nil {
		return err
	}
	// publish the object versionHash
	versionHash := m.Publish(u)
	ikey := snapshot.IdentityKey{
		Kind:     kind,
		ObjectID: objectID,
	}
	// add the version to the object's lifecycle
	m.InsertSynthesizedVersion(ikey, versionHash, frameID)

	// now manifest an event and record it as an effect
	reffects, ok := m.effects[frameID]
	if !ok {
		reffects = reconcileEffects{
			reads:  make([]effect, 0),
			writes: make([]effect, 0),
		}
	}

	eff := newEffect(kind, objectID, versionHash)
	if opType == sleeveclient.GET || opType == sleeveclient.LIST {
		reffects.reads = append(reffects.reads, eff)
	} else {
		reffects.writes = append(reffects.writes, eff)
	}
	m.effects[frameID] = reffects
	logger.V(2).Info("recorded effects", "frameID", frameID, "reads", reffects.reads, "writes", reffects.writes)

	return nil
}

func (m *manager) retrieveEffects(frameID string) (ObjectVersions, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	effects := m.effects[frameID]
	out := make(ObjectVersions)
	for _, eff := range effects.writes {
		// TODO handle the case where there are multiple writes to the same object
		// in the same frame
		out[eff.ObjectKey] = eff.version
	}
	return out, nil
}
