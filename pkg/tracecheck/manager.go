package tracecheck

import (
	"context"
	"fmt"
	"sync"

	"github.com/samber/lo"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/replay"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"github.com/tgoodwin/sleeve/pkg/tag"
	"github.com/tgoodwin/sleeve/pkg/util"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// 1. need to map from reconcileID to the object versions that were read at that reconcile

type VersionManager interface {
	Resolve(key snapshot.VersionHash) *unstructured.Unstructured
	Publish(obj *unstructured.Unstructured) snapshot.VersionHash
	Diff(prev, curr *snapshot.VersionHash) string
}

type effect struct {
	OpType    event.OperationType
	ObjectKey snapshot.IdentityKey
	Version   snapshot.VersionHash
	// Timestamp time.Time
}

type reconcileEffects struct {
	reads  []effect
	writes []effect
}

func newEffect(kind, uid string, version snapshot.VersionHash, op event.OperationType) effect {
	return effect{
		OpType: op,
		ObjectKey: snapshot.IdentityKey{
			Kind:     kind,
			ObjectID: uid,
		},
		Version: version,
		// Timestamp: time.Now(),
	}
}

// manager is the "database" for the tracecheck package. It handles
// all state management responsibilities for tracechecking.
type manager struct {
	*versionStore // maps hashes to full object values

	snapStore *snapshot.Store

	// need to add frame data to the manager as well for reconciler reads
	*converterImpl

	// resourceValidator resourceValidator

	// populated by RecordEffect
	effects map[string]reconcileEffects

	effectContext map[string]util.Set[snapshot.ResourceKey]

	mu sync.RWMutex
}

func (m *manager) Summary() {
	store := m.versionStore.snapStore.GetVersionMap(snapshot.AnonymizedHash)
	for k, v := range store {
		fmt.Printf("Key: %s, Value: %s\n", k, v)
	}
}

// ensure that manager implements the necessary interfaces
var _ VersionManager = (*manager)(nil)
var _ effectReader = (*manager)(nil)
var _ replay.EffectRecorder = (*manager)(nil)

var DefaultHasher = snapshot.JSONHasher{}

func (m *manager) RecordEffect(ctx context.Context, obj client.Object, opType event.OperationType) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// this will insert a resourceKey into the resourceValidator store
	if err := m.validateEffect(ctx, opType, obj); err != nil {
		return err
	}

	logger := log.FromContext(ctx)
	logger.V(2).Info("recording effect", "opType", opType, "kind", util.GetKind(obj))

	kind := util.GetKind(obj)
	objectID := tag.GetSleeveObjectID(obj)
	if objectID == "" {
		panic("object does not have a sleeve object ID")
	}

	frameID := replay.FrameIDFromContext(ctx)
	u, err := util.ConvertToUnstructured(obj)
	if err != nil {
		return err
	}
	// publish the object versionHash
	versionHash := m.Publish(u)

	// now manifest an event and record it as an effect
	reffects, ok := m.effects[frameID]
	if !ok {
		reffects = reconcileEffects{
			reads:  make([]effect, 0),
			writes: make([]effect, 0),
		}
	}

	eff := newEffect(kind, objectID, versionHash, opType)
	if opType == event.GET || opType == event.LIST {
		reffects.reads = append(reffects.reads, eff)
	} else {
		reffects.writes = append(reffects.writes, eff)
	}
	m.effects[frameID] = reffects
	logger.V(2).Info("recorded effects", "frameID", frameID, "reads", reffects.reads, "writes", reffects.writes)

	return nil
}

func (m *manager) PrepareEffectContext(ctx context.Context, ov ObjectVersions) error {
	frameID := replay.FrameIDFromContext(ctx)
	iKeys := lo.Keys(ov)

	rKeys, err := m.snapStore.ResolveResourceKeys(iKeys...)
	if err != nil {
		return err
	}
	m.effectContext[frameID] = rKeys

	return nil
}

func (m *manager) CleanupEffectContext(ctx context.Context) {
	frameID := replay.FrameIDFromContext(ctx)
	delete(m.effectContext, frameID)
}

func (m *manager) retrieveEffects(frameID string) (Changes, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	effects := m.effects[frameID]
	out := make(ObjectVersions)
	for _, eff := range effects.writes {
		// TODO handle the case where there are multiple writes to the same object
		// in the same frame
		out[eff.ObjectKey] = eff.Version
	}

	changes := Changes{
		ObjectVersions: out,
		Effects:        effects.writes,
	}
	return changes, nil
}

func (m *manager) validateEffect(ctx context.Context, op event.OperationType, obj client.Object) error {
	frameID := replay.FrameIDFromContext(ctx)
	keys, ok := m.effectContext[frameID]
	if !ok {
		return fmt.Errorf("no effect context found for frameID %s", frameID)
	}
	gvk := obj.GetObjectKind().GroupVersionKind()
	key := snapshot.ResourceKey{
		Kind:      gvk.Kind,
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}
	_, exists := keys[key]
	if !exists && op != event.CREATE {
		fmt.Println("resource does not exist in the following keys")
		for k := range keys {
			fmt.Println(k)
		}
	}

	switch op {
	case event.CREATE:
		if exists {
			return apierrors.NewAlreadyExists(
				schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind},
				obj.GetName())
		}
		// Automatically track the resource if CREATE is valid
		keys[key] = struct{}{}

	case event.GET:
		if !exists {
			return apierrors.NewNotFound(
				schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind},
				obj.GetName())
		}
		// GET is read-only, no state changes

	case event.LIST:
		// LIST doesn't operate on a specific object
		// Always succeeds, returns empty list if no objects match
		return nil

	case event.UPDATE, event.PATCH:
		if !exists {
			return apierrors.NewNotFound(
				schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind},
				obj.GetName())
		}
		// No need to change tracking state for UPDATE/PATCH

	case event.DELETE:
		if !exists {
			fmt.Println("KEY NOT FOUND", key)
			return apierrors.NewNotFound(
				schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind},
				obj.GetName())
		}
		// Automatically remove tracking if DELETE is valid
		fmt.Println("----deleting key----", key)
		delete(keys, key)

	case event.APPLY:
		// APPLY implements upsert semantics - creates or updates as needed
		if !exists {
			// Add it for a new resource
			keys[key] = struct{}{}
		}
		// Existing resource just gets updated, no change to tracking state
	}

	return nil

}
