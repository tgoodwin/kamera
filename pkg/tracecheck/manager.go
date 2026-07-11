package tracecheck

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/samber/lo"
	"github.com/tgoodwin/kamera/pkg/apiserver"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/replay"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/util"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// 1. need to map from reconcileID to the object versions that were read at that reconcile

type VersionManager interface {
	Resolve(key snapshot.VersionHash) *unstructured.Unstructured
	Publish(obj *unstructured.Unstructured) snapshot.VersionHash
	Diff(prev, curr *snapshot.VersionHash) string
	Lookup(rawHash string, targetStrategy snapshot.HashStrategy) (snapshot.VersionHash, bool)
	DebugKey(key string)
}

type Effect struct {
	OpType       event.OperationType
	Key          snapshot.CompositeKey
	Version      snapshot.VersionHash
	Subresource  string `json:"subresource,omitempty"`
	Materialized bool   `json:"materialized,omitempty"`

	Precondition *replay.PreconditionInfo
}

type reconcileEffects struct {
	reads  []Effect
	writes []Effect
}

func newEffect(key snapshot.CompositeKey, version snapshot.VersionHash, op event.OperationType) Effect {
	return Effect{
		OpType:  op,
		Key:     key,
		Version: version,
	}
}

func newEffectWithOptions(key snapshot.CompositeKey, version snapshot.VersionHash, op event.OperationType, precondition *replay.PreconditionInfo, options *replay.EffectOptions) Effect {
	subresource := ""
	if options != nil {
		subresource = options.Subresource
	}
	return Effect{
		OpType:       op,
		Key:          key,
		Version:      version,
		Subresource:  subresource,
		Precondition: precondition,
	}
}

// manager is the "database" for the tracecheck package. It handles
// all state management responsibilities for tracechecking.
type manager struct {
	*versionStore // maps hashes to full object values

	scheme *runtime.Scheme

	// need to add frame data to the manager as well for reconciler reads
	*converterImpl

	// populated by RecordEffect
	effects map[string]reconcileEffects

	effectRKeys map[string]util.Set[string]
	effectIKeys map[string]util.Set[snapshot.IdentityKey]

	// effectRVs tracks the current integer resourceVersion for each resource key
	// within a reconcile frame, used for optimistic concurrency conflict checking.
	// frameID → resourceKey → current integer RV
	effectRVs map[string]map[string]int64

	// effectObjects is the synchronous API-server view for a reconcile frame.
	// Unlike the cache frame, it advances after each successful write.
	effectObjects  map[string]map[string]*unstructured.Unstructured
	effectNextRV   map[string]int64
	effectVersions map[string]map[string]snapshot.VersionHash

	schemaRegistry *apiserver.Registry
	requireSchemas bool

	// rvLookup maps VersionHash → integer resourceVersion.
	// Set from the explorer's resourceVersions map.
	rvLookup func(snapshot.VersionHash) int64

	mu sync.RWMutex
}

func canonicalResourceKeyString(group, kind, namespace, name string) string {
	return fmt.Sprintf("%s/%s/%s", util.CanonicalGroupKind(group, kind), namespace, name)
}

func (m *manager) Summary() {
	store := m.versionStore.GetVersionMap(snapshot.AnonymizedHash)
	for k, v := range store {
		fmt.Printf("Key: %s, Value: %s\n", k, v)
	}
}

func (m *manager) Scheme() *runtime.Scheme {
	return m.scheme
}

// ensure that manager implements the necessary interfaces
var _ VersionManager = (*manager)(nil)
var _ effectReader = (*manager)(nil)
var _ replay.EffectRecorder = (*manager)(nil)

var DefaultHasher = snapshot.JSONHasher{}

func (m *manager) RecordEffect(ctx context.Context, obj client.Object, opType event.OperationType, precondition *replay.PreconditionInfo, options *replay.EffectOptions) error {
	// Check fault injection crash threshold before recording.
	if ci := getCrashInjector(ctx); ci != nil {
		if err := ci.CheckWrite(opType); err != nil {
			return err
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	materialized := false
	materializedChanged := false
	materializedKey := ""
	if event.IsWriteOp(opType) && opType != event.MARK_FOR_DELETION && opType != event.REMOVE {
		var err error
		obj, materialized, materializedChanged, materializedKey, err = m.materializeSchemaWrite(ctx, obj, opType, precondition, options)
		if err != nil {
			return err
		}
		if precondition != nil && precondition.DryRun && materialized {
			return nil
		}
	}

	// Schema-backed writes were validated against the synchronous object view
	// while being materialized. Legacy effects retain the existing validator.
	if !materialized {
		if err := m.validateEffect(ctx, opType, obj, precondition, options); err != nil {
			return err
		}
	}

	gvk := ensureObjectGVK(obj, m.scheme)
	kind := gvk.Kind

	logger := log.FromContext(ctx)
	logger.V(2).Info("recording effect", "opType", opType, "kind", kind)
	sleeveObjectID := tag.GetSleeveObjectID(obj)

	// TODO SLE-28 figure out why this can happen.
	if sleeveObjectID == "" {
		logger.V(4).Info("object missing sleeve object ID", "kind", kind)
	}

	frameID := replay.FrameIDFromContext(ctx)
	var u *unstructured.Unstructured
	var err error
	if materialized {
		u, err = schemaResponseToUnstructured(obj, gvk)
	} else {
		u, err = util.ConvertToUnstructured(obj)
	}
	if err != nil {
		return err
	}

	// A successful no-op returns the current version. Publishing the response
	// would create a different hash when its served resourceVersion was stamped
	// from frame bookkeeping rather than stored object metadata.
	var versionHash snapshot.VersionHash
	if materialized && !materializedChanged {
		var found bool
		versionHash, found = m.effectVersions[frameID][materializedKey]
		if !found {
			return fmt.Errorf("schema-backed no-op has no live version for %s", materializedKey)
		}
	} else {
		versionHash = m.Publish(u)
	}
	if materialized {
		m.effectVersions[frameID][materializedKey] = versionHash
	}

	reffects, ok := m.effects[frameID]
	if !ok {
		reffects = reconcileEffects{
			reads:  make([]Effect, 0),
			writes: make([]Effect, 0),
		}
	}

	key := snapshot.NewCompositeKeyWithGroup(gvk.Group, kind, obj.GetNamespace(), obj.GetName(), sleeveObjectID)
	eff := newEffectWithOptions(key, versionHash, opType, precondition, options)
	eff.Materialized = materialized
	if opType == event.GET || opType == event.LIST {
		reffects.reads = append(reffects.reads, eff)
	} else {
		reffects.writes = append(reffects.writes, eff)

		// Note: we do NOT advance effectRVs here. The tracked RV stays at
		// the pre-reconcile ground truth value. This means all writes within
		// a single reconcile are checked against the same baseline. The conflict
		// only fires when the controller was served a stale cache frame (RV=N)
		// but the ground truth is already at RV=N+1 from a prior reconcile step.
	}
	m.effects[frameID] = reffects
	logger.V(2).Info("recorded effects", "frameID", frameID, "numReads", len(reffects.reads), "numWrites", len(reffects.writes))

	return nil
}

func (m *manager) PrepareEffectContext(ctx context.Context, ov ObjectVersions) error {
	frameID := replay.FrameIDFromContext(ctx)
	if m.effectObjects == nil {
		m.effectObjects = make(map[string]map[string]*unstructured.Unstructured)
	}
	if m.effectNextRV == nil {
		m.effectNextRV = make(map[string]int64)
	}
	if m.effectVersions == nil {
		m.effectVersions = make(map[string]map[string]snapshot.VersionHash)
	}
	cKeys := lo.Keys(ov)
	// holds objectID
	iKeys := lo.Map(cKeys, func(k snapshot.CompositeKey, _ int) snapshot.IdentityKey {
		return k.IdentityKey
	})

	// holds kind/namespace/name
	rKeySet := util.NewSet[string]()
	rvs := make(map[string]int64, len(ov))
	objects := make(map[string]*unstructured.Unstructured, len(ov))
	versions := make(map[string]snapshot.VersionHash, len(ov))
	var highestRV int64
	for ck, vh := range ov {
		primary := canonicalResourceKeyString(ck.ResourceKey.Group, ck.ResourceKey.Kind, ck.ResourceKey.Namespace, ck.ResourceKey.Name)
		rKeySet.Add(primary)
		versions[primary] = vh
		// Look up the integer RV for this object version
		if m.rvLookup != nil {
			if rv := m.rvLookup(vh); rv > 0 {
				rvs[primary] = rv
				if rv > highestRV {
					highestRV = rv
				}
			}
		}
		if obj := m.Resolve(vh); obj != nil {
			copy := obj.DeepCopy()
			if rv := rvs[primary]; rv > 0 {
				copy.SetResourceVersion(strconv.FormatInt(rv, 10))
			}
			objects[primary] = copy
		}
	}
	m.effectRKeys[frameID] = rKeySet
	m.effectIKeys[frameID] = util.NewSet(iKeys...)
	m.effectRVs[frameID] = rvs
	m.effectObjects[frameID] = objects
	m.effectNextRV[frameID] = highestRV
	m.effectVersions[frameID] = versions
	return nil
}

func (m *manager) CleanupEffectContext(ctx context.Context) {
	frameID := replay.FrameIDFromContext(ctx)
	delete(m.effectRKeys, frameID)
	delete(m.effectIKeys, frameID)
	delete(m.effectRVs, frameID)
	delete(m.effectObjects, frameID)
	delete(m.effectNextRV, frameID)
	delete(m.effectVersions, frameID)
}

func (m *manager) GetEffects(ctx context.Context) (Changes, error) {
	frameID := replay.FrameIDFromContext(ctx)
	return m.retrieveEffects(frameID)
}

func (m *manager) retrieveEffects(frameID string) (Changes, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	effects := m.effects[frameID]
	out := make(ObjectVersions)
	for _, eff := range effects.writes {
		// TODO handle the case where there are multiple writes to the same object
		// in the same frame
		out[eff.Key] = eff.Version
	}

	changes := Changes{
		ObjectVersions: out,
		Effects:        effects.writes,
		Observations:   effects.reads,
	}
	return changes, nil
}

// validateEffect checks that the effect operation is valid given the current tracked state with respect to Kubernetes API semantics.
func (m *manager) validateEffect(ctx context.Context, op event.OperationType, obj client.Object, precondition *replay.PreconditionInfo, options *replay.EffectOptions) error {
	frameID := replay.FrameIDFromContext(ctx)
	rKeys, ok := m.effectRKeys[frameID]
	if !ok {
		return fmt.Errorf("no effect context found for frameID %s", frameID)
	}
	gvk := util.GetGroupVersionKind(obj)
	if (gvk.Kind == "" || gvk.Group == "") && obj != nil && m.scheme != nil {
		if gvks, _, err := m.scheme.ObjectKinds(obj); err == nil && len(gvks) > 0 {
			if gvk.Kind == "" {
				gvk.Kind = gvks[0].Kind
			}
			if gvk.Group == "" {
				gvk.Group = gvks[0].Group
			}
		}
	}
	// as objects may be created under simulation,
	// we need to use reflection to infer the kind
	safeKind := util.GetKind(obj)

	resourceKey := canonicalResourceKeyString(gvk.Group, safeKind, obj.GetNamespace(), obj.GetName())
	rKeyExists := rKeys.Contains(resourceKey)
	iKey := snapshot.IdentityKey{
		Group:    gvk.Group,
		Kind:     safeKind,
		ObjectID: tag.GetSleeveObjectID(obj),
	}

	// objecty with same kind/objectID already exists
	_, iKeyExists := m.effectIKeys[frameID][iKey]

	logger := log.FromContext(ctx)
	logger.V(2).Info("[validateEffect]", "frame", frameID, "op", op, "key", resourceKey, "exists", rKeyExists, "trackedKeys", rKeys.List())

	switch op {
	// there are no UID preconditions for create.
	// a dry run flag can be used to validate without committing
	case event.CREATE:
		if rKeyExists {
			return apierrors.NewAlreadyExists(
				schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind},
				obj.GetName())
		}
		// Automatically track the resource if CREATE is valid
		rKeys.Add(resourceKey)
		logger.V(2).Info("tracked create", "key", resourceKey)

	case event.GET:
		if !rKeyExists {
			logger.V(1).Info("GET miss", "key", resourceKey, "tracked", rKeys.List())
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
		if !rKeyExists {
			logger.V(1).Info("UPDATE/PATCH miss", "key", resourceKey, "tracked", rKeys.List())
			panic("Object not found for UPDATE/PATCH: probably a serious logic error in the tracecheck manager")
		}
		// Optimistic concurrency check. The claimed RV comes from:
		// - Update (PUT): metadata.resourceVersion on the object body
		// - Patch with OptimisticLock: resourceVersion embedded in the patch
		//   payload (extracted into preconditions.ResourceVersion by the client)
		// - Patch without OptimisticLock: no RV check (matches real K8s)
		var claimedRVStr string
		if op == event.UPDATE {
			claimedRVStr = obj.GetResourceVersion()
		} else if precondition != nil && precondition.ResourceVersion != nil {
			claimedRVStr = *precondition.ResourceVersion
		}
		if claimedRVStr != "" {
			if rvs, ok := m.effectRVs[frameID]; ok {
				currentRV, tracked := rvs[resourceKey]
				if tracked {
					claimedRV, err := strconv.ParseInt(claimedRVStr, 10, 64)
					if err == nil && claimedRV != currentRV {
						logger.V(1).Info("resourceVersion conflict",
							"key", resourceKey,
							"claimedRV", claimedRV,
							"currentRV", currentRV)
						return apierrors.NewConflict(
							schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind},
							obj.GetName(),
							fmt.Errorf("the object has been modified; please apply your changes to the latest version"),
						)
					}
				}
			}
		}

	case event.MARK_FOR_DELETION:
		// need to handle cases where:
		// 1. no precondition, rkey does not exist (error)
		// 2. no precondition, rkey exists (delete)
		// 3. precondition with UID, rkey does not exist (error)
		// 4. precondition with UID, rkey exists, UID does not match (error)
		// 5. precondition with UID, rkey exists, UID matches iKey (delete)

		if !rKeyExists {
			// case 1 and 3
			return apierrors.NewNotFound(
				schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind},
				obj.GetName())
		}

		if precondition == nil {
			// case 2
			// Automatically remove tracking if DELETE is valid
			rKeys.Delete(resourceKey)
			delete(m.effectIKeys[frameID], iKey)
			logger.V(2).Info("marked for deletion", "key", resourceKey)
			return nil
		}

		if precondition.UID != nil {
			if !iKeyExists {
				// case 4
				return apierrors.NewConflict(
					schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind},
					obj.GetName(),
					fmt.Errorf("UID precondition failed"),
				)
			}
			if iKeyExists {
				// case 5
				// Automatically remove tracking if DELETE is valid
				rKeys.Delete(resourceKey)
				delete(m.effectIKeys[frameID], iKey)
				logger.V(2).Info("marked for deletion", "key", resourceKey, "preconditionUID", *precondition.UID)
				return nil
			}
		}

	case event.APPLY:
		if options != nil && options.Subresource == "status" {
			if !rKeyExists {
				logger.V(1).Info("status APPLY miss", "key", resourceKey, "tracked", rKeys.List())
				return apierrors.NewNotFound(
					schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind},
					obj.GetName())
			}
			return nil
		}

		// APPLY implements upsert semantics - creates or updates as needed
		if !rKeyExists {
			// Add it for a new resource
			rKeys.Add(resourceKey)
		}
		// Existing resource just gets updated, no change to tracking state

	case event.REMOVE:
		// need to egnsure the object being removed is already marked for deletion
		// TODO remove tracking rKeys and iKeys here...
	default:
		panic("unhandled operation type in validateEffect: " + string(op))
	}

	return nil

}
