package tracecheck

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/tgoodwin/sleeve/pkg/replay"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"github.com/tgoodwin/sleeve/pkg/tag"
	"github.com/tgoodwin/sleeve/pkg/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type effectReader interface {
	// TODO how to more idiomatically represent "not found" ?
	retrieveEffects(frameID string) (Changes, error)
}

type frameInserter interface {
	InsertCacheFrame(id string, data replay.CacheFrame)
}

type ReconcileStrategy interface {
	// DoReconcile performs the reconciliation.
	// If observableState is provided, it first prepares the environment.
	DoReconcile(ctx context.Context, req reconcile.Request, observableState ObjectVersions) (reconcile.Result, error)
}

type ControllerRuntimeStrategy struct {
	reconcile.Reconciler
	versionManager VersionManager
	frameInserter
	reconcilerName string
}

func NewControllerRuntimeStrategy(r reconcile.Reconciler, vm VersionManager, fi frameInserter, name string) *ControllerRuntimeStrategy {
	return &ControllerRuntimeStrategy{
		Reconciler:     r,
		versionManager: vm,
		frameInserter:  fi,
		reconcilerName: name,
	}
}

func (s *ControllerRuntimeStrategy) DoReconcile(ctx context.Context, req reconcile.Request, observableState ObjectVersions) (reconcile.Result, error) {
	if observableState != nil {
		frameID := replay.FrameIDFromContext(ctx)
		frameData := s.toFrameData(observableState)
		s.InsertCacheFrame(frameID, frameData)

		// our cleanup reconciler implementation needs to know what kind of object it is reconciling
		// as reconcile.Request is only namespace/name. so we inject it through the context.
		if s.reconcilerName == CleanupReconcilerID {
			for kind, objs := range frameData {
				for nn := range objs {
					if nn.Name == req.Name && nn.Namespace == req.Namespace {
						ctx = context.WithValue(ctx, tag.CleanupKindKey{}, kind)
					}
				}
			}
		}
	}
	return s.Reconciler.Reconcile(ctx, req)
}

// toFrameData converts an ObjectVersions map (which tracks per-branch cluster state)
// into a CacheFrame by resolving the object hashes to full objects.
func (s *ControllerRuntimeStrategy) toFrameData(ov ObjectVersions) replay.CacheFrame {
	out := make(replay.CacheFrame)
	for key, hash := range ov {
		kind := key.IdentityKey.Kind
		if _, ok := out[kind]; !ok {
			out[kind] = make(map[types.NamespacedName]*unstructured.Unstructured)
		}
		obj := s.versionManager.Resolve(hash)
		if obj == nil {
			logger.Error(nil, "unable to resolve object hash", "key", key, "hash", util.ShortenHash(hash.Value))
			panic(fmt.Sprintf("unable to resolve object hash for key: %s stragegy %s", key, hash.Strategy))
		}
		namespacedName := types.NamespacedName{
			Name:      obj.GetName(),
			Namespace: obj.GetNamespace(),
		}
		out[kind][namespacedName] = obj
		logger.V(2).WithValues(
			"Key", key,
			"Hash", util.ShortenHash(hash.Value),
		).Info("resolved frame data item")
	}
	return out
}

type ReconcilerContainer struct {
	// The name of the reconciler
	Name string

	// the primary resource type that this reconciler manages
	For string

	Strategy ReconcileStrategy

	// both implemented by the manager type
	versionManager VersionManager

	// effectReader lets us observe what the reconciler did
	// TODO this could live elsewhere
	effectReader
}

func (r *ReconcilerContainer) doReconcile(ctx context.Context, observableState ObjectVersions, req reconcile.Request) (*ReconcileResult, error) {
	frameID := replay.FrameIDFromContext(ctx)

	res, err := r.Strategy.DoReconcile(ctx, req, observableState)
	if err != nil {
		return nil, errors.Wrap(err, "executing reconcile")
	}
	logger.V(2).Info("reconcile complete", "result", res)
	effects, err := r.retrieveEffects(frameID)
	if err != nil {
		return nil, errors.Wrap(err, "retrieving reconcile effects")
	}
	deltas := r.computeDeltas(observableState, effects.ObjectVersions)

	return &ReconcileResult{
		ControllerID: r.Name,
		FrameID:      frameID,
		FrameType:    FrameTypeExplore,
		Changes:      effects,
		Deltas:       deltas,
		ctrlRes:      res,
	}, nil
}

func (r *ReconcilerContainer) replayReconcile(ctx context.Context, request reconcile.Request) (*ReconcileResult, error) {
	frameID := replay.FrameIDFromContext(ctx)
	if _, err := r.Strategy.DoReconcile(ctx, request, nil); err != nil {
		return nil, errors.Wrap(err, "executing reconcile")
	}
	effects, err := r.retrieveEffects(frameID)
	if err != nil {
		return nil, errors.Wrap(err, "retrieving reconcile effects")
	}
	return &ReconcileResult{
		ControllerID: r.Name,
		FrameID:      frameID,
		FrameType:    FrameTypeReplay,
		Changes:      effects,
		// Deltas:       effects,
	}, nil
}

func Wrap(name string, r reconcile.Reconciler, vm VersionManager, fi frameInserter, er effectReader) reconciler {
	strategy := &ControllerRuntimeStrategy{
		Reconciler:     r,
		versionManager: vm,
		frameInserter:  fi,
		reconcilerName: name,
	}
	return &ReconcilerContainer{
		Name:           name,
		Strategy:       strategy,
		versionManager: vm,
		effectReader:   er,
	}
}

func (r *ReconcilerContainer) computeDeltas(readSet, writeSet ObjectVersions) map[snapshot.CompositeKey]Delta {
	out := make(map[snapshot.CompositeKey]Delta)
	for key, hash := range writeSet {
		if prevHash, ok := readSet[key]; ok {
			delta := r.versionManager.Diff(&prevHash, &hash)
			out[key] = Delta(delta)
		}
	}
	return out
}
