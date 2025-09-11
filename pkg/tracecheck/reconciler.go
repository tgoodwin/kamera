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

type ReconcilerContainer struct {
	// The name of the reconciler
	Name string

	// the primary resource type that this reconciler manages
	For string

	reconcile.Reconciler

	// both implemented by the manager type
	versionManager VersionManager

	// effectReader lets us observe what the reconciler did
	// TODO this could live elsewhere
	effectReader

	// frameInserter lets us insert the frame data for the wrapped Reconciler's client to read
	frameInserter
}

func (r *ReconcilerContainer) doReconcile(ctx context.Context, observableState ObjectVersions, req reconcile.Request) (*ReconcileResult, error) {
	frameID := replay.FrameIDFromContext(ctx)

	// insert a "frame" to hold the readset data ahead of the reconcile
	frameData := r.toFrameData(observableState)
	r.InsertCacheFrame(frameID, frameData)

	// our cleanup reconciler implementation needs to know what kind of object it is reconciling
	// as reconcile.Request is only namespace/name. so we inject it through the context.
	if r.Name == CleanupReconcilerID {
		for kind, objs := range frameData {
			for nn := range objs {
				if nn.Name == req.Name && nn.Namespace == req.Namespace {
					ctx = context.WithValue(ctx, tag.CleanupKindKey{}, kind)
				}
			}
		}
	}

	res, err := r.Reconcile(ctx, req)
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
	if _, err := r.Reconcile(ctx, request); err != nil {
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

func Wrap(name string, r reconcile.Reconciler) reconciler {
	return &ReconcilerContainer{
		Name:       name,
		Reconciler: r,
	}
}

func (r *ReconcilerContainer) toFrameData(ov ObjectVersions) replay.CacheFrame {
	out := make(replay.CacheFrame)
	for key, hash := range ov {
		kind := key.IdentityKey.Kind
		if _, ok := out[kind]; !ok {
			out[kind] = make(map[types.NamespacedName]*unstructured.Unstructured)
		}
		obj := r.versionManager.Resolve(hash)
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
