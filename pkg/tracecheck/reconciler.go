package tracecheck

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/tgoodwin/sleeve/pkg/replay"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"github.com/tgoodwin/sleeve/pkg/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type effectReader interface {
	// TODO how to more idiomatically represent "not found" ?
	retrieveEffects(frameID string) (Changes, error)
}

type frameInserter interface {
	InsertCacheFrame(id string, data replay.CacheFrame)
}

type reconcileImpl struct {
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

func (r *reconcileImpl) doReconcile(ctx context.Context, observableState ObjectVersions, req reconcile.Request) (*ReconcileResult, error) {
	frameID := util.UUID()
	logger = log.FromContext(ctx).WithValues("reconciler", r.Name, "frameID", frameID)
	ctx = replay.WithFrameID(ctx, frameID)

	// insert a "frame" to hold the readset data ahead of the reconcile
	r.InsertCacheFrame(frameID, r.toFrameData(observableState))
	frameData := r.toFrameData(observableState)

	if logger.V(2).Enabled() {
		logger.V(2).Info("frame data for frameID: %s\n", frameID)
		for kind, objs := range frameData {
			for nn, obj := range objs {
				logger.V(2).Info("kind: %s, nn: %s, obj: %v\n", kind, nn, obj)
			}
		}
	}

	compare := false
	if compare {
		inferredReq, err := r.inferReconcileRequest(observableState)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("inferring reconcile request, frameID: %s", frameID))
		}

		if inferredReq != req {
			logger.V(2).Info("MISMATCH")
			logger.V(2).Info("inferred: %v, passed: %v\n", inferredReq, req)
		}
	}

	// add the logger back to the context
	ctx = log.IntoContext(ctx, logger)

	if _, err := r.Reconcile(ctx, req); err != nil {
		return nil, errors.Wrap(err, "executing reconcile")
	}
	effects, err := r.retrieveEffects(frameID)
	if err != nil {
		return nil, errors.Wrap(err, "retrieving reconcile effects")
	}
	deltas := r.computeDeltas(observableState, effects.ObjectVersions)
	logger.V(2).Info("reconcile complete")

	return &ReconcileResult{
		ControllerID: r.Name,
		FrameID:      frameID,
		FrameType:    FrameTypeExplore,
		Changes:      effects,
		Deltas:       deltas,
	}, nil
}

func (r *reconcileImpl) replayReconcile(ctx context.Context, request reconcile.Request) (*ReconcileResult, error) {
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
	return &reconcileImpl{
		Name:       name,
		Reconciler: r,
	}
}

func (r *reconcileImpl) inferReconcileRequest(readset ObjectVersions) (reconcile.Request, error) {
	for key, version := range readset {
		if key.Kind == r.For {
			obj := r.versionManager.Resolve(version)
			if obj == nil {
				fmt.Printf("missing full object for hash %s\n", version)
				return reconcile.Request{}, errors.Errorf("no object found for key %s", key)
			}
			req := reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      obj.GetName(),
					Namespace: obj.GetNamespace(),
				},
			}
			return req, nil
		}
	}
	return reconcile.Request{}, errors.New(fmt.Sprintf("no object of kind %s in readset", r.For))
}

func (r *reconcileImpl) toFrameData(ov ObjectVersions) replay.CacheFrame {
	out := make(replay.CacheFrame)

	for key, hash := range ov {
		kind := key.Kind
		if _, ok := out[kind]; !ok {
			out[kind] = make(map[types.NamespacedName]*unstructured.Unstructured)
		}
		obj := r.versionManager.Resolve(hash)
		if obj == nil {
			panic(fmt.Sprintf("unable to resolve object hash for key: %s", key))
		}
		namespacedName := types.NamespacedName{
			Name:      obj.GetName(),
			Namespace: obj.GetNamespace(),
		}
		out[kind][namespacedName] = obj
	}

	return out
}

func (r *reconcileImpl) computeDeltas(readSet, writeSet ObjectVersions) map[snapshot.IdentityKey]Delta {
	out := make(map[snapshot.IdentityKey]Delta)
	for key, hash := range writeSet {
		if prevHash, ok := readSet[key]; ok {
			delta := r.versionManager.Diff(&prevHash, &hash)
			out[key] = Delta(delta)
		}
	}
	return out
}
