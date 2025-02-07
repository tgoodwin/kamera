package tracecheck

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	"github.com/tgoodwin/sleeve/pkg/replay"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"github.com/tgoodwin/sleeve/pkg/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var logger logr.Logger

type effectReader interface {
	// TODO how to more idiomatically represent "not found" ?
	retrieveEffects(frameID string) (ObjectVersions, error)
}

type frameInserter interface {
	InsertFrame(id string, data replay.FrameData)
}

type reconcileImpl struct {
	// The name of the reconciler
	Name string

	// the primary resource type that this reconciler manages
	For string

	reconcile.Reconciler

	// both implemented by teh manager type
	versionManager VersionManager
	effectReader
	frameInserter
}

func (r *reconcileImpl) doReconcile(ctx context.Context, currState ObjectVersions) (*ReconcileResult, error) {
	// create a new cache frame from the current state of the world of objects.
	// the Reconciler's readset will be a subset of this frame
	frameID := util.UUID()
	ctx = replay.WithFrameID(ctx, frameID)
	logger = log.FromContext(ctx).WithValues("reconciler", r.Name, "frameID", frameID)
	// add the logger back to the context
	ctx = log.IntoContext(ctx, logger)

	req, err := r.inferReconcileRequest(currState)
	r.InsertFrame(frameID, r.toFrameData(currState))
	if err != nil {
		return nil, errors.Wrap(err, "inferring reconcile request")
	}
	// TODO handle explicit requeue requests
	if frameid := replay.FrameIDFromContext(ctx); frameid != frameID {
		panic("frameID mismatch")
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		return nil, errors.Wrap(err, "executing reconcile")
	}
	effects, err := r.retrieveEffects(frameID)
	if err != nil {
		return nil, errors.Wrap(err, "retrieving reconcile effects")
	}
	deltas := r.computeDeltas(currState, effects)
	logger.V(2).Info("reconcile complete")

	return &ReconcileResult{
		ControllerID: r.Name,
		FrameID:      frameID,
		Changes:      effects,
		Deltas:       deltas,
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
				return reconcile.Request{}, errors.New("no object found for version")
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
	return reconcile.Request{}, errors.New(fmt.Sprintf("no object of kind %s", r.Name))
}

func (r *reconcileImpl) toFrameData(ov ObjectVersions) replay.FrameData {
	out := make(replay.FrameData)

	for key, hash := range ov {
		kind := key.Kind
		if _, ok := out[kind]; !ok {
			out[kind] = make(map[types.NamespacedName]*unstructured.Unstructured)
		}
		obj := r.versionManager.Resolve(hash)
		if obj == nil {
			panic("unable to resolve object hash")
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
