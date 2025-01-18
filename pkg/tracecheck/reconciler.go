package tracecheck

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	"github.com/tgoodwin/sleeve/pkg/replay"
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

type reconcileImpl struct {
	// The name of the reconciler
	Name string
	reconcile.Reconciler

	client *replay.Client

	// both implemented by teh manager type
	versionManager VersionManager
	effectReader
}

func (r *reconcileImpl) doReconcile(ctx context.Context, currState ObjectVersions) (ObjectVersions, error) {
	// create a new cache frame from the current state of the world of objects.
	// the Reconciler's readset will be a subset of this frame
	frameID := util.UUID()
	ctx = replay.WithFrameID(ctx, frameID)
	logger = log.FromContext(ctx)

	req, err := r.inferReconcileRequest(currState)
	r.client.InsertFrame(frameID, r.toFrameData(currState))
	if err != nil {
		return nil, errors.Wrap(err, "inferring reconcile request")
	}
	// TODO handle explicit requeue requests
	if _, err := r.Reconcile(ctx, req); err != nil {
		return nil, errors.Wrap(err, "executing reconcile")
	}
	effects, err := r.retrieveEffects(frameID)
	logger.WithValues("reconciler", r.Name, "effects", len(effects)).Info("reconcile complete")
	if err != nil {
		return nil, errors.Wrap(err, "retrieving reconcile effects")
	}
	return effects, nil
}

func Wrap(name string, r reconcile.Reconciler) reconciler {
	return &reconcileImpl{
		Name:       name,
		Reconciler: r,
	}
}

func (r *reconcileImpl) inferReconcileRequest(readset ObjectVersions) (reconcile.Request, error) {
	for key, version := range readset {
		if key.Kind == r.Name {
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
