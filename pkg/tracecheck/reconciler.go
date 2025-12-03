package tracecheck

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/tgoodwin/kamera/pkg/replay"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type effectReader interface {
	// TODO how to more idiomatically represent "not found" ?
	GetEffects(ctx context.Context) (Changes, error)
}

type EffectHandler interface {
	effectReader
	replay.EffectRecorder
}

type frameInserter interface {
	InsertCacheFrame(id string, data replay.CacheFrame)
}

type Strategy interface {
	PrepareState(ctx context.Context, state []runtime.Object) (context.Context, func(), error)
	ReconcileAtState(ctx context.Context, name types.NamespacedName) (reconcile.Result, error)
}

type ControllerRuntimeStrategy struct {
	reconcile.Reconciler
	frameInserter
	reconcilerName string
	effectReader
	scheme *runtime.Scheme
}

func NewControllerRuntimeStrategy(r reconcile.Reconciler, fi frameInserter, er effectReader, name string) *ControllerRuntimeStrategy {
	return &ControllerRuntimeStrategy{
		Reconciler:     r,
		frameInserter:  fi,
		reconcilerName: name,
		effectReader:   er,
	}
}

func (s *ControllerRuntimeStrategy) RetrieveEffects(ctx context.Context) (Changes, error) {
	return s.effectReader.GetEffects(ctx)
}

func (s *ControllerRuntimeStrategy) PrepareState(ctx context.Context, state []runtime.Object) (context.Context, func(), error) {
	frameID := replay.FrameIDFromContext(ctx)
	frameData := s.toFrameData(state)
	s.InsertCacheFrame(frameID, frameData)
	cleanup := func() {}
	return ctx, cleanup, nil
}

func (s *ControllerRuntimeStrategy) ReconcileAtState(ctx context.Context, name types.NamespacedName) (reconcile.Result, error) {
	// our cleanup reconciler implementation needs to know what kind of object it is reconciling
	// as reconcile.Request is only namespace/name. so we inject it through the context.
	// TODO factor this cleanup-specific stuff out into a dedicated strategy
	if s.reconcilerName == cleanupReconcilerID {
		frameID := replay.FrameIDFromContext(ctx)
		frameData, err := s.frameInserter.(*replay.FrameManager).GetCacheFrame(frameID)
		if err != nil {
			return reconcile.Result{}, err
		}
		for kind, objs := range frameData {
			for nn := range objs {
				if nn.Name == name.Name && nn.Namespace == name.Namespace {
					ctx = context.WithValue(ctx, tag.CleanupKindKey{}, kind)
				}
			}
		}
	}
	req := reconcile.Request{NamespacedName: name}
	return s.Reconciler.Reconcile(ctx, req)
}

// toFrameData converts a slice of runtime objects into a CacheFrame.
func (s *ControllerRuntimeStrategy) toFrameData(objects []runtime.Object) replay.CacheFrame {
	out := make(replay.CacheFrame)
	for _, obj := range objects {
		if obj == nil {
			continue
		}

		if gvk := obj.GetObjectKind().GroupVersionKind(); gvk.Empty() && s.scheme != nil {
			if gvks, _, err := s.scheme.ObjectKinds(obj); err == nil && len(gvks) > 0 {
				obj.GetObjectKind().SetGroupVersionKind(gvks[0])
			}
		}

		u, err := util.ConvertToUnstructured(obj.(client.Object))
		if err != nil {
			// This should ideally not happen if the input is valid
			logger.Error(err, "failed to convert object to unstructured")
			continue
		}

		kind := u.GetKind()
		gvk := u.GroupVersionKind()
		if gvk.Kind == "" {
			gvk.Kind = kind
		}

		canonicalKind := util.CanonicalGroupKind(gvk.Group, gvk.Kind)
		if canonicalKind == "" {
			canonicalKind = util.CanonicalGroupKind("", kind)
		}

		if _, ok := out[canonicalKind]; !ok {
			out[canonicalKind] = make(map[types.NamespacedName]*unstructured.Unstructured)
		}

		namespacedName := types.NamespacedName{
			Name:      u.GetName(),
			Namespace: u.GetNamespace(),
		}
		out[canonicalKind][namespacedName] = u
	}
	return out
}

type ReconcilerContainer struct {
	// The name of the reconciler
	Name string

	Strategy     Strategy
	effectReader effectReader

	// both implemented by the manager type
	versionManager VersionManager
}

func (r *ReconcilerContainer) doReconcile(ctx context.Context, observableState ObjectVersions, req reconcile.Request) (*ReconcileResult, error) {
	frameID := replay.FrameIDFromContext(ctx)

	// convert ObjectVersions to []runtime.Object
	var objects []runtime.Object
	for _, hash := range observableState {
		obj := r.versionManager.Resolve(hash)
		if obj != nil {
			objects = append(objects, obj)
		}
	}

	ctx, cleanup, err := r.Strategy.PrepareState(ctx, objects)
	if err != nil {
		logger.V(1).Error(err, "error preparing state")
		return nil, errors.Wrap(err, "preparing state")
	}
	defer cleanup()

	res, err := r.Strategy.ReconcileAtState(ctx, req.NamespacedName)
	if err != nil {
		return nil, errors.Wrap(err, "executing reconcile")
	}

	logger.V(2).Info("reconcile complete", "result", res)
	effects, err := r.effectReader.GetEffects(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "retrieving reconcile effects")
	}
	if len(effects.ObjectVersions) > 0 && len(effects.Effects) == 0 {
		panic(fmt.Sprintf("reconcile %s (%s) recorded %d object version(s) without effect metadata", frameID, r.Name, len(effects.ObjectVersions)))
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
	if _, err := r.Strategy.ReconcileAtState(ctx, request.NamespacedName); err != nil {
		return nil, errors.Wrap(err, "executing reconcile")
	}
	effects, err := r.effectReader.GetEffects(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "retrieving reconcile effects")
	}
	if len(effects.ObjectVersions) > 0 && len(effects.Effects) == 0 {
		panic(fmt.Sprintf("replay reconcile %s (%s) recorded %d object version(s) without effect metadata", frameID, r.Name, len(effects.ObjectVersions)))
	}
	return &ReconcileResult{
		ControllerID: r.Name,
		FrameID:      frameID,
		FrameType:    FrameTypeReplay,
		Changes:      effects,
	}, nil
}

func Wrap(name string, r reconcile.Reconciler, vm VersionManager, fi frameInserter, er effectReader) *ReconcilerContainer {
	var scheme *runtime.Scheme
	if scProvider, ok := vm.(interface {
		Scheme() *runtime.Scheme
	}); ok {
		scheme = scProvider.Scheme()
	}

	strategy := &ControllerRuntimeStrategy{
		Reconciler:     r,
		frameInserter:  fi,
		reconcilerName: name,
		effectReader:   er,
		scheme:         scheme,
	}
	return &ReconcilerContainer{
		Name:           name,
		Strategy:       strategy,
		effectReader:   er,
		versionManager: vm,
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
