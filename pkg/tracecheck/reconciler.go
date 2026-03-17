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

type frameReader interface {
	GetCacheFrame(id string) (replay.CacheFrame, error)
}

type Strategy interface {
	PrepareState(ctx context.Context, state []runtime.Object) (context.Context, func(), error)
	ReconcileAtState(ctx context.Context, name types.NamespacedName) (reconcile.Result, error)
}

type ControllerRuntimeStrategy struct {
	reconcile.Reconciler
	frameInserter
	name ReconcilerID
	effectReader
	scheme *runtime.Scheme
}

func NewControllerRuntimeStrategy(r reconcile.Reconciler, fi frameInserter, er effectReader, name ReconcilerID) *ControllerRuntimeStrategy {
	return &ControllerRuntimeStrategy{
		Reconciler:    r,
		frameInserter: fi,
		name:          name,
		effectReader:  er,
	}
}

func (s *ControllerRuntimeStrategy) PrepareState(ctx context.Context, state []runtime.Object) (context.Context, func(), error) {
	frameID := replay.FrameIDFromContext(ctx)
	frameData := runtimeObjectsToCacheFrame(state, s.scheme)
	s.InsertCacheFrame(frameID, frameData)
	cleanup := func() {}
	return ctx, cleanup, nil
}

func (s *ControllerRuntimeStrategy) ReconcileAtState(ctx context.Context, name types.NamespacedName) (reconcile.Result, error) {
	req := reconcile.Request{NamespacedName: name}
	return s.Reconciler.Reconcile(ctx, req)
}

type cleanupRuntimeStrategy struct {
	*ControllerRuntimeStrategy
	frameReader frameReader
}

func newCleanupRuntimeStrategy(base *ControllerRuntimeStrategy, frameReader frameReader) *cleanupRuntimeStrategy {
	return &cleanupRuntimeStrategy{
		ControllerRuntimeStrategy: base,
		frameReader:               frameReader,
	}
}

func (s *cleanupRuntimeStrategy) ReconcileAtState(ctx context.Context, name types.NamespacedName) (reconcile.Result, error) {
	frameID := replay.FrameIDFromContext(ctx)
	frameData, err := s.frameReader.GetCacheFrame(frameID)
	if err != nil {
		return reconcile.Result{}, err
	}

	for kind, objs := range frameData {
		if _, ok := objs[name]; ok {
			ctx = context.WithValue(ctx, tag.CleanupKindKey{}, kind)
			break
		}
	}

	return s.ControllerRuntimeStrategy.ReconcileAtState(ctx, name)
}

// runtimeObjectsToCacheFrame converts a slice of runtime objects into a replay cache frame.
func runtimeObjectsToCacheFrame(objects []runtime.Object, scheme *runtime.Scheme) replay.CacheFrame {
	out := make(replay.CacheFrame)
	for _, obj := range objects {
		if obj == nil {
			continue
		}

		if gvk := obj.GetObjectKind().GroupVersionKind(); gvk.Empty() && scheme != nil {
			if gvks, _, err := scheme.ObjectKinds(obj); err == nil && len(gvks) > 0 {
				obj.GetObjectKind().SetGroupVersionKind(gvks[0])
			}
		}

		u, err := util.ConvertToUnstructured(obj.(client.Object))
		if err != nil {
			panic(fmt.Sprintf("could not convert object to unstructured: %v", err))
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
	Name           ReconcilerID
	Strategy       Strategy
	effectReader   effectReader
	versionManager VersionManager
}

func (r *ReconcilerContainer) doReconcile(ctx context.Context, observableState ObjectVersions, req reconcile.Request) (*ReconcileResult, error) {
	frameID := replay.FrameIDFromContext(ctx)

	// convert ObjectVersions to []runtime.Object
	var objects = make([]runtime.Object, 0, len(observableState))
	var unresolvedKeys []string
	for key, hash := range observableState {
		obj := r.versionManager.Resolve(hash)
		if obj != nil {
			objects = append(objects, obj)
		} else {
			unresolvedKeys = append(unresolvedKeys, key.String())
		}
	}
	if len(unresolvedKeys) > 0 {
		// FAIL LOUDLY: This should never happen in normal operation
		errMsg := fmt.Sprintf("%d/%d objects could not be resolved for reconciler %s. Unresolved: %v",
			len(unresolvedKeys), len(observableState), r.Name, unresolvedKeys)
		logger.Error(errors.New(errMsg), "error resolving objects")
		panic(errMsg)
	}

	ctx, cleanup, err := r.Strategy.PrepareState(ctx, objects)
	if err != nil {
		logger.V(1).Error(err, "error preparing state")
		return nil, errors.Wrap(err, "preparing state")
	}
	defer cleanup()

	res, reconcileErr := r.Strategy.ReconcileAtState(ctx, req.NamespacedName)

	// Always retrieve effects, even when the reconcile returned an error.
	// In real Kubernetes, API writes that occurred before the error are
	// durable — they already landed on the API server. Discarding them
	// would be a simulation fidelity gap.
	logger.V(2).Info("reconcile complete", "result", res, "crashed", reconcileErr != nil)
	effects, err := r.effectReader.GetEffects(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "retrieving reconcile effects")
	}
	if len(effects.ObjectVersions) > 0 && len(effects.Effects) == 0 {
		panic(fmt.Sprintf("reconcile %s (%s) recorded %d object version(s) without effect metadata", frameID, r.Name, len(effects.ObjectVersions)))
	}
	deltas := r.computeDeltas(observableState, effects.ObjectVersions)

	if reconcileErr != nil && !isFaultInjectionCrash(reconcileErr) {
		// Return the error so the caller can handle re-enqueue semantics,
		// but include any effects that were already recorded. The caller
		// should apply these effects (they represent durable API writes)
		// and also re-enqueue the reconciler.
		return &ReconcileResult{
			ControllerID: r.Name,
			FrameID:      frameID,
			FrameType:    FrameTypeExplore,
			Changes:      effects,
			Deltas:       deltas,
			ctrlRes:      res,
		}, errors.Wrap(reconcileErr, "executing reconcile")
	}

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

func Wrap(name ReconcilerID, r reconcile.Reconciler, vm VersionManager, fi frameInserter, er effectReader) *ReconcilerContainer {
	var scheme *runtime.Scheme
	if scProvider, ok := vm.(interface {
		Scheme() *runtime.Scheme
	}); ok {
		scheme = scProvider.Scheme()
	}

	strategy := &ControllerRuntimeStrategy{
		Reconciler:    r,
		frameInserter: fi,
		name:          name,
		effectReader:  er,
		scheme:        scheme,
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
