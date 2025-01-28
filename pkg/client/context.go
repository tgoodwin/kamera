package client

import (
	"context"
	"sync"

	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"github.com/tgoodwin/sleeve/pkg/tag"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrl "sigs.k8s.io/controller-runtime/pkg/controller"
)

type ReconcileContext struct {
	reconcileID string
	rootID      string

	mu sync.Mutex
}

func (rc *ReconcileContext) SetReconcileID(id string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.reconcileID = id
}

func (rc *ReconcileContext) SetRootID(id string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.rootID = id
}

func (rc *ReconcileContext) GetReconcileID() string {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.reconcileID
}

func (rc *ReconcileContext) GetRootID() string {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.rootID
}

type ContextTracker struct {
	rc           *ReconcileContext
	emitter      event.Emitter
	getFrameID   func(ctx context.Context) string
	reconcilerID string
}

func NewProdTracker(reconcilerID string) *ContextTracker {
	return &ContextTracker{
		rc: &ReconcileContext{},
		getFrameID: func(ctx context.Context) string {
			return string(ctrl.ReconcileIDFromContext(ctx))
		},
		reconcilerID: reconcilerID,
		emitter:      event.NewLogEmitter(log),
	}
}

func (ct *ContextTracker) propagateLabels(target client.Object) {
	currLabels := target.GetLabels()
	out := make(map[string]string)
	for k, v := range currLabels {
		out[k] = v
	}
	out[tag.TraceyCreatorID] = ct.reconcilerID
	out[tag.TraceyRootID] = ct.rc.GetRootID()

	// update prev-write-reconcile-id to the current reconcileID
	out[tag.TraceyReconcileID] = ct.rc.GetReconcileID()

	target.SetLabels(out)
}

type reconcileIDKey struct{}

func (ct *ContextTracker) setReconcileID(ctx context.Context) {
	var frameID string
	frameID = ct.getFrameID(ctx)
	if frameID == "" {
		f, ok := ctx.Value(reconcileIDKey{}).(string)
		if !ok {
			panic("reconcileID not set in context")
		}
		frameID = f
	}
	currFrameID := ct.rc.GetReconcileID()
	if currFrameID == "" {
		// first time setting the frameID
		ct.rc.SetReconcileID(frameID)
	} else if currFrameID != frameID {
		log.V(2).Info("frameID changed", "currFrameID", currFrameID, "newFrameID", frameID)
		// frameID changed within the reconcile, so reset the rootID
		ct.rc.SetRootID("")
		ct.rc.SetReconcileID(frameID)
	}
}

func (ct *ContextTracker) setRootContext(obj client.Object) {
	rootID, err := tag.GetRootID(obj)
	if err != nil {
		panic(err)
	}
	currRootID := ct.rc.GetRootID()
	if currRootID != "" && currRootID != rootID {
		log.V(0).Error(err, "rootID changed within the reconcile", "currRootID", currRootID, "newRootID", rootID)
	}

	ct.rc.SetRootID(rootID)
}

func (ct *ContextTracker) TrackOperation(ctx context.Context, obj client.Object, op event.OperationType) {
	if err := tag.SanityCheckLabels(obj); err != nil {
		log.V(0).Error(err, "sanity checking object labels")
	}
	rid := ct.getFrameID(ctx)
	ct.rc.SetReconcileID(rid)

	if op == event.GET || op == event.LIST {
		ct.setRootContext(obj)

		// log the observed object version
		r := snapshot.AsRecord(obj)
		ct.emitter.LogObjectVersion(r)
	}

	// assign a change label to the object
	if _, ok := event.MutationTypes[op]; ok {
		tag.LabelChange(obj)
	}

	// assign a sleeve objectID to the object if it is being created
	if op == event.CREATE {
		tag.AddSleeveObjectID(obj)
	}

	if op == event.DELETE {
		tag.AddDeletionID(obj)
	}

	ct.emitter.LogOperation(Operation(obj, rid, ct.reconcilerID, ct.rc.GetRootID(), op))
	// propagate labels after logging so we capture the label values prior to the operation
	// e.g. we want to log out "prev-write-reconcile-id" before we overwrite it
	// with the current reconcileID when we are propagating labels
	// however, only do this for mutation operations

	if _, ok := event.MutationTypes[op]; ok {
		ct.propagateLabels(obj)
	}
}
