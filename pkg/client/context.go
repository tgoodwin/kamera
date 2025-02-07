package client

import (
	"context"
	"fmt"
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

	rootIDByReconcileID map[string]string

	mu sync.Mutex
}

func (rc *ReconcileContext) SetReconcileID(id string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.reconcileID = id
}

func (rc *ReconcileContext) SetRootID(reconcileID, rootID string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if rc.rootIDByReconcileID == nil {
		rc.rootIDByReconcileID = make(map[string]string)
	}
	rc.rootID = rootID

	rc.rootIDByReconcileID[rc.reconcileID] = rootID
}

func (rc *ReconcileContext) GetReconcileID() string {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.reconcileID
}

func (rc *ReconcileContext) GetRootID(reconcileID string) string {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.rootIDByReconcileID[reconcileID]
}

type frameExtractor func(ctx context.Context) string

type ContextTracker struct {
	rc           *ReconcileContext
	emitter      event.Emitter
	getFrameID   frameExtractor
	reconcilerID string

	mu sync.Mutex
}

func NewContextTracker(reconcilerID string, emitter event.Emitter, extract frameExtractor) *ContextTracker {
	return &ContextTracker{
		rc:           &ReconcileContext{},
		getFrameID:   extract,
		reconcilerID: reconcilerID,
		emitter:      emitter,
	}
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
	rootID := ct.rc.GetRootID(ct.rc.reconcileID)
	out[tag.TraceyCreatorID] = ct.reconcilerID
	if _, ok := out[tag.TraceyRootID]; !ok {
		if rootID == "" {
			fmt.Printf("current propagation target: %#v\n", target)
			panic("rootID is empty")
		}
		out[tag.TraceyRootID] = rootID
	}

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
		// frameID changed, so reset the rootID
		ct.rc.SetRootID(currFrameID, "")
		ct.rc.SetReconcileID(frameID)
	}
}

func (ct *ContextTracker) setRootContext(ctx context.Context, obj client.Object) {
	ct.setReconcileID(ctx)
	rootID, err := tag.GetRootID(obj)
	if err != nil {
		log.V(2).WithValues("labels", obj.GetLabels()).Error(err, "setting root context")
		panic(err)
	}
	if rootID == "" {
		log.Error(nil, "rootID is empty")
		panic("rootID is empty")
	}
	currRootID, ok := ct.rc.rootIDByReconcileID[ct.rc.GetReconcileID()]
	if ok && currRootID != rootID {
		log.V(0).Error(err, "rootID changed within the reconcile", "currRootID", currRootID, "newRootID", rootID)
	}

	currReconcileID := ct.rc.GetReconcileID()
	ct.rc.SetRootID(currReconcileID, rootID)
}

// TODO refactor cause this is only ever called by GET and LIST
func (ct *ContextTracker) TrackOperation(ctx context.Context, obj client.Object, op event.OperationType) {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	if err := tag.SanityCheckLabels(obj); err != nil {
		log.V(0).Error(err, "sanity checking object labels")
	}

	if op == event.GET || op == event.LIST {
		ct.setRootContext(ctx, obj)

		// log the observed object version
		// r := snapshot.AsRecord(obj, ct.rc.GetReconcileID())
		// ct.emitter.LogObjectVersion(r)
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

	operation := Operation(obj, ct.rc.reconcileID, ct.reconcilerID, ct.rc.GetRootID(ct.rc.reconcileID), op)
	ct.emitter.LogOperation(operation)
	r := snapshot.AsRecord(obj, ct.rc.GetReconcileID())
	r.OperationID = operation.ID
	ct.emitter.LogObjectVersion(r)

	// TODO REMOVE THIS
	// We need to know if the actual API operation succeeded or not to determine if we should log the operation
	// but we dont have that information here.

	// OLD COMMENT
	// propagate labels after logging so we capture the label values prior to the operation
	// e.g. we want to log out "prev-write-reconcile-id" before we overwrite it
	// with the current reconcileID when we are propagating labels
	// however, only do this for mutation operations

	if _, ok := event.MutationTypes[op]; ok {
		ct.propagateLabels(obj)
	}
}
