package client

import (
	"context"
	"fmt"
	"sync"

	"github.com/tgoodwin/sleeve/pkg/event"
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

	// if strict is true, panic if the labels necessary for full causal tracing are not present
	// full causal tracing requires every controller in the system to be instrumented with sleeve,
	// which may not be the case, so this is off by default
	strict bool

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

func (ct *ContextTracker) handleError(msg string) {
	if ct.strict {
		panic(msg)
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
			ct.handleError("rootID is empty")
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
			// this indicates there's a bug in our code. we set the reconcileID in the context
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

func (ct *ContextTracker) setRootContextFromObservation(ctx context.Context, obj client.Object) error {
	ct.setReconcileID(ctx)
	gvk := obj.GetObjectKind().GroupVersionKind()
	name := obj.GetName()
	rootID, err := tag.GetRootID(obj)
	if err != nil {
		log.V(2).WithValues("labels", obj.GetLabels()).Error(err, "setting root context")
		ct.handleError(fmt.Sprintf("no root ID on object - gvk: %s, name: %s", gvk, name))
	}
	if rootID == "" {
		log.Error(nil, "rootID is empty")
		ct.handleError(fmt.Sprintf("no root ID on object - gvk: %s, name: %s", gvk, name))
	}
	currRootID, ok := ct.rc.rootIDByReconcileID[ct.rc.GetReconcileID()]
	if ok && currRootID != rootID {
		log.V(0).Error(err, "rootID changed within the reconcile", "currRootID", currRootID, "newRootID", rootID)

		// Prioritize the first rootID we see.
		// Sometimes we may observe a Resource with a different rootID than the one
		// that the top-level Reconcile was invoked with. This happens if the controller
		// queries for resources that existed prior to the top-level causal event
		// corresponding to the first rootID we set within this reconcile.
		// For now, we just ignore the rootID update and honor the first one we saw.
		// TODO: SLE-27
		return nil
	}

	currReconcileID := ct.rc.GetReconcileID()
	ct.rc.SetRootID(currReconcileID, rootID)
	return nil
}

func (Ct *ContextTracker) MustSetRootContextFromObservation(ctx context.Context, obj client.Object) {
	if err := Ct.setRootContextFromObservation(ctx, obj); err != nil {
		panic(err)
	}
}

// TODO refactor cause this is only ever called by GET and LIST
func (ct *ContextTracker) TrackOperation(ctx context.Context, obj client.Object, op event.OperationType) {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	if err := tag.SanityCheckLabels(obj); err != nil {
		log.V(0).Error(err, "sanity checking object labels")
	}

	if op == event.GET || op == event.LIST {
		ct.MustSetRootContextFromObservation(ctx, obj)
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

	if _, ok := event.MutationTypes[op]; ok {
		ct.propagateLabels(obj)
	}
}
