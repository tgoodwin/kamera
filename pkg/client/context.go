package client

import (
	"context"
	"sync"

	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"github.com/tgoodwin/sleeve/pkg/tag"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
	getReconcile func(ctx context.Context) string
	reconcilerID string
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

func (ct *ContextTracker) TrackOperation(ctx context.Context, obj client.Object, op OperationType) {
	if err := tag.SanityCheckLabels(obj); err != nil {
		panic(err)
	}
	rid := ct.getReconcile(ctx)
	ct.rc.SetReconcileID(rid)

	if op == GET || op == LIST {
		rootID, err := tag.GetRootID(obj)
		if err != nil {
			panic(err)
		}
		ct.rc.SetRootID(rootID)

		// log the observed object version
		r := snapshot.AsRecord(obj)
		ct.emitter.LogObjectVersion(r)
	}

	// assign a change label to the object
	if _, ok := mutationTypes[op]; ok {
		tag.LabelChange(obj)
	}

	// assign a sleeve objectID to the object if it is being created
	if op == CREATE {
		tag.AddSleeveObjectID(obj)
	}

	if op == DELETE {
		tag.AddDeletionID(obj)
	}

	ct.emitter.LogOperation(Operation(obj, rid, ct.reconcilerID, ct.rc.GetRootID(), op))
	// propagate labels after logging so we capture the label values prior to the operation
	// e.g. we want to log out "prev-write-reconcile-id" before we overwrite it
	// with the current reconcileID when we are propagating labels
	// however, only do this for mutation operations

	if _, ok := mutationTypes[op]; ok {
		ct.propagateLabels(obj)
	}
}
