package tracecheck

import (
	"sort"

	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// TODO this code can likely be replaced by the staleness eventsourcing package

// joinRecord facilitates conversion from trace event data to tracecheck data
type joinRecord struct {
	event       event.Event
	ikey        snapshot.IdentityKey
	versionHash snapshot.VersionHash
	// cache data is queried by namespace/name
	nsName types.NamespacedName
}

type converterImpl struct {
	orderedJoinRecords []joinRecord
	framesByReconciler map[string][]string
	reconcileIDToReads map[string]ObjectVersions
}

func newConverter(things []joinRecord) *converterImpl {
	sort.Slice(things, func(i, j int) bool {
		return things[i].event.Timestamp < things[j].event.Timestamp
	})

	reads := lo.Filter(things, func(t joinRecord, _ int) bool {
		return event.IsReadOp(event.OperationType(t.event.OpType))
	})

	byReconcileID := lo.GroupBy(reads, func(t joinRecord) string {
		return t.event.ReconcileID
	})

	reconcileIDToReads := make(map[string]ObjectVersions)
	for reconcileID, reads := range byReconcileID {
		out := make(ObjectVersions)
		for _, r := range reads {
			cKey := snapshot.NewCompositeKey(r.ikey.Kind, r.nsName.Namespace, r.nsName.Name, r.ikey.ObjectID)
			out[cKey] = r.versionHash
		}
		reconcileIDToReads[reconcileID] = out
	}

	byReconciler := lo.GroupBy(things, func(t joinRecord) string {
		return t.event.ControllerID
	})
	framesByReconciler := make(map[string][]string)
	for reconciler, things := range byReconciler {
		sort.Slice(things, func(i, j int) bool {
			return things[i].event.Timestamp < things[j].event.Timestamp
		})
		framesByReconciler[reconciler] = lo.Map(things, func(t joinRecord, _ int) string {
			return t.event.ReconcileID
		})
	}

	return &converterImpl{
		orderedJoinRecords: things,
		framesByReconciler: framesByReconciler,
		reconcileIDToReads: reconcileIDToReads,
	}
}

func (c *converterImpl) getStateAtReconcileStart(reconcileID string) (ObjectVersions, error) {
	// need to also include reads from previous reconciles
	ov, ok := c.reconcileIDToReads[reconcileID]
	if !ok {
		return nil, errors.New("reconcileID not found")
	}
	return ov, nil
}

func (c *converterImpl) getFirstReconcileID(reconcilerID string) (string, error) {
	frames, ok := c.framesByReconciler[reconcilerID]
	if !ok {
		return "", errors.New("reconcilerID not found")
	}
	if len(frames) == 0 {
		return "", errors.New("no reconcile frames for reconciler")
	}
	return frames[0], nil
}

func (c *converterImpl) getNextReconcile(reconcilerID, frameID string) (string, error) {
	frames, ok := c.framesByReconciler[reconcilerID]
	if !ok {
		return "", errors.New("reconcilerID not found")
	}
	for i, f := range frames {
		if f == frameID {
			if i+1 < len(frames) {
				return frames[i+1], nil
			} else {
				return "", errors.New("no next frame")
			}
		}
	}
	return "", errors.New("frameID not found")
}

func (c *converterImpl) getStart() StateNode {
	firstRecord := c.orderedJoinRecords[0]
	firstOV := c.reconcileIDToReads[firstRecord.event.ReconcileID]
	return StateNode{
		Contents: StateSnapshot{
			contents: firstOV,
			KindSequences: KindSequences{
				firstRecord.ikey.Kind: 1,
			},
		},
		PendingReconciles: []PendingReconcile{
			{
				ReconcilerID: firstRecord.event.ControllerID,
				Request:      reconcile.Request{},
			},
		},
	}
}
