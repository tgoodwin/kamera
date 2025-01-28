package replay

import (
	"context"

	sleeveclient "github.com/tgoodwin/sleeve/pkg/client"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/util"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type EffectRecorder interface {
	RecordEffect(ctx context.Context, obj client.Object, opType event.OperationType) error
}

type DataEffect struct {
	Reads  []event.Event
	Writes []event.Event
}

type EffectContainer map[string]DataEffect

func (ec EffectContainer) AddEffect(frameID string, e *event.Event) {
	de, exists := ec[frameID]
	if !exists {
		de = DataEffect{}
	}

	if event.IsReadOp(*e) {
		de.Reads = append(de.Reads, *e)
	} else if event.IsWriteOp(*e) {
		de.Writes = append(de.Writes, *e)
	}

	ec[frameID] = de
}

type Recorder struct {
	reconcilerID    string
	effectContainer EffectContainer

	predicates []*executionPredicate
}

var _ EffectRecorder = (*Recorder)(nil)

func (r *Recorder) RecordEffect(ctx context.Context, obj client.Object, opType event.OperationType) error {
	reconcileID := FrameIDFromContext(ctx)
	e := sleeveclient.Operation(obj, reconcileID, r.reconcilerID, "<REPLAY>", opType)
	r.effectContainer.AddEffect(reconcileID, e)

	if event.IsWriteOp(*e) {
		// in the case where we are recording a perturbed execution,
		// see if the perturbation produced the desired effect
		r.evaluatePredicates(ctx, obj)
	}

	return nil
}

func (r *Recorder) evaluatePredicates(_ context.Context, obj client.Object) {
	uns, err := util.ConvertToUnstructured(obj)
	if err != nil {
		panic(err)
	}
	for _, p := range r.predicates {
		if p.evaluate(uns) {
			p.satisfied = true
		}
	}
}
