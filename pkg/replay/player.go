package replay

import (
	"context"
	"fmt"

	"maps"

	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/util"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type Harness struct {
	ReconcilerID       string
	frames             []Frame
	frameDataByFrameID frameContainer

	// trace data effect by frameID (reconcileID)
	tracedEffects map[string]DataEffect

	// container for the effects that are recorded during replay
	replayEffects map[string]DataEffect

	predicates []*executionPredicate
}

func newHarness(reconcilerID string, frames []Frame, frameData map[string]FrameData, effects map[string]DataEffect) *Harness {
	replayEffects := make(map[string]DataEffect)
	return &Harness{
		frames:             frames,
		frameDataByFrameID: frameData,
		ReconcilerID:       reconcilerID,
		tracedEffects:      effects,
		replayEffects:      replayEffects,
		predicates:         make([]*executionPredicate, 0),
	}
}

func (p *Harness) FrameData() frameContainer {
	copy := make(map[string]FrameData)
	maps.Copy(copy, p.frameDataByFrameID)
	return copy
}

func (p *Harness) EffectfulFrames() []Frame {
	out := make([]Frame, 0)
	for _, f := range p.frames {
		if len(p.tracedEffects[f.ID].Writes) > 0 {
			return append(out, f)
		}
	}
	return out
}

func (p *Harness) FrameForReconcile(reconcileID string) (*Frame, error) {
	for _, f := range p.frames {
		if f.ID == reconcileID {
			return &f, nil
		}
	}
	return nil, fmt.Errorf("frame not found for reconcileID %s", reconcileID)
}

// return the index of the frame that is closest to the given timestamp while still preceding it
func (p *Harness) priorFrame(ts string) int {
	nearestIndex := -1
	for i, f := range p.frames {
		if f.sequenceID < ts {
			nearestIndex = i
		} else {
			break
		}
	}
	return nearestIndex
}

func (p *Harness) nextFrame(ts string) int {
	for i, f := range p.frames {
		if f.sequenceID > ts {
			return i
		}
	}
	return -1
}

func (p *Harness) nearestFrame(ts string) Frame {
	upperIdx := p.nextFrame(ts)
	lowerIdx := p.priorFrame(ts)

	if upperIdx == -1 {
		//return the last frame
		return p.frames[len(p.frames)-1]
	}
	if lowerIdx == -1 {
		//return the first frame
		return p.frames[0]
	}
	if upperIdx == lowerIdx {
		return p.frames[upperIdx]
	}
	if upperIdx-lowerIdx == 1 {
		return p.frames[lowerIdx]
	}
	panic("ambiguous frame")
}

func (p *Harness) insertFrame(f Frame) {
	ts := f.sequenceID
	prevIdx := p.priorFrame(ts)
	nextIdx := p.nextFrame(ts)

	out := make([]Frame, 0)
	if prevIdx == -1 {
		out = append(out, f)
		out = append(out, p.frames...)
		p.frames = out
		return
	}
	if nextIdx == -1 {
		out = append(out, p.frames...)
		out = append(out, f)
		p.frames = out
		return
	}

	priorFrames := p.frames[:prevIdx+1]
	nextFrames := p.frames[nextIdx:]
	out = append(out, priorFrames...)
	out = append(out, f)
	out = append(out, nextFrames...)
	p.frames = out
}

func (p *Harness) WithPredicate(predicate Predicate) *Harness {
	p.predicates = append(p.predicates, &executionPredicate{evaluate: predicate})
	return p
}

func (p *Harness) ReplayClient(scheme *runtime.Scheme) *Client {
	recorder := &Recorder{
		reconcilerID:    p.ReconcilerID,
		effectContainer: p.replayEffects,
		predicates:      p.predicates,
	}
	return NewClient(p.ReconcilerID, scheme, p.frameDataByFrameID, recorder)
}

func (p *Harness) Load(r reconcile.Reconciler) *Player {
	return &Player{
		reconciler: r,
		harness:    p,
	}
}

type Player struct {
	reconciler reconcile.Reconciler
	harness    *Harness
}

func (r *Player) Play() error {
	for _, f := range r.harness.frames {
		// skip traced frames with no writes
		if f.Type == FrameTypeTraced && len(r.harness.tracedEffects[f.ID].Writes) == 0 {
			continue
		}
		ctx := WithFrameID(context.Background(), f.ID)
		fmt.Printf("Replaying %s frame %s for controller %s\n", f.Type, f.ID, r.harness.ReconcilerID)
		if f.Type == FrameTypeTraced {
			fmt.Printf("Traced Readset:\n%s\n", formatEventList(r.harness.tracedEffects[f.ID].Reads))
			fmt.Printf("Traced Writeset:\n%s\n", formatEventList(r.harness.tracedEffects[f.ID].Writes))
		}

		if _, err := r.reconciler.Reconcile(ctx, f.Req); err != nil {
			fmt.Println("Error during replay:", err)
			return err
		}

		fmt.Printf("Actual Readset:\n%s\n", formatEventList(r.harness.replayEffects[f.ID].Reads))
		fmt.Printf("Actual Writeset:\n%s\n", formatEventList(r.harness.replayEffects[f.ID].Writes))

		// check predicates
		for _, p := range r.harness.predicates {
			if p.satisfied {
				fmt.Println("Predicate satisfied!!!")
				return nil
			}
		}
	}
	return nil
}

func formatEventList(events []event.Event) string {
	if len(events) == 0 {
		return "\t<empty>\n"
	}
	s := ""
	for _, e := range events {
		if event.IsReadOp(e) {
			s += fmt.Sprintf("\t{kind: %s, id: %s, ver: %s}\n", e.Kind, util.Shorter(e.ObjectID), e.Version)
		} else {
			s += fmt.Sprintf("\t{kind: %s, id: %s, op: %s}\n", e.Kind, util.Shorter(e.ObjectID), e.OpType)
		}

	}
	return s
}
