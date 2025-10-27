package interactive

import (
	"fmt"

	"github.com/tgoodwin/kamera/pkg/tracecheck"
)

func validateResultStates(states []tracecheck.ResultState) []tracecheck.ResultState {
	for stateIdx, state := range states {
		for pathIdx, path := range state.Paths {
			for stepIdx, step := range path {
				if step == nil {
					continue
				}
				writeCount := len(step.Changes.ObjectVersions)
				effectCount := len(step.Changes.Effects)
				if writeCount > 0 && effectCount == 0 {
					panic(fmt.Sprintf("state integrity violation: frame=%s controller=%s recorded %d object version(s) but zero effect(s) (state=%d path=%d step=%d)",
						step.FrameID, step.ControllerID, writeCount, stateIdx, pathIdx, stepIdx))
				}
			}
		}
	}
	return states
}
