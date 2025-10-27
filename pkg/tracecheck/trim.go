package tracecheck

import (
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// TrimStatesForInspection drops heavy references that are not needed by the interactive inspector.
// This helps reduce memory pressure when inspecting large exploration result sets.
func TrimStatesForInspection(states []ResultState) []ResultState {
	for i := range states {
		states[i].State.TrimForInspection()
		for _, path := range states[i].Paths {
			for _, step := range path {
				if step == nil {
					continue
				}
				step.ctrlRes = reconcile.Result{}
			}
		}
	}
	return states
}
