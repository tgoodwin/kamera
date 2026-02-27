package explore

import "github.com/tgoodwin/kamera/pkg/tracecheck"

func cloneUserActions(actions []tracecheck.UserAction) []tracecheck.UserAction {
	if len(actions) == 0 {
		return nil
	}

	cloned := make([]tracecheck.UserAction, 0, len(actions))
	for _, action := range actions {
		copyAction := action
		if action.Payload != nil {
			copyAction.Payload = action.Payload.DeepCopyObject()
		}
		cloned = append(cloned, copyAction)
	}

	return cloned
}
