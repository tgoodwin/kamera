package replay

import (
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/util"
)

type Node struct {
	// The name of the node
	Name string
	// The children of the node
	Children []*Node
}

func FindReadDeps(events []event.Event) map[string]util.Set[string] {
	// reads by controller ID
	reads := make(map[string]util.Set[string])
	for _, e := range events {
		if event.IsReadOp(event.OperationType(e.OpType)) {
			if _, ok := reads[e.ControllerID]; !ok {
				reads[e.ControllerID] = util.NewSet[string]()
			}
			reads[e.ControllerID].Add(e.CanonicalGroupKind())
		}
	}
	return reads
}

func FindWriteDeps(events []event.Event) map[string]util.Set[string] {
	// writes by controller ID
	writes := make(map[string]util.Set[string])
	for _, e := range events {
		if event.IsWriteOp(event.OperationType(e.OpType)) {
			if _, ok := writes[e.ControllerID]; !ok {
				writes[e.ControllerID] = util.NewSet[string]()
			}
			writes[e.ControllerID].Add(e.CanonicalGroupKind())
		}
	}
	return writes
}
