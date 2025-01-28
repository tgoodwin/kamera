package graph

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"

	"github.com/goccy/go-graphviz"
	"github.com/samber/lo"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/util"
)

var readOps map[event.OperationType]struct{} = map[event.OperationType]struct{}{
	event.GET:  {},
	event.LIST: {},
}

func BackfillLabels(events []*event.Event) []*event.Event {
	readEvents := lo.Filter(events, func(e *event.Event, _ int) bool {
		_, ok := readOps[event.OperationType(e.OpType)]
		return ok
	})

	byChangeID := make(map[string]*event.Event, 0)
	for _, e := range readEvents {
		var changeId string
		var ok bool
		changeId, ok = e.Labels["change-id"]
		if !ok {
			changeId = e.Labels["root-event-id"]
		}
		byChangeID[changeId] = e
	}

	for _, e := range events {
		if _, ok := readOps[event.OperationType(e.OpType)]; ok {
			continue
		}

		changeId, ok := e.Labels["change-id"]
		if !ok {
			continue
			// panic("no change-id on a write event")
		}

		if readEvent, ok := byChangeID[changeId]; ok {
			e.Kind = readEvent.Kind
			e.ObjectID = readEvent.ObjectID
			e.Version = readEvent.Version
		} else {
			fmt.Println("no corresponding downstream read event for change-id", changeId)
			continue
		}
	}

	return events
}

func getNodeName(e *event.Event) string {
	return fmt.Sprintf("%s:%s:%s", e.Kind, util.Shorter(e.ObjectID), util.Shorter(e.CausalKey().String()))
}

func Graph(events []*event.Event) {
	ctx := context.Background()
	g, err := graphviz.New(ctx)
	if err != nil {
		panic(err)
	}
	graph, err := g.Graph()
	if err != nil {
		panic(err)
	}
	defer g.Close()

	backfilled := BackfillLabels(events)
	fmt.Println("Graphing", len(backfilled), "events")
	byReconcileID := lo.GroupBy(backfilled, func(e *event.Event) string {
		return e.ReconcileID
	})
	for rid, events := range byReconcileID {
		fmt.Println("ReconcileID", rid, "has", len(events), "events")
		readEvents := lo.Filter(events, func(e *event.Event, _ int) bool {
			_, ok := readOps[event.OperationType(e.OpType)]
			return ok
		})
		writeEvents := lo.Filter(events, func(e *event.Event, _ int) bool {
			_, ok := readOps[event.OperationType(e.OpType)]
			return !ok
		})
		fmt.Println("Read events:", len(readEvents))
		fmt.Println("Write events:", len(writeEvents))
		for _, writeEvent := range writeEvents {
			w, _ := graph.CreateNodeByName(getNodeName(writeEvent))
			for _, readEvent := range readEvents {
				r, _ := graph.CreateNodeByName(getNodeName(readEvent))
				e, _ := graph.CreateEdgeByName(fmt.Sprintf("%s:%s", readEvent.CausalKey().String(), writeEvent.CausalKey().String()), r, w)
				e.SetLabel(util.Shorter(rid))
			}
		}
	}

	var b bytes.Buffer
	if err := g.Render(ctx, graph, graphviz.PNG, &b); err != nil {
		panic(err)
	}

	if err := g.RenderFilename(ctx, graph, graphviz.PNG, "graph.png"); err != nil {
		panic(err)
	}

	// Open the image with the default system viewer
	if err := exec.Command("open", "graph.png").Start(); err != nil {
		panic(err)
	}
}
