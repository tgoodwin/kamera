package main

import (
	"flag"
	"fmt"
	"log"
	"sort"

	"github.com/samber/lo"
	"github.com/tgoodwin/sleeve/pkg/tracecheck"
	"github.com/tgoodwin/sleeve/pkg/util"
	"k8s.io/apimachinery/pkg/runtime"
)

func main() {
	logfile := flag.String("logfile", "app.log", "path to the log file")
	slug := flag.String("slug", "default", "slug representing sleeve object ID prefix")
	flag.Parse()

	eb := tracecheck.NewExplorerBuilder(runtime.NewScheme())

	// this is stateful
	traces, err := eb.ParseJSONLTrace(*logfile)
	if err != nil {
		log.Fatalf("failed to parse JSONL trace: %v", err)
	}
	log.Printf("Parsed %d trace events", len(traces))

	sort.Slice(traces, func(i, j int) bool {
		return traces[i].Timestamp < traces[j].Timestamp
	})

	fmt.Println("===before rollup===")
	for _, e := range traces {
		sleeveObjectID := e.Effect.Key.IdentityKey.ObjectID
		fmt.Printf("ts:%s (%d) frameID:%s controller=%s op=%s item=%s:%s %s\n", e.Timestamp, e.Sequence, util.Shorter(e.ReconcileID), e.ControllerID, e.OpType, e.Kind, util.Shorter(sleeveObjectID), util.ShortenHash(e.Effect.Version.Value))
	}
	fmt.Println("===before rollup===")

	byKind := lo.GroupBy(traces, func(t tracecheck.StateEvent) string {
		return t.Kind
	})
	for kind, traces := range byKind {
		log.Printf("Kind: %s, count: %d", kind, len(traces))
		byOpType := lo.GroupBy(traces, func(t tracecheck.StateEvent) string {
			return t.OpType
		})
		for opType, traces := range byOpType {
			log.Printf("  OpType: %s, count: %d", opType, len(traces))
		}
	}

	preroll := tracecheck.AssignResourceVersions(traces)
	for _, e := range preroll {
		sleeveObjectID := e.Effect.Key.IdentityKey.ObjectID
		fmt.Printf("ts:%s (%d) frameID:%s controller=%s op=%s item=%s:%s %s\n", e.Timestamp, e.Sequence, util.Shorter(e.ReconcileID), e.ControllerID, e.OpType, e.Kind, util.Shorter(sleeveObjectID), util.ShortenHash(e.Effect.Version.Value))
	}
	topState := tracecheck.CausalRollup(traces)
	topState.Debug()
	fmt.Println("HI")

	eb2 := tracecheck.NewExplorerBuilder(runtime.NewScheme())
	lensManager, err := eb2.LensManager(*logfile)
	if err != nil {
		log.Fatalf("failed to create lens manager: %v", err)
	}
	err = lensManager.LifecycleLens(*slug)
	if err != nil {
		log.Fatalf("failed to get lifecycle lens: %v", err)
	}
}
