package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/tgoodwin/sleeve/pkg/tracecheck"
	"k8s.io/apimachinery/pkg/runtime"
)

func main() {
	logfile := flag.String("logfile", "app.log", "path to the log file")
	slug := flag.String("slug", "default", "slug representing sleeve object ID prefix")
	reconcileID := flag.String("reconcileID", "", "object ID to analyze (optional)")
	flag.Parse()

	// eb := tracecheck.NewExplorerBuilder(runtime.NewScheme())

	// this is stateful
	// traces, err := eb.ParseJSONLTrace(*logfile)
	// if err != nil {
	// 	log.Fatalf("failed to parse JSONL trace: %v", err)
	// }
	// log.Printf("Parsed %d trace events", len(traces))

	// sort.Slice(traces, func(i, j int) bool {
	// 	return traces[i].Timestamp < traces[j].Timestamp
	// })

	// fmt.Println("===before rollup===")
	// for _, e := range traces {
	// 	sleeveObjectID := e.Effect.Key.IdentityKey.ObjectID
	// 	fmt.Printf("ts:%s (%d) frameID:%s controller=%s op=%s item=%s:%s %s\n", e.Timestamp, e.Sequence, util.Shorter(e.ReconcileID), e.ControllerID, e.OpType, e.Kind, util.Shorter(sleeveObjectID), util.ShortenHash(e.Effect.Version.Value))
	// }
	// fmt.Println("===before rollup===")

	// byKind := lo.GroupBy(traces, func(t tracecheck.StateEvent) string {
	// 	return t.Kind
	// })
	// for kind, traces := range byKind {
	// 	log.Printf("Kind: %s, count: %d", kind, len(traces))
	// 	byOpType := lo.GroupBy(traces, func(t tracecheck.StateEvent) string {
	// 		return t.OpType
	// 	})
	// 	for opType, traces := range byOpType {
	// 		log.Printf("  OpType: %s, count: %d", opType, len(traces))
	// 	}
	// }

	// preroll := tracecheck.AssignResourceVersions(traces)
	// for _, e := range preroll {
	// 	sleeveObjectID := e.Effect.Key.IdentityKey.ObjectID
	// 	fmt.Printf("ts:%s (%d) frameID:%s controller=%s op=%s item=%s:%s %s\n", e.Timestamp, e.Sequence, util.Shorter(e.ReconcileID), e.ControllerID, e.OpType, e.Kind, util.Shorter(sleeveObjectID), util.ShortenHash(e.Effect.Version.Value))
	// }
	// topState := tracecheck.CausalRollup(traces)
	// topState.Debug()

	eb2 := tracecheck.NewExplorerBuilder(runtime.NewScheme())
	startTime := time.Now()
	lensManager, err := eb2.BuildLensManager(*logfile)
	elapsedTime := time.Since(startTime)
	log.Printf("BuildLensManager completed in %s", elapsedTime)
	if err != nil {
		log.Fatalf("failed to create lens manager: %v", err)
	}
	err = lensManager.LifecycleLens(*slug)
	if err != nil {
		log.Fatalf("failed to get lifecycle lens: %v", err)
	}

	fmt.Println("===provenance===")
	provenanceStart := time.Now()
	err = lensManager.ProvenanceLens(*slug)
	if err != nil {
		log.Fatalf("failed to get provenance lens: %v", err)
	}
	provenanceElapsed := time.Since(provenanceStart)
	log.Printf("ProvenanceLens completed in %s", provenanceElapsed)
	fmt.Println("===lifecycle===")
	lifecycleStart := time.Now()
	err = lensManager.LifecycleLens(*slug)
	if err != nil {
		log.Fatalf("failed to get lifecycle lens: %v", err)
	}
	lifecycleElapsed := time.Since(lifecycleStart)
	log.Printf("LifecycleLens completed in %s", lifecycleElapsed)
	if *reconcileID != "" {
		fmt.Println("===knowledge===")
		knowledgeStart := time.Now()
		err = lensManager.KnowledgeLens(*reconcileID)
		if err != nil {
			log.Fatalf("failed to get knowledge lens: %v", err)
		}
		knowledgeElapsed := time.Since(knowledgeStart)
		log.Printf("KnowledgeLens completed in %s", knowledgeElapsed)

		fmt.Println("===Replay===")
		replayStart := time.Now()
		err = lensManager.ReplayLens(*reconcileID)
		if err != nil {
			log.Fatalf("failed to get replay lens: %v", err)
		}
		replayElapsed := time.Since(replayStart)
		log.Printf("ReplayLens completed in %s", replayElapsed)
	}
}
