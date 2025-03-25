package main

import (
	"flag"
	"fmt"
	"log"
	"sort"

	"github.com/samber/lo"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/tracecheck"
	"github.com/tgoodwin/sleeve/pkg/util"
	"k8s.io/apimachinery/pkg/runtime"
)

func main() {
	logfile := flag.String("logfile", "app.log", "path to the log file")
	flag.Parse()

	eb := tracecheck.NewExplorerBuilder(runtime.NewScheme())

	traces, err := eb.ParseJSONLTrace(*logfile)
	if err != nil {
		log.Fatalf("failed to parse JSONL trace: %v", err)
	}
	log.Printf("Parsed %d trace events", len(traces))

	sort.Slice(traces, func(i, j int) bool {
		return traces[i].Timestamp < traces[j].Timestamp
	})

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

	for _, trace := range traces {
		if !event.IsWriteOp(event.OperationType(trace.OpType)) {
			continue
		}
		fmt.Printf("Timestamp: %s, Kind: %s, ObjectID: %s, %s:%s\n", trace.Timestamp, trace.Kind, util.Shorter(trace.ObjectID), trace.ControllerID, trace.OpType)
	}

	topState := tracecheck.Rollup(traces)
	log.Print("state keys in rollup:")
	allKeys := lo.Keys(topState.All())
	sort.Slice(allKeys, func(i, j int) bool {
		return allKeys[i].IdentityKey.Kind < allKeys[j].IdentityKey.Kind
	})
	for _, key := range allKeys {
		log.Printf("  %s", key)
	}

	log.Printf("kind sequences")
	for kind, seq := range topState.KindSequences {
		log.Printf("  %s: %d", kind, seq)
	}

	log.Println("Traces sorted by timestamp")
}
