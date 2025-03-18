package main

import (
	"flag"
	"log"
	"sort"

	"github.com/samber/lo"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/tracecheck"
	"github.com/tgoodwin/sleeve/pkg/util"
)

func main() {
	logfile := flag.String("logfile", "app.log", "path to the log file")
	flag.Parse()

	traces, err := tracecheck.ParseJSONLTrace(*logfile)
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

	log.Println("Write operations:")
	for _, t := range traces {
		if event.IsWriteOp(t.Effect.OpType) {
			log.Printf("  %s %s %s %s %s %s", t.Timestamp, util.Shorter(t.RootEventID), t.OpType, t.Kind, util.Shorter(t.ObjectID), t.ControllerID)
		}
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

	log.Println("Traces sorted by timestamp")
}
