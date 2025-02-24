package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/tgoodwin/sleeve/pkg/replay"
	"github.com/tgoodwin/sleeve/pkg/tracecheck"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var inFile = flag.String("logfile", "default.log", "path to the log file")
var objectID = flag.String("objectID", "", "object ID to analyze")
var reconcileID = flag.String("reconcileID", "", "object ID to analyze")

func main() {
	flag.Parse()
	f, err := os.Open(*inFile)
	if err != nil {
		panic(err.Error())
	}
	defer f.Close()
	data, err := os.ReadFile(*inFile)
	if err != nil {
		panic(err.Error())
	}
	builder, err := replay.ParseTrace(data)
	if err != nil {
		panic(err.Error())
	}
	builder.Debug()
	if *reconcileID != "" {
		builder.AnalyzeReconcile(*reconcileID)
	}
	if *objectID != "" {
		builder.AnalyzeObject(*objectID)
	}

	events := builder.Events()
	store := builder.Store()

	logger := zap.New(zap.UseDevMode(true))
	tracecheck.SetLogger(logger)

	km := tracecheck.NewGlobalKnowledge(store)
	km.Load(events)
	rPodKnowledge := km.Kinds["RPod"]
	rPodKnowledge.Summarize()

	tc := tracecheck.FromBuilder(builder)
	init := tc.GetStartState()
	fmt.Println("Initial state:", init)
	// := tc.NewExplorer(10)
}
