package main

import (
	"flag"
	"fmt"
	"os"
	"sort"

	"slices"

	appsv1 "github.com/tgoodwin/sleeve/examples/robinhood/api/v1"
	controller "github.com/tgoodwin/sleeve/examples/robinhood/controller"
	"github.com/tgoodwin/sleeve/pkg/replay"
	"github.com/tgoodwin/sleeve/pkg/tracecheck"
	"github.com/tgoodwin/sleeve/pkg/util"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var inFile = flag.String("logfile", "default.log", "path to the log file")
var objectID = flag.String("objectID", "", "object ID to analyze")
var reconcileID = flag.String("reconcileID", "", "object ID to analyze")
var debug = flag.Bool("debug", false, "enable debug logging")

var scheme = runtime.NewScheme()

func init() {
	flag.Parse()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(appsv1.AddToScheme(scheme))
}

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
	if *debug {
		builder.Debug()
	}

	builder.AssignReconcilerToKind("RPodReconciler", "RPod")
	builder.AssignReconcilerToKind("FelixReconciler", "RouteConfig")

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

	// sort events by timestamp
	sortedEvents := slices.Clone(events)
	sort.Slice(sortedEvents, func(i, j int) bool {
		return sortedEvents[i].Timestamp < sortedEvents[j].Timestamp
	})
	for _, e := range sortedEvents {
		fmt.Println(e.Timestamp, e.ControllerID, util.Shorter(e.ReconcileID), e.OpType, e.CausalKey().Short())
	}

	km := tracecheck.NewEventKnowledge(store)
	km.Load(events)
	rPodKnowledge := km.Kinds["RPod"]
	rPodKnowledge.Summarize()

	tc := tracecheck.FromBuilder(builder)
	// init := tc.GetStartState()
	// fmt.Println("Initial state:", init)

	tc.AssignReconcilerToKind("RPodReconciler", "RPod")
	tc.AssignReconcilerToKind("FelixReconciler", "RouteConfig")

	deps := make(tracecheck.ResourceDeps)
	deps["RPod"] = make(util.Set[string])
	deps["RPod"].Add("RPodReconciler")
	deps["RPod"].Add("FelixReconciler")

	deps["RouteConfig"] = make(util.Set[string])
	deps["RouteConfig"].Add("FelixReconciler")
	tc.ResourceDeps = deps

	tc.AddReconciler("RPodReconciler", func(c tracecheck.Client) tracecheck.Reconciler {
		return &controller.RPodReconciler{
			Client: c,
			Scheme: scheme,
		}
	})

	tc.AddReconciler("FelixReconciler", func(c tracecheck.Client) tracecheck.Reconciler {
		return &controller.FelixReconciler{
			Client: c,
			Scheme: scheme,
		}
	})

	tc.AddEmitter(tracecheck.NewDebugEmitter())
	explorer := tc.NewExplorer(10)

	reconcileEvents := builder.OrderedReconcileEvents()
	result := explorer.Walk(reconcileEvents)
	fmt.Println("# converged states: ", len(result.ConvergedStates))
	stateNodes := make([]tracecheck.StateNode, 0)
	for _, convergedState := range result.ConvergedStates {
		stateNodes = append(stateNodes, convergedState.State)
	}
	tc.Unique(stateNodes)
	// tc.MaterializeResults(result, "results")
}
