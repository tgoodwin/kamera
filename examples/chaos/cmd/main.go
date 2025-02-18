package main

import (
	"context"
	"flag"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"

	appsv1 "github.com/tgoodwin/sleeve/examples/chaos/api/v1"
	controller "github.com/tgoodwin/sleeve/examples/chaos/internal/controller"
	tracecheck "github.com/tgoodwin/sleeve/pkg/tracecheck"
	"github.com/tgoodwin/sleeve/pkg/util"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var scheme = runtime.NewScheme()

func init() {
	flag.Parse()

	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(appsv1.AddToScheme(scheme))
}

func main() {
	tc := tracecheck.NewTraceChecker(scheme)
	deps := make(tracecheck.ResourceDeps)

	tc.AddReconciler("OrchestrationReconciler", func(c tracecheck.Client) tracecheck.Reconciler {
		return &controller.OrchestrationReconciler{
			Client: c,
			Scheme: scheme,
		}
	})
	tc.AddReconciler("HealthReconciler", func(c tracecheck.Client) tracecheck.Reconciler {
		return &controller.HealthReconciler{
			Client: c,
			Scheme: scheme,
		}
	})

	tc.AssignReconcilerToKind("OrchestrationReconciler", "Orchestration")
	tc.AssignReconcilerToKind("HealthReconciler", "Orchestration")

	deps["Orchestration"] = make(util.Set[string])
	deps["Orchestration"].Add("OrchestrationReconciler")
	deps["Orchestration"].Add("HealthReconciler")
	tc.ResourceDeps = deps

	logger := zap.New(zap.UseDevMode(true))
	log.SetLogger(logger)

	tc.AddEmitter(tracecheck.NewDebugEmitter())

	topLevelObj := &appsv1.Orchestration{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "orchestralmaneuvers",
			Namespace: "default",
			Labels: map[string]string{
				"tracey-uid":                       "foo",
				"discrete.events/sleeve-object-id": "foo-123",
			},
		},
		TypeMeta: metav1.TypeMeta{
			APIVersion: "appsv1",
			Kind:       "Orchestration",
		},
		Spec: appsv1.OrchestrationSpec{
			Services: []string{"users", "auth"},
		},
	}

	initialState := tc.GetStartStateFromObject(topLevelObj, "OrchestrationReconciler", "HealthReconciler")
	explorer := tc.NewExplorer(20)

	ctx := log.IntoContext(context.Background(), logger)
	result := explorer.Explore(ctx, initialState)
	tc.SummarizeResults(result)
	tc.MaterializeResults(result, "results")

	fmt.Printf("# converged states: %v\n", len(result.ConvergedStates))
	fmt.Println("Duration: ", result.Duration)
	fmt.Println("Aborted paths: ", result.AbortedPaths)
	for _, convergedState := range result.ConvergedStates {
		state := convergedState.State
		paths := convergedState.Paths
		fmt.Printf("Converged state: %v\n", state.ObjectVersions)
		fmt.Println("# paths to this state: ", len(paths))
		// for _, path := range paths {
		// 	path.Summarize()
		// }
	}
}
