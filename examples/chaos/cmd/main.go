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
	"github.com/tgoodwin/sleeve/pkg/emitter"
	tracecheck "github.com/tgoodwin/sleeve/pkg/tracecheck"
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
	eb := tracecheck.NewExplorerBuilder(scheme)

	eb.WithReconciler("OrchestrationReconciler", func(c tracecheck.Client) tracecheck.Reconciler {
		return &controller.OrchestrationReconciler{
			Client: c,
			Scheme: scheme,
		}
	})
	eb.WithReconciler("HealthReconciler", func(c tracecheck.Client) tracecheck.Reconciler {
		return &controller.HealthReconciler{
			Client: c,
			Scheme: scheme,
		}
	})

	eb.AssignReconcilerToKind("OrchestrationReconciler", "Orchestration")
	eb.AssignReconcilerToKind("HealthReconciler", "Orchestration")
	eb.WithResourceDep("Orchestration", "OrchestrationReconciler", "HealthReconciler")

	logger := zap.New(zap.UseDevMode(true))
	log.SetLogger(logger)

	eb.WithEmitter(emitter.NewDebugEmitter())

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
			Services: []string{"users", "auth", "billing", "shipping"},
		},
	}

	initialState := eb.GetStartStateFromObject(topLevelObj, "OrchestrationReconciler", "HealthReconciler")
	explorer, err := eb.Build("standalone")
	if err != nil {
		panic(err)
	}

	ctx := log.IntoContext(context.Background(), logger)
	result := explorer.Explore(ctx, initialState)

	fmt.Printf("# converged states: %v\n", len(result.ConvergedStates))
	for _, convergedState := range result.ConvergedStates {
		state := convergedState.State
		paths := convergedState.Paths
		fmt.Printf("Converged state: %v\n", state.Objects())
		fmt.Println("# paths to this state: ", len(paths))
		// for _, path := range paths {
		// 	path.Summarize()
		// }
	}
}
