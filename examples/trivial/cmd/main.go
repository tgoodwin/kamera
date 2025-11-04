package main

import (
	"context"
	"fmt"

	"github.com/tgoodwin/kamera/pkg/event"
	appsv1 "github.com/tgoodwin/kamera/pkg/test/integration/api/v1"
	"github.com/tgoodwin/kamera/pkg/test/integration/controller"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	sleevelog "github.com/tgoodwin/kamera/pkg/util/logger"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
)

var scheme = runtime.NewScheme()

func formatResults(paths []tracecheck.ExecutionHistory) [][]string {
	var formatted [][]string
	for _, path := range paths {
		var formattedPath []string
		for _, r := range path {
			formattedPath = append(formattedPath, fmt.Sprintf("%s@%d", r.ControllerID, len(r.Deltas)))
		}
		formatted = append(formatted, formattedPath)
	}
	return formatted
}

func main() {
	log := sleevelog.GetLogger(sleevelog.Debug)
	ctrl.SetLogger(log)
	tracecheck.SetLogger(ctrl.Log.WithName("tracecheck"))

	eb := tracecheck.NewExplorerBuilder(scheme)
	eb.WithMaxDepth(10)
	// eb.WithDebug()
	eb.WithEmitter(event.NewInMemoryEmitter())
	eb.WithReconciler("FooController", func(c tracecheck.Client) tracecheck.Reconciler {
		return &controller.TestReconciler{
			Client: c,
			Scheme: scheme,
		}
	})
	eb.WithReconciler("BarController", func(c tracecheck.Client) tracecheck.Reconciler {
		return &controller.TestReconciler{
			Client: c,
			Scheme: scheme,
		}
	})
	const fooKind = "webapp.discrete.events/Foo"
	eb.WithResourceDep(fooKind, "FooController", "BarController")
	eb.AssignReconcilerToKind("FooController", fooKind)
	eb.AssignReconcilerToKind("BarController", fooKind)

	// Testing two controllers whos behavior is identical
	// and who both depend on the same object.

	topLevelObj := &appsv1.Foo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
			Labels: map[string]string{
				"tracey-uid":                       "foo",
				"discrete.events/sleeve-object-id": "foo-123",
			},
		},
		TypeMeta: metav1.TypeMeta{
			APIVersion: "webapp.discrete.events/v1",
			Kind:       "Foo",
		},
		Spec: appsv1.FooSpec{
			Mode: "A",
		},
	}

	initialState := eb.GetStartStateFromObject(topLevelObj, "FooController", "BarController")
	explorer, err := eb.Build("standalone")
	if err != nil {
		panic("err")
	}

	// observable := initialState.Contents.Observable()
	// base := initialState.Contents.All()
	// no staleness here - the observable should be the same as the base

	result := explorer.Explore(context.Background(), initialState)
	fmt.Println("number of converged states: ", len(result.ConvergedStates))

	for _, converged := range result.ConvergedStates {
		fmt.Println("converged state: ", converged.State.Objects())
		fmt.Println("number of paths to this state: ", len(converged.Paths))
		formattedPaths := formatResults(converged.Paths)
		for _, path := range formattedPaths {
			fmt.Println(path)
		}
	}
}
