package main

import (
	"context"
	"fmt"

	rcv1 "github.com/tgoodwin/kamera/examples/racecondition/api/v1"
	"github.com/tgoodwin/kamera/examples/racecondition/internal/controller"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/interactive"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	sleevelog "github.com/tgoodwin/kamera/pkg/util/logger"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
)

func main() {
	scheme := runtime.NewScheme()
	utilruntime.Must(rcv1.AddToScheme(scheme))

	log := sleevelog.GetLogger(sleevelog.Info)
	ctrl.SetLogger(log)
	tracecheck.SetLogger(ctrl.Log.WithName("tracecheck"))

	builder := tracecheck.NewExplorerBuilder(scheme)
	builder.WithEmitter(event.NewInMemoryEmitter())
	builder.WithMaxDepth(6)

	builder.WithReconciler("AlphaController", func(c tracecheck.Client) tracecheck.Reconciler {
		return &controller.ToggleController{Client: c, Scheme: scheme, ControllerID: "alpha"}
	})
	builder.WithReconciler("BetaController", func(c tracecheck.Client) tracecheck.Reconciler {
		return &controller.ToggleController{Client: c, Scheme: scheme, ControllerID: "beta"}
	})

	builder.AssignReconcilerToKind("AlphaController", "Toggle")
	builder.AssignReconcilerToKind("BetaController", "Toggle")
	builder.WithResourceDep("Toggle", "AlphaController", "BetaController")

	explorer, err := builder.Build("standalone")
	if err != nil {
		panic(fmt.Sprintf("build explorer: %v", err))
	}

	toggle := &rcv1.Toggle{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "race-toggle",
			Namespace: "default",
		},
		Spec: rcv1.ToggleSpec{Desired: "alpha"},
	}

	tag.AddSleeveObjectID(toggle)

	initialState := builder.GetStartStateFromObject(toggle, "AlphaController", "BetaController")

	result := explorer.Explore(context.Background(), initialState)
	fmt.Printf("converged states: %d\n", len(result.ConvergedStates))
	fmt.Printf("aborted states: %d\n", len(result.AbortedStates))

	states := append(append([]tracecheck.ResultState{}, result.ConvergedStates...), result.AbortedStates...)
	if len(states) == 0 {
		fmt.Println("no results to inspect")
		return
	}

	interactive.RunStateInspectorTUIView(states, true)
}
