package test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/simclock"
	foov1 "github.com/tgoodwin/kamera/pkg/test/integration/api/v1"
	"github.com/tgoodwin/kamera/pkg/test/integration/controller"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	"github.com/tgoodwin/kamera/pkg/util"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func TestSimClockAdvancesWithExploreDepth(t *testing.T) {
	restoreDepth := simclock.SetDepth(0)
	t.Cleanup(restoreDepth)

	startTime := simclock.Now()

	eb := tracecheck.NewExplorerBuilder(scheme)
	eb.WithMaxDepth(5)
	eb.WithEmitter(event.NewInMemoryEmitter())
	eb.WithReconciler("SimClockController", func(c ctrlclient.Client) tracecheck.Reconciler {
		return &controller.SimClockReconciler{Client: c, Scheme: scheme}
	})

	fooKind := util.CanonicalGroupKind("webapp.discrete.events", "Foo")
	eb.WithResourceDep(fooKind, "SimClockController")
	eb.AssignReconcilerToKind("SimClockController", fooKind)

	foo := &foov1.Foo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "clocked",
			Namespace: "default",
			Labels: map[string]string{
				"tracey-uid":                       "clocked",
				"discrete.events/sleeve-object-id": "clocked-123",
			},
		},
		TypeMeta: metav1.TypeMeta{APIVersion: "webapp.discrete.events/v1", Kind: "Foo"},
		Spec:     foov1.FooSpec{Mode: "A"},
	}

	initialState := eb.GetStartStateFromObject(foo, "SimClockController")
	explorer, err := eb.Build("standalone")
	require.NoError(t, err)

	result := explorer.Explore(context.Background(), initialState)
	require.Len(t, result.ConvergedStates, 1)

	converged := result.ConvergedStates[0]
	expectedTypes := []string{"ReadyPhaseOne", "ReadyPhaseTwo", "ReadyPhaseThree"}

	require.Len(t, converged.Paths, 1)
	require.GreaterOrEqual(t, len(converged.Paths[0]), len(expectedTypes))

	var resolvedFoo foov1.Foo
	for _, vHash := range converged.State.Objects() {
		obj := converged.Resolver.Resolve(vHash)
		require.NoError(t, runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &resolvedFoo))
		break
	}

	clockConds := make(map[string]metav1.Condition)
	for _, cond := range resolvedFoo.Status.Conditions {
		existing, ok := clockConds[cond.Type]
		if !ok || cond.LastTransitionTime.Before(&existing.LastTransitionTime) {
			clockConds[cond.Type] = cond
		}
	}

	require.Len(t, clockConds, len(expectedTypes))

	for i, typ := range expectedTypes {
		cond := clockConds[typ]
		expected := startTime.Add(time.Duration(i) * time.Hour)
		require.True(t, cond.LastTransitionTime.Time.Equal(expected))
	}

	for _, step := range converged.Paths[0] {
		require.Equal(t, "SimClockController", step.ControllerID)
	}
}
