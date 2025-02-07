package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tgoodwin/sleeve/pkg/event"
	appsv1 "github.com/tgoodwin/sleeve/pkg/test/integration/api/v1"
	"github.com/tgoodwin/sleeve/pkg/test/integration/internal/controller"
	"github.com/tgoodwin/sleeve/pkg/tracecheck"
	"github.com/tgoodwin/sleeve/pkg/util"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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

func TestIntegration(t *testing.T) {
	tc := tracecheck.NewTraceChecker(scheme)
	deps := make(tracecheck.ResourceDeps)
	deps["Foo"] = make(util.Set[string])
	deps["Foo"].Add("FooController")
	deps["Foo"].Add("BarController")

	tc.ResourceDeps = deps

	tc.AddReconciler("FooController", func(c tracecheck.Client) tracecheck.Reconciler {
		return &controller.FooTrivialReconciler{
			Client: c,
			Scheme: scheme,
		}
	})
	tc.AddReconciler("BarController", func(c tracecheck.Client) tracecheck.Reconciler {
		return &controller.BarTrivialReconciler{
			Client: c,
			Scheme: scheme,
		}
	})

	tc.AssignReconcilerToKind("FooController", "Foo")
	tc.AssignReconcilerToKind("BarController", "Foo")

	tc.AddEmitter(event.NewInMemoryEmitter())

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
			APIVersion: "appsv1",
			Kind:       "Foo",
		},
		Spec: appsv1.FooSpec{
			Mode: "A",
		},
	}

	initialState := tc.GetStartStateFromObject(topLevelObj, "FooController", "BarController")
	explorer := tc.NewExplorer(10)

	result := explorer.Explore(context.Background(), initialState)
	tc.SummarizeResults(result)

	if len(result.ConvergedStates) != 1 {
		t.Errorf("expected 1 result, got %d", len(result.ConvergedStates))
	}
	convergedState := result.ConvergedStates[0]
	if len(convergedState.Paths) != 8 {
		t.Errorf("expected 8 paths, got %d", len(convergedState.Paths))
	}

	expected := [][]string{
		{"FooController@1", "BarController@1", "FooController@0", "BarController@0"},
		{"FooController@1", "BarController@1", "BarController@0", "FooController@0"},

		{"FooController@1", "FooController@1", "BarController@0", "FooController@0"},
		{"FooController@1", "FooController@1", "FooController@0", "BarController@0"},

		{"BarController@1", "FooController@1", "FooController@0", "BarController@0"},
		{"BarController@1", "FooController@1", "BarController@0", "FooController@0"},

		{"BarController@1", "BarController@1", "BarController@0", "FooController@0"},
		{"BarController@1", "BarController@1", "FooController@0", "BarController@0"},
	}
	actual := formatResults(convergedState.Paths)

	assert.ElementsMatch(t, expected, actual)

}
