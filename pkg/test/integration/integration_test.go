package test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tgoodwin/kamera/pkg/event"
	foov1 "github.com/tgoodwin/kamera/pkg/test/integration/api/v1"
	"github.com/tgoodwin/kamera/pkg/test/integration/controller"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	"github.com/tgoodwin/kamera/pkg/util"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(foov1.AddToScheme(scheme))
}

func canonicalizeKindSequences(seq tracecheck.KindSequences) tracecheck.KindSequences {
	if seq == nil {
		return nil
	}
	out := make(tracecheck.KindSequences, len(seq))
	for k, v := range seq {
		if strings.Contains(k, "/") {
			out[k] = v
			continue
		}
		out[util.CanonicalGroupKind(groupForTestKind(k), k)] = v
	}
	return out
}

func groupForTestKind(kind string) string {
	switch kind {
	case "Foo":
		return "webapp.discrete.events"
	default:
		return ""
	}
}

func summarizeState(state tracecheck.ResultState) (status string, hasAnnotation bool, ok bool) {
	for _, vHash := range state.State.Objects() {
		if state.Resolver == nil {
			return "", false, false
		}
		obj := state.Resolver.Resolve(vHash)
		if obj == nil {
			return "", false, false
		}
		status, found, err := unstructured.NestedString(obj.Object, "status", "state")
		if err != nil || !found {
			return "", false, false
		}
		annotation, _, _ := unstructured.NestedString(obj.Object, "metadata", "annotations", "mode-flip-done")
		return status, annotation == "true", true
	}
	return "", false, false
}

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

func TestExhaustiveInterleavings(t *testing.T) {
	opts := zap.Options{
		Development: true,
	}
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	tracecheck.SetLogger(ctrl.Log.WithName("tracecheck").V(2))

	eb := tracecheck.NewExplorerBuilder(scheme)
	eb.WithMaxDepth(10)
	// eb.WithDebug()
	eb.WithEmitter(event.NewInMemoryEmitter())
	eb.WithReconciler("FooController", func(c ctrlclient.Client) tracecheck.Reconciler {
		return &controller.TestReconciler{
			Client: c,
			Scheme: scheme,
		}
	})
	eb.WithReconciler("BarController", func(c ctrlclient.Client) tracecheck.Reconciler {
		return &controller.TestReconciler{
			Client: c,
			Scheme: scheme,
		}
	})
	fooKind := "webapp.discrete.events/Foo"
	eb.WithResourceDep(fooKind, "FooController", "BarController")
	eb.AssignReconcilerToKind("FooController", fooKind)
	eb.AssignReconcilerToKind("BarController", fooKind)

	// Testing two controllers whos behavior is identical
	// and who both depend on the same object.

	topLevelObj := &foov1.Foo{
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
		Spec: foov1.FooSpec{
			Mode: "A",
		},
	}

	initialState := eb.GetStartStateFromObject(topLevelObj, "FooController", "BarController")
	initialState.Contents.KindSequences = canonicalizeKindSequences(initialState.Contents.KindSequences)
	explorer, err := eb.Build("standalone")
	if err != nil {
		t.Fatal()
	}

	observable := initialState.Contents.Observable()
	base := initialState.Contents.All()
	// no staleness here - the observable should be the same as the base
	assert.Equal(t, observable, base)

	result := explorer.Explore(context.Background(), initialState)

	if len(result.ConvergedStates) != 1 {
		for _, state := range result.ConvergedStates {
			fmt.Println(state.State.Objects())
		}
		t.Fatalf("expected 1 result, got %d", len(result.ConvergedStates))
	}
	convergedState := result.ConvergedStates[0]
	if len(convergedState.Paths) != 4 {
		t.Errorf("expected 4 paths, got %d", len(convergedState.Paths))
	}

	expected := [][]string{
		{"FooController@1", "BarController@1", "BarController@0", "FooController@0"},
		{"FooController@1", "FooController@1", "BarController@0", "FooController@0"},
		{"BarController@1", "BarController@1", "BarController@0", "FooController@0"},
		{"BarController@1", "FooController@1", "BarController@0", "FooController@0"},
	}
	actual := formatResults(convergedState.Paths)

	assert.ElementsMatch(t, expected, actual)
}

func TestConvergedStateIdentification(t *testing.T) {
	eb := tracecheck.NewExplorerBuilder(scheme)
	eb.WithMaxDepth(10)
	eb.WithEmitter(event.NewInMemoryEmitter())

	// Testing two controllers whos behavior is identical
	// and who both depend on the same object.
	eb.WithReconciler("FooController", func(c ctrlclient.Client) tracecheck.Reconciler {
		return &controller.FooReconciler{
			Client: c,
			Scheme: scheme,
		}
	})
	eb.WithReconciler("BarController", func(c ctrlclient.Client) tracecheck.Reconciler {
		return &controller.BarReconciler{
			Client: c,
			Scheme: scheme,
		}
	})
	eb.WithResourceDep("webapp.discrete.events/Foo", "FooController", "BarController")
	eb.AssignReconcilerToKind("FooController", "webapp.discrete.events/Foo")
	eb.AssignReconcilerToKind("BarController", "webapp.discrete.events/Foo")

	topLevelObj := &foov1.Foo{
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
		Spec: foov1.FooSpec{
			Mode: "A",
		},
	}

	sb := eb.NewStateEventBuilder()
	initialState := sb.AddTopLevelObject(topLevelObj, "FooController", "BarController")
	initialState.Contents.KindSequences = canonicalizeKindSequences(initialState.Contents.KindSequences)
	explorer, err := eb.Build("standalone")
	if err != nil {
		t.Fatal(err)
	}

	result := explorer.Explore(context.Background(), initialState)
	assert.Equal(t, 2, len(result.ConvergedStates))

	expected := []struct {
		status        string
		hasAnnotation bool
		numPaths      int
		pathSummaries [][]string
	}{
		{
			status:        "A-Final",
			hasAnnotation: false,
			numPaths:      2,
			pathSummaries: [][]string{
				{"FooController@1", "FooController@1", "BarController@1"},
				{"FooController@1", "FooController@1", "FooController@1"},
			},
		},
		{
			status:        "B-Final",
			hasAnnotation: true,
			numPaths:      2,
			pathSummaries: [][]string{
				{"FooController@1", "BarController@1", "FooController@1", "FooController@1", "BarController@1"},
				{"FooController@1", "BarController@1", "FooController@1", "FooController@1", "FooController@1"},
			},
		},
	}

	for _, expectedState := range expected {
		var matchedState *tracecheck.ResultState
		for _, convergedState := range result.ConvergedStates {
			status, hasAnnotation, ok := summarizeState(convergedState)
			if !ok {
				continue
			}
			if status == expectedState.status && hasAnnotation == expectedState.hasAnnotation {
				matchedState = &convergedState
				break
			}
		}
		if matchedState == nil {
			for _, convergedState := range result.ConvergedStates {
				t.Logf("converged state objects: %+v", convergedState.State.Objects())
			}
			t.Errorf("no matching converged state found for expected state: status=%s, annotation=%t", expectedState.status, expectedState.hasAnnotation)
			continue
		}
		uniquePaths := tracecheck.GetUniquePaths(matchedState.Paths)
		assert.Equal(t, expectedState.numPaths, len(uniquePaths))
		actualPathSummaries := formatResults(uniquePaths)
		assert.ElementsMatch(t, expectedState.pathSummaries, actualPathSummaries)
	}
}
