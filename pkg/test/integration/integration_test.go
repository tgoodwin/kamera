package test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	appsv1 "github.com/tgoodwin/sleeve/pkg/test/integration/api/v1"
	"github.com/tgoodwin/sleeve/pkg/test/integration/controller"
	"github.com/tgoodwin/sleeve/pkg/tracecheck"
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

func TestExhaustiveInterleavings(t *testing.T) {
	eb := tracecheck.NewExplorerBuilder(scheme)
	eb.WithMaxDepth(10)
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
	eb.WithResourceDep("Foo", "FooController", "BarController")
	eb.AssignReconcilerToKind("FooController", "Foo")
	eb.AssignReconcilerToKind("BarController", "Foo")

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
			APIVersion: "appsv1",
			Kind:       "Foo",
		},
		Spec: appsv1.FooSpec{
			Mode: "A",
		},
	}

	initialState := eb.GetStartStateFromObject(topLevelObj, "FooController", "BarController")
	explorer, err := eb.Build("standalone")
	if err != nil {
		t.Fail()
	}

	result := explorer.Explore(context.Background(), initialState)

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

func TestConvergedStateIdentification(t *testing.T) {
	eb := tracecheck.NewExplorerBuilder(scheme)
	eb.WithMaxDepth(10)
	eb.WithEmitter(event.NewInMemoryEmitter())

	// Testing two controllers whos behavior is identical
	// and who both depend on the same object.
	eb.WithReconciler("FooController", func(c tracecheck.Client) tracecheck.Reconciler {
		return &controller.FooReconciler{
			Client: c,
			Scheme: scheme,
		}
	})
	eb.WithReconciler("BarController", func(c tracecheck.Client) tracecheck.Reconciler {
		return &controller.BarReconciler{
			Client: c,
			Scheme: scheme,
		}
	})
	eb.WithResourceDep("Foo", "FooController", "BarController")
	eb.AssignReconcilerToKind("FooController", "Foo")
	eb.AssignReconcilerToKind("BarController", "Foo")

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

	sb := eb.NewStateEventBuilder()
	initialState := sb.AddTopLevelObject(topLevelObj, "FooController", "BarController")
	explorer, err := eb.Build("standalone")
	if err != nil {
		t.Fatal(err)
	}

	result := explorer.Explore(context.Background(), initialState)
	assert.Equal(t, 2, len(result.ConvergedStates))

	expected := []struct {
		objects       tracecheck.ObjectVersions
		numPaths      int
		pathSummaries [][]string
	}{
		{
			objects: tracecheck.ObjectVersions{
				snapshot.IdentityKey{"Foo", "foo-123"}: snapshot.NewDefaultHash(
					`{
						"apiVersion": "appsv1",
						"kind": "Foo",
						"metadata": {
							"creationTimestamp": null,
							"labels": {
								"discrete.events/change-id": "CHANGE_ID",
								"discrete.events/creator-id": "CREATOR_ID",
								"discrete.events/prev-write-reconcile-id": "RECONCILE_ID",
								"discrete.events/root-event-id": "foo",
								"discrete.events/sleeve-object-id": "OBJECT_ID",
								"tracey-uid": "foo"
							},
							"name": "foo",
							"namespace": "default"
						},
						"spec": {
							"mode": "A"
						},
						"status": {
							"state": "A-Final"
						}
					}`),
			},
			numPaths: 2,
			pathSummaries: [][]string{
				{"FooController@1", "FooController@1", "BarController@1"},
				{"FooController@1", "FooController@1", "FooController@1"},
			},
		},
		{
			objects: tracecheck.ObjectVersions{
				snapshot.IdentityKey{"Foo", "foo-123"}: snapshot.NewDefaultHash(
					`{
						"apiVersion": "appsv1",
						"kind": "Foo",
						"metadata": {
							"annotations": {
								"mode-flip-done": "true"
							},
							"creationTimestamp": null,
							"labels": {
								"discrete.events/change-id": "CHANGE_ID",
								"discrete.events/creator-id": "CREATOR_ID",
								"discrete.events/prev-write-reconcile-id": "RECONCILE_ID",
								"discrete.events/root-event-id": "foo",
								"discrete.events/sleeve-object-id": "OBJECT_ID",
								"tracey-uid": "foo"
							},
							"name": "foo",
							"namespace": "default"
						},
						"spec": {
							"mode": "B"
						},
						"status": {
							"state": "B-Final"
						}
					}`),
			},
			numPaths: 2,
			pathSummaries: [][]string{
				{"FooController@1", "BarController@1", "FooController@1", "FooController@1", "BarController@1"},
				{"FooController@1", "BarController@1", "FooController@1", "FooController@1", "FooController@1"},
			},
		},
	}

	for _, expectedState := range expected {
		var matchedState *tracecheck.ConvergedState
		for _, convergedState := range result.ConvergedStates {
			match := true
			for key, vHash := range convergedState.State.Objects() {
				expectedJSON := string(expectedState.objects[key].Value)
				expectedJSON = strings.ReplaceAll(expectedJSON, "\n", "")
				expectedJSON = strings.ReplaceAll(expectedJSON, "\t", "")
				expectedJSON = strings.ReplaceAll(expectedJSON, " ", "")
				if expectedJSON != string(vHash.Value) {
					match = false
					break
				}
			}
			if match {
				matchedState = &convergedState
				break
			}
		}
		if matchedState == nil {
			t.Errorf("no matching converged state found for expected state: %+v", expectedState.objects)
			continue
		}
		uniquePaths := tracecheck.GetUniquePaths(matchedState.Paths)
		assert.Equal(t, expectedState.numPaths, len(uniquePaths))
		actualPathSummaries := formatResults(uniquePaths)
		assert.ElementsMatch(t, expectedState.pathSummaries, actualPathSummaries)
	}
}
