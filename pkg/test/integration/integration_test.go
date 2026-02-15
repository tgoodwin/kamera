package test

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/tgoodwin/kamera/pkg/event"
	foov1 "github.com/tgoodwin/kamera/pkg/test/integration/api/v1"
	"github.com/tgoodwin/kamera/pkg/test/integration/controller"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	"github.com/tgoodwin/kamera/pkg/util"
	gozap "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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

// canonicalizeKindSequences rewrites the keys of the provided KindSequences map
// to use canonical "group/kind" string notation. If the key already contains a "/"
// (i.e., is already canonical), it is copied through as-is. Otherwise, it infers
// the group using groupForTestKind and rewrites the key using util.CanonicalGroupKind.
// This ensures that all KindSequences keys are normalized for comparison during tests.
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

func summarizeState(state tracecheck.ResultState, resolver tracecheck.VersionManager) (status string, hasAnnotation bool, ok bool) {
	if resolver == nil {
		return "", false, false
	}
	for _, vHash := range state.State.Objects() {
		obj := resolver.Resolve(vHash)
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

func setExplorePermuteConfig(eb *tracecheck.ExplorerBuilder, perms map[tracecheck.ReconcilerID]bool) {
	cfg := eb.Config()
	for id, enabled := range perms {
		cfg.PermuteOrder[id] = enabled
	}
	eb.SetConfig(cfg)
}

func TestExhaustiveInterleavings(t *testing.T) {
	opts := zap.Options{
		Development: true,
	}
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	tracecheck.SetLogger(ctrl.Log.WithName("tracecheck").V(2))

	eb := tracecheck.NewExplorerBuilder(scheme)
	eb.WithMaxDepth(10)
	eb.WithOptimizations(tracecheck.OptimizationConfig{
		OrderingPruning:      true,
		OnlyPermuteTriggered: true,
	})
	fooKind := "webapp.discrete.events/Foo"
	eb.WithReconciler("FooController", func(c ctrlclient.Client) tracecheck.Reconciler {
		return &controller.TestReconciler{
			Client: c,
			Scheme: scheme,
		}
	}).For(fooKind).Watches(fooKind, tracecheck.EnqueueRequestForObject())

	eb.WithReconciler("BarController", func(c ctrlclient.Client) tracecheck.Reconciler {
		return &controller.TestReconciler{
			Client: c,
			Scheme: scheme,
		}
	}).For(fooKind).Watches(fooKind, tracecheck.EnqueueRequestForObject())
	setExplorePermuteConfig(eb, map[tracecheck.ReconcilerID]bool{
		"FooController": true,
		"BarController": true,
	})

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

	// State0: A/{Foo, Bar}  (Initial pending list respects argument order: [Foo, Bar])
	//
	// Path 1 (current head Foo):
	//   ├─ Foo@1 →
	//   │   State1: A-1/{Foo, Bar} (Triggered set sorted: [Bar, Foo])
	//   │     BRANCH #2 (StateHash A-1) expands pending [Bar, Foo]
	//   │     ├─ (current) Bar@1 →
	//   │     │   State2: A-Final/{Foo, Bar} (Pending [Bar, Foo])
	//   │     │     BRANCH #3 (StateHash A-Final)
	//   │     │     ├─ (current) Bar@0 → Foo@0 (no-op tail)
	//   │     │     └─ (enqueued) Foo@0 → Bar@0 (no-op tail)
	//   │     └─ (from BRANCH #2 enqueued) Foo@1 →
	//   │         State2': A-Final/{Foo, Bar} (matches State2 hash; branching skipped)
	//   │         Proceeds with current head (Bar) as triggered (Foo) are added to the back.
	//   │         └─ Bar@0 → Foo@0 (no-op tail)
	//
	// Path 2 (enqueued head Bar):
	//   └─ Bar@1 →
	//       State1': A-1/{Foo, Bar} (matches State1 hash; branching skipped)
	//       Only runs current head of pending [Foo, Bar] which is Foo. (trigged Bar added to back of pending)
	//       Alternative branch (Bar) not taken because State1 expansion covers it.
	//       └─ Foo@1 → Bar@0 → Foo@0
	//
	// Resulting Paths:
	//   1. Foo@1 → Bar@1 → Bar@0 → Foo@0
	//   2. Foo@1 → Foo@1 → Bar@0 → Foo@0
	//   3. Bar@1 → Foo@1 → Bar@0 → Foo@0

	actual := formatResults(convergedState.Paths)
	// Trailing no-ops may appear in any order; we preserve the natural execution order
	// rather than sorting. The deduplication considers different no-op orderings as equivalent.
	expectedAll := [][]string{
		{"FooController@1", "BarController@1", "FooController@0", "BarController@0"},
		{"FooController@1", "FooController@1", "BarController@0", "FooController@0"},
		{"BarController@1", "FooController@1", "BarController@0", "FooController@0"},
	}
	assert.Len(t, actual, 3)
	assert.ElementsMatch(t, expectedAll, actual)
}

// runFooBarExplore executes the Foo/Bar controller scenario with the given optimizations.
// It returns the explorer (to inspect stats) and the exploration result.
func runFooBarExplore(t *testing.T, opt tracecheck.OptimizationConfig) (*tracecheck.Explorer, *tracecheck.Result) {
	t.Helper()

	eb := tracecheck.NewExplorerBuilder(scheme)
	eb.WithMaxDepth(10)
	if opt.AnyEnabled() {
		eb.WithOptimizations(opt)
	} else {
		eb.WithoutOptimizations()
	}

	fooKind := "webapp.discrete.events/Foo"
	eb.WithReconciler("FooController", func(c ctrlclient.Client) tracecheck.Reconciler {
		return &controller.TestReconciler{
			Client: c,
			Scheme: scheme,
		}
	}).For(fooKind).Watches(fooKind, tracecheck.EnqueueRequestForObject())

	eb.WithReconciler("BarController", func(c ctrlclient.Client) tracecheck.Reconciler {
		return &controller.TestReconciler{
			Client: c,
			Scheme: scheme,
		}
	}).For(fooKind).Watches(fooKind, tracecheck.EnqueueRequestForObject())
	setExplorePermuteConfig(eb, map[tracecheck.ReconcilerID]bool{
		"FooController": true,
		"BarController": true,
	})

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
		t.Fatalf("build explorer: %v", err)
	}

	result := explorer.Explore(context.Background(), initialState)
	return explorer, result
}

func TestConvergedStateIdentification(t *testing.T) {
	eb := tracecheck.NewExplorerBuilder(scheme)
	eb.WithMaxDepth(10)
	eb.WithoutOptimizations() // Disable optimizations to test exhaustive exploration

	// Testing two controllers whose behavior is identical
	// and who both depend on the same object.
	eb.WithReconciler("FooController", func(c ctrlclient.Client) tracecheck.Reconciler {
		return &controller.FooReconciler{
			Client: c,
			Scheme: scheme,
		}
	}).For("webapp.discrete.events/Foo")

	eb.WithReconciler("BarController", func(c ctrlclient.Client) tracecheck.Reconciler {
		return &controller.BarReconciler{
			Client: c,
			Scheme: scheme,
		}
	}).For("webapp.discrete.events/Foo")
	setExplorePermuteConfig(eb, map[tracecheck.ReconcilerID]bool{
		"FooController": true,
		"BarController": true,
	})

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
	explorer, err := eb.Build()
	if err != nil {
		t.Fatal(err)
	}

	result := explorer.Explore(context.Background(), initialState)
	assert.Equal(t, 2, len(result.ConvergedStates))

	// Trailing no-ops may appear in any order; we preserve the natural execution order
	// rather than sorting. The deduplication considers different no-op orderings as equivalent.
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
				{"FooController@1", "FooController@1", "BarController@1", "FooController@0", "BarController@0"},
				{"FooController@1", "FooController@1", "FooController@1", "BarController@0", "FooController@0"},
			},
		},
		{
			status:        "B-Final",
			hasAnnotation: true,
			numPaths:      2,
			pathSummaries: [][]string{
				{"FooController@1", "BarController@1", "FooController@1", "BarController@0", "FooController@1", "BarController@1", "FooController@0", "BarController@0"},
				{"FooController@1", "BarController@1", "FooController@1", "BarController@0", "FooController@1", "FooController@1", "BarController@0", "FooController@0"},
			},
		},
	}

	for _, expectedState := range expected {
		var matchedState *tracecheck.ResultState
		for _, convergedState := range result.ConvergedStates {
			status, hasAnnotation, ok := summarizeState(convergedState, explorer.VersionManager())
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

// Ensures that enabling optimizations on the Foo/Bar scenario reduces exploration work
// while preserving convergence.

func BenchmarkExhaustiveInterleavingsExplore(b *testing.B) {
	ctrl.SetLogger(zap.New(
		zap.UseDevMode(false),
		zap.WriteTo(io.Discard),
		zap.Level(gozap.NewAtomicLevelAt(zapcore.ErrorLevel)),
	))
	tracecheck.SetLogger(logr.Discard())

	eb := tracecheck.NewExplorerBuilder(scheme)
	eb.WithMaxDepth(10)
	eb.WithEmitter(event.NewInMemoryEmitter())
	fooKind := "webapp.discrete.events/Foo"
	eb.WithReconciler("FooController", func(c ctrlclient.Client) tracecheck.Reconciler {
		return &controller.TestReconciler{
			Client: c,
			Scheme: scheme,
		}
	}).For(fooKind).Watches(fooKind, tracecheck.EnqueueRequestForObject())
	eb.WithReconciler("BarController", func(c ctrlclient.Client) tracecheck.Reconciler {
		return &controller.TestReconciler{
			Client: c,
			Scheme: scheme,
		}
	}).For(fooKind).Watches(fooKind, tracecheck.EnqueueRequestForObject())
	setExplorePermuteConfig(eb, map[tracecheck.ReconcilerID]bool{
		"FooController": true,
		"BarController": true,
	})

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
		b.Fatalf("build explorer: %v", err)
	}

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	// Silence stdout for the benchmark run to keep output clean.
	devnull, _ := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	stdout := os.Stdout
	stderr := os.Stderr
	os.Stdout = devnull
	os.Stderr = devnull
	defer func() {
		_ = devnull.Close()
		os.Stdout = stdout
		os.Stderr = stderr
	}()
	for i := 0; i < b.N; i++ {
		state := initialState.Clone()
		result := explorer.Explore(ctx, state)
		if len(result.ConvergedStates) == 0 {
			b.Fatal("no converged states")
		}
	}
}
