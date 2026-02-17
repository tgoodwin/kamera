package explore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tgoodwin/kamera/pkg/analysis"
	foov1 "github.com/tgoodwin/kamera/pkg/test/integration/api/v1"
	"github.com/tgoodwin/kamera/pkg/test/integration/controller"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func newTestBuilder(t *testing.T) (*tracecheck.ExplorerBuilder, tracecheck.StateNode) {
	t.Helper()

	scheme := runtime.NewScheme()
	utilruntime.Must(foov1.AddToScheme(scheme))

	builder := tracecheck.NewExplorerBuilder(scheme)
	fooKind := "webapp.discrete.events/Foo"
	builder.WithReconciler("FooController", func(c ctrlclient.Client) tracecheck.Reconciler {
		return &controller.TestReconciler{Client: c, Scheme: scheme}
	}).For(fooKind).Watches(fooKind, tracecheck.EnqueueRequestForObject()).Done()
	builder.WithResourceDep(fooKind, "FooController")

	foo := &foov1.Foo{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "example",
		},
	}
	state := builder.GetStartStateFromObject(foo, "FooController")
	return builder, state
}

func TestParallelRunnerDoesNotLeakConfig(t *testing.T) {
	ctx := context.Background()
	builder, state := newTestBuilder(t)

	runner, err := NewParallelRunner(builder)
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	scenarios := []Scenario{
		{
			Name:         "max-depth-low",
			InitialState: state.Clone(),
			Config:       tracecheck.ExploreConfig{MaxDepth: 1},
		},
		{
			Name:         "max-depth-normal",
			InitialState: state.Clone(),
			Config:       tracecheck.ExploreConfig{MaxDepth: 5},
		},
	}

	results, err := runner.RunAll(ctx, scenarios, ParallelOptions{MaxParallel: 1})
	if err != nil {
		t.Fatalf("run all: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].Err != nil {
		t.Fatalf("scenario 0 error: %v", results[0].Err)
	}
	if results[1].Err != nil {
		t.Fatalf("scenario 1 error: %v", results[1].Err)
	}
	if results[0].Result == nil || results[1].Result == nil {
		t.Fatalf("expected results to be populated")
	}
	if len(results[0].Result.AbortedStates) == 0 {
		t.Fatalf("expected max-depth-low to abort")
	}
	if len(results[1].Result.AbortedStates) != 0 {
		t.Fatalf("expected max-depth-normal to converge without aborting")
	}
}

func TestParallelRunnerWritesDump(t *testing.T) {
	ctx := context.Background()
	builder, state := newTestBuilder(t)

	runner, err := NewParallelRunner(builder)
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	dumpDir := t.TempDir()

	scenarios := []Scenario{
		{
			Name:         "Foo Scenario",
			InitialState: state.Clone(),
			Config:       tracecheck.ExploreConfig{MaxDepth: 5},
			Context: ScenarioContext{
				Workflow: "smoke-workflow",
				InputRef: "inputs.json#foo-scenario",
				Attributes: map[string]string{
					"seed": "1234",
				},
			},
		},
	}

	results, err := runner.RunAll(ctx, scenarios, ParallelOptions{DumpDir: dumpDir})
	if err != nil {
		t.Fatalf("run all: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	entries, err := os.ReadDir(dumpDir)
	if err != nil {
		t.Fatalf("read dump dir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 dump file, got %d", len(entries))
	}
	name := entries[0].Name()
	if strings.Contains(name, " ") {
		t.Fatalf("expected sanitized dump filename, got %q", name)
	}
	if filepath.Ext(name) != ".jsonl" {
		t.Fatalf("expected .jsonl dump, got %q", name)
	}
	if _, err := os.Stat(filepath.Join(dumpDir, name)); err != nil {
		t.Fatalf("stat dump file: %v", err)
	}

	dumpPath := filepath.Join(dumpDir, name)
	dump, err := analysis.LoadDump(dumpPath)
	if err != nil {
		t.Fatalf("load dump: %v", err)
	}
	if dump.Context == nil || dump.Context.Scenario == nil {
		t.Fatalf("expected dump context scenario metadata to be present")
	}
	if dump.Context.Scenario.Name != "Foo Scenario" {
		t.Fatalf("expected scenario name in dump context, got %q", dump.Context.Scenario.Name)
	}
	if dump.Context.Scenario.RunIndex == nil || *dump.Context.Scenario.RunIndex != 0 {
		t.Fatalf("expected run index 0 in dump context")
	}
	if dump.Context.Scenario.Workflow != "smoke-workflow" {
		t.Fatalf("expected workflow in dump context, got %q", dump.Context.Scenario.Workflow)
	}
	if dump.Context.Scenario.InputRef != "inputs.json#foo-scenario" {
		t.Fatalf("expected input ref in dump context, got %q", dump.Context.Scenario.InputRef)
	}
	if dump.Context.Scenario.Attributes["seed"] != "1234" {
		t.Fatalf("expected seed attribute in dump context")
	}
}

func TestParallelRunnerCapturesInvariantError(t *testing.T) {
	ctx := context.Background()
	builder, state := newTestBuilder(t)

	runner, err := NewParallelRunner(builder)
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	scenarios := []Scenario{
		{
			Name:         "invariant-fails",
			InitialState: state.Clone(),
			Config:       tracecheck.ExploreConfig{MaxDepth: 5},
			Invariant: func(tracecheck.StateNode) error {
				return errors.New("invariant failed")
			},
		},
	}

	results, err := runner.RunAll(ctx, scenarios, ParallelOptions{})
	if err != nil {
		t.Fatalf("run all: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].InvariantError == nil {
		t.Fatalf("expected invariant error to be captured")
	}
}
