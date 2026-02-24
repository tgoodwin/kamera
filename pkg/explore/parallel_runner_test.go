package explore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tgoodwin/kamera/pkg/analysis"
	"github.com/tgoodwin/kamera/pkg/coverage"
	foov1 "github.com/tgoodwin/kamera/pkg/test/integration/api/v1"
	"github.com/tgoodwin/kamera/pkg/test/integration/controller"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
			EnvironmentState: state.Clone(),
			Config:       tracecheck.ExploreConfig{MaxDepth: 1},
		},
		{
			Name:         "max-depth-normal",
			EnvironmentState: state.Clone(),
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
			EnvironmentState: state.Clone(),
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
			EnvironmentState: state.Clone(),
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

func TestParallelRunnerClosedLoopRunsReferenceThenRerunPerScenario(t *testing.T) {
	ctx := context.Background()
	builder, state := newTestBuilder(t)

	runner, err := NewParallelRunner(builder)
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	plannerCalls := 0
	scenarios := []Scenario{
		{
			Name:         "closed-loop",
			EnvironmentState: state.Clone(),
			Config:       tracecheck.ExploreConfig{MaxDepth: 1},
			ClosedLoop: &ClosedLoopSpec{
				Plan: func(reference ScenarioPhaseResult) ([]ScenarioPhasePlan, error) {
					plannerCalls++
					if reference.Name != "reference" {
						t.Fatalf("expected reference phase, got %q", reference.Name)
					}
					if reference.Result == nil || len(reference.Result.AbortedStates) == 0 {
						t.Fatalf("expected reference phase to abort due to low depth")
					}
					return []ScenarioPhasePlan{
						{Name: "rerun", Config: tracecheck.ExploreConfig{MaxDepth: 5}},
					}, nil
				},
			},
		},
	}

	results, err := runner.RunAll(ctx, scenarios, ParallelOptions{MaxParallel: 1})
	if err != nil {
		t.Fatalf("run all: %v", err)
	}
	if plannerCalls != 1 {
		t.Fatalf("expected planner to be called once, got %d", plannerCalls)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if len(results[0].Phases) != 2 {
		t.Fatalf("expected 2 phase results, got %d", len(results[0].Phases))
	}
	if results[0].Phases[0].Name != "reference" {
		t.Fatalf("expected first phase to be reference, got %q", results[0].Phases[0].Name)
	}
	if results[0].Phases[1].Name != "rerun" {
		t.Fatalf("expected second phase to be rerun, got %q", results[0].Phases[1].Name)
	}
	if results[0].Phases[1].Result == nil || len(results[0].Phases[1].Result.AbortedStates) != 0 {
		t.Fatalf("expected rerun phase to converge without aborting")
	}
}

func TestParallelRunnerClosedLoopWritesPhaseDumps(t *testing.T) {
	ctx := context.Background()
	builder, state := newTestBuilder(t)

	runner, err := NewParallelRunner(builder)
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	dumpDir := t.TempDir()
	scenarios := []Scenario{
		{
			Name:         "closed-loop-dump",
			EnvironmentState: state.Clone(),
			Config:       tracecheck.ExploreConfig{MaxDepth: 1},
			ClosedLoop: &ClosedLoopSpec{
				Plan: func(reference ScenarioPhaseResult) ([]ScenarioPhasePlan, error) {
					return []ScenarioPhasePlan{
						{Name: "rerun", Config: tracecheck.ExploreConfig{MaxDepth: 5}},
					}, nil
				},
			},
		},
	}

	results, err := runner.RunAll(ctx, scenarios, ParallelOptions{DumpDir: dumpDir, MaxParallel: 1})
	if err != nil {
		t.Fatalf("run all: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if len(results[0].Phases) != 2 {
		t.Fatalf("expected 2 phase results, got %d", len(results[0].Phases))
	}
	for _, phase := range results[0].Phases {
		if strings.TrimSpace(phase.DumpPath) == "" {
			t.Fatalf("expected dump path for phase %q", phase.Name)
		}
		if _, err := os.Stat(phase.DumpPath); err != nil {
			t.Fatalf("expected dump file for phase %q: %v", phase.Name, err)
		}
	}
}

func TestParallelRunnerClosedLoopPrefixHistoryHashesAreDumpable(t *testing.T) {
	ctx := context.Background()
	builder, state := newTestBuilder(t)

	runner, err := NewParallelRunner(builder)
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	dumpDir := t.TempDir()
	scenarios := []Scenario{
		{
			Name:         "closed-loop-prefix-hashes",
			EnvironmentState: state.Clone(),
			Config:       tracecheck.ExploreConfig{MaxDepth: 5},
			ClosedLoop: &ClosedLoopSpec{
				Plan: func(reference ScenarioPhaseResult) ([]ScenarioPhasePlan, error) {
					if reference.Result == nil || len(reference.Result.ConvergedStates) == 0 {
						t.Fatalf("expected converged reference result")
					}
					if reference.VersionManager == nil {
						t.Fatalf("expected reference version manager")
					}
					path := reference.Result.ConvergedStates[0].Paths[0]
					if len(path) == 0 {
						t.Fatalf("expected reference path entries")
					}
					checkpoint := path[len(path)-1]
					if checkpoint == nil || len(checkpoint.StateAfter) == 0 {
						t.Fatalf("expected checkpoint with stateAfter")
					}

					var prefixVersions tracecheck.ObjectVersions
					chooseOutsideCheckpoint := func(versions tracecheck.ObjectVersions) bool {
						for key, hash := range versions {
							if finalHash, ok := checkpoint.StateAfter[key]; !ok || finalHash != hash {
								prefixVersions = tracecheck.ObjectVersions{key: hash}
								return true
							}
						}
						return false
					}

					for _, step := range path {
						if step == nil {
							continue
						}
						if chooseOutsideCheckpoint(step.StateBefore) || chooseOutsideCheckpoint(step.Changes.ObjectVersions) || chooseOutsideCheckpoint(step.StateAfter) {
							break
						}
					}
					if len(prefixVersions) == 0 {
						t.Fatalf("expected at least one historical hash outside checkpoint state")
					}

					seed, err := tracecheck.BuildRestartSeedFromState(checkpoint.StateAfter, reference.VersionManager, checkpoint.PendingReconciles)
					if err != nil {
						t.Fatalf("build checkpoint seed: %v", err)
					}
					prefix := tracecheck.ExecutionHistory{
						{
							ControllerID: "prefix",
							StateBefore:  prefixVersions,
						},
					}
					return []ScenarioPhasePlan{
						{
							Name:   "rerun",
							Config: tracecheck.ExploreConfig{MaxDepth: 1},
							Seed:   &seed,
							Prefix: prefix,
						},
					}, nil
				},
			},
		},
	}

	results, err := runner.RunAll(ctx, scenarios, ParallelOptions{DumpDir: dumpDir, MaxParallel: 1})
	if err != nil {
		t.Fatalf("run all: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Err != nil {
		t.Fatalf("expected scenario success, got %v", results[0].Err)
	}
	if len(results[0].Phases) != 2 {
		t.Fatalf("expected reference+rerun phases, got %d", len(results[0].Phases))
	}
	for _, phase := range results[0].Phases {
		if strings.TrimSpace(phase.DumpPath) == "" {
			t.Fatalf("expected dump path for phase %q", phase.Name)
		}
		if _, err := os.Stat(phase.DumpPath); err != nil {
			t.Fatalf("expected dump file for phase %q: %v", phase.Name, err)
		}
	}
}

func TestParallelRunnerProcessModeRequiresInputsFile(t *testing.T) {
	withProcessModeFlags(t, true, -1, "")

	builder, state := newTestBuilder(t)
	runner, err := NewParallelRunner(builder)
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	scenarios := []Scenario{
		{
			Name:         "x",
			EnvironmentState: state.Clone(),
			Config:       tracecheck.ExploreConfig{MaxDepth: 3},
		},
	}

	_, err = runner.RunAll(context.Background(), scenarios, ParallelOptions{})
	if err == nil || !strings.Contains(err.Error(), "--inputs") {
		t.Fatalf("expected --inputs validation error, got %v", err)
	}
}

func TestParallelRunnerChildModeFailsWhenSelectedInputMapsToMultipleScenarios(t *testing.T) {
	builder, state := newTestBuilder(t)
	runner, err := NewParallelRunner(builder)
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	inputsPath := writeInputNamesFile(t, "alpha", "beta")
	withProcessModeFlags(t, true, 0, inputsPath)
	dumpDir := t.TempDir()

	scenarios := []Scenario{
		{
			Name:         "alpha/base",
			EnvironmentState: state.Clone(),
			Config:       tracecheck.ExploreConfig{MaxDepth: 3},
		},
		{
			Name:         "alpha/single/foo",
			EnvironmentState: state.Clone(),
			Config:       tracecheck.ExploreConfig{MaxDepth: 3},
		},
		{
			Name:         "beta",
			EnvironmentState: state.Clone(),
			Config:       tracecheck.ExploreConfig{MaxDepth: 3},
		},
	}

	results, err := runner.RunAll(context.Background(), scenarios, ParallelOptions{DumpDir: dumpDir})
	if err == nil || !strings.Contains(err.Error(), "expected exactly one scenario") {
		t.Fatalf("expected single-scenario contract error, got %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected one child result, got %d", len(results))
	}
	if results[0].Err == nil {
		t.Fatalf("expected child result error")
	}
	if results[0].DumpPath == "" {
		t.Fatalf("expected child failure dump path")
	}

	dump, loadErr := analysis.LoadDump(results[0].DumpPath)
	if loadErr != nil {
		t.Fatalf("load failure dump: %v", loadErr)
	}
	if dump.Context == nil || dump.Context.Scenario == nil {
		t.Fatalf("expected dump context scenario metadata")
	}
	attrs := dump.Context.Scenario.Attributes
	if attrs["status"] != "error" {
		t.Fatalf("expected status=error attribute")
	}
	if attrs["error_phase"] != "select_scenario" {
		t.Fatalf("expected select_scenario phase, got %q", attrs["error_phase"])
	}
	if attrs["error_message"] == "" {
		t.Fatalf("expected error_message attribute")
	}
}

func TestParallelRunnerSupervisorRunsAllChildrenAndAggregatesFailures(t *testing.T) {
	withProcessModeFlags(t, true, -1, "/tmp/inputs.json")

	builder, state := newTestBuilder(t)
	runner, err := NewParallelRunner(builder)
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	var mu sync.Mutex
	var seen []int
	var calls atomic.Int32

	runner.loadInputsFn = func(path string) ([]coverage.Input, error) {
		if path != "/tmp/inputs.json" {
			return nil, fmt.Errorf("unexpected path %q", path)
		}
		return []coverage.Input{
			{Name: "a"},
			{Name: "b"},
			{Name: "c"},
		}, nil
	}
	runner.parentArgsFn = func() []string {
		return []string{"harness", "--parallel-processes", "--inputs", "/tmp/inputs.json", "--interactive=false"}
	}
	runner.cwdFn = func() (string, error) {
		return t.TempDir(), nil
	}
	runner.checkModuleDirFn = func(string) error { return nil }
	runner.spawnChildFn = func(_ context.Context, req childProcessRequest) childProcessResult {
		calls.Add(1)
		mu.Lock()
		seen = append(seen, req.ChildIndex)
		mu.Unlock()
		if req.ChildIndex == 1 {
			return childProcessResult{
				ChildIndex: req.ChildIndex,
				Err:        errors.New("exit status 1"),
				Stderr:     "boom",
			}
		}
		return childProcessResult{ChildIndex: req.ChildIndex}
	}

	scenarios := []Scenario{
		{Name: "a", EnvironmentState: state.Clone(), Config: tracecheck.ExploreConfig{MaxDepth: 1}},
		{Name: "b", EnvironmentState: state.Clone(), Config: tracecheck.ExploreConfig{MaxDepth: 1}},
		{Name: "c", EnvironmentState: state.Clone(), Config: tracecheck.ExploreConfig{MaxDepth: 1}},
	}
	results, runErr := runner.RunAll(context.Background(), scenarios, ParallelOptions{MaxParallel: 2})

	if calls.Load() != 3 {
		t.Fatalf("expected 3 child runs, got %d", calls.Load())
	}
	slices.Sort(seen)
	if !slices.Equal(seen, []int{0, 1, 2}) {
		t.Fatalf("expected child indices [0 1 2], got %v", seen)
	}
	if runErr == nil || !strings.Contains(runErr.Error(), "failed child indices: 1") {
		t.Fatalf("expected aggregate failure with index 1, got %v", runErr)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	if results[1].Err == nil {
		t.Fatalf("expected result error for child index 1")
	}
}

func TestParallelRunnerChildModeForcesSingleProcessParallelism(t *testing.T) {
	opts := childParallelOptions(ParallelOptions{MaxParallel: 32})
	if opts.MaxParallel != 1 {
		t.Fatalf("expected child max parallel to be 1, got %d", opts.MaxParallel)
	}
}

func withProcessModeFlags(t *testing.T, enabled bool, childIdx int, inputsPath string) {
	t.Helper()

	oldParallelProcesses := *parallelProcessesFlag
	oldChildIndex := *parallelChildIndexFlag
	oldInputsPath := *inputsPathFlag
	t.Cleanup(func() {
		*parallelProcessesFlag = oldParallelProcesses
		*parallelChildIndexFlag = oldChildIndex
		*inputsPathFlag = oldInputsPath
	})

	*parallelProcessesFlag = enabled
	*parallelChildIndexFlag = childIdx
	*inputsPathFlag = inputsPath
}

func writeInputNamesFile(t *testing.T, names ...string) string {
	t.Helper()

	inputs := make([]coverage.Input, 0, len(names))
	for _, name := range names {
		inputs = append(inputs, coverage.Input{
			Name: name,
			EnvironmentState: coverage.EnvironmentState{
				Objects: []*unstructured.Unstructured{
					{
						Object: map[string]interface{}{
							"apiVersion": "v1",
							"kind":       "ConfigMap",
							"metadata": map[string]interface{}{
								"name":      name + "-cm",
								"namespace": "default",
							},
						},
					},
				},
			},
		})
	}

	path := filepath.Join(t.TempDir(), "inputs.json")
	data, err := json.Marshal(inputs)
	if err != nil {
		t.Fatalf("marshal inputs: %v", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write inputs: %v", err)
	}
	return path
}
