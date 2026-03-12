package explore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tgoodwin/kamera/pkg/analysis"
	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/snapshot"
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
	withPerturbFlag(t, false)

	runner, err := NewParallelRunner(builder)
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	scenarios := []Scenario{
		{
			Name:             "max-depth-low",
			EnvironmentState: state.Clone(),
			Config:           tracecheck.ExploreConfig{MaxDepth: 1},
		},
		{
			Name:             "max-depth-normal",
			EnvironmentState: state.Clone(),
			Config:           tracecheck.ExploreConfig{MaxDepth: 5},
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
	withPerturbFlag(t, false)

	runner, err := NewParallelRunner(builder)
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	dumpDir := t.TempDir()

	scenarios := []Scenario{
		{
			Name:             "Foo Scenario",
			EnvironmentState: state.Clone(),
			Config:           tracecheck.ExploreConfig{MaxDepth: 5},
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

func TestParallelRunnerEmbedsStatsInDumpWhenPerfStatsEnabled(t *testing.T) {
	ctx := context.Background()
	builder, state := newTestBuilder(t)

	runner, err := NewParallelRunner(builder)
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	dumpDir := t.TempDir()

	scenarios := []Scenario{
		{
			Name:             "with-perf-stats",
			EnvironmentState: state.Clone(),
			Config: tracecheck.ExploreConfig{
				MaxDepth:        5,
				RecordPerfStats: true,
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
	if results[0].DumpPath == "" {
		t.Fatalf("expected dump path")
	}

	dump, err := analysis.LoadDump(results[0].DumpPath)
	if err != nil {
		t.Fatalf("load dump: %v", err)
	}
	if dump.Stats == nil {
		t.Fatalf("expected stats embedded in dump")
	}
	if dump.Stats.TotalNodeVisits == 0 {
		t.Fatalf("expected embedded stats to include node visits")
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
			Name:             "invariant-fails",
			EnvironmentState: state.Clone(),
			Config:           tracecheck.ExploreConfig{MaxDepth: 5},
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
			Name:             "closed-loop",
			EnvironmentState: state.Clone(),
			Config:           tracecheck.ExploreConfig{MaxDepth: 1},
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

func TestParallelRunnerClosedLoopDisablesRerunWhenPerturbDisabled(t *testing.T) {
	ctx := context.Background()
	builder, state := newTestBuilder(t)

	runner, err := NewParallelRunner(builder)
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	withPerturbFlag(t, false)
	plannerCalls := 0
	scenarios := []Scenario{
		{
			Name:             "closed-loop-disabled",
			EnvironmentState: state.Clone(),
			Config:           tracecheck.ExploreConfig{MaxDepth: 1},
			ClosedLoop: &ClosedLoopSpec{
				Plan: func(reference ScenarioPhaseResult) ([]ScenarioPhasePlan, error) {
					plannerCalls++
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
	if plannerCalls != 0 {
		t.Fatalf("expected planner to be skipped when perturb is disabled, got %d calls", plannerCalls)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if len(results[0].Phases) != 1 {
		t.Fatalf("expected only one phase when perturb disabled, got %d", len(results[0].Phases))
	}
	if results[0].Phases[0].Name != "run" {
		t.Fatalf("expected single phase name to be run, got %q", results[0].Phases[0].Name)
	}
}

func TestParallelRunnerAutoClosedLoopRunsReferenceThenRerunWhenScenarioHasNoPlanner(t *testing.T) {
	ctx := context.Background()
	builder, state := newTestBuilder(t)

	runner, err := NewParallelRunner(builder)
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	scenarios := []Scenario{
		{
			Name:             "auto-closed-loop",
			EnvironmentState: state.Clone(),
			Config:           tracecheck.ExploreConfig{MaxDepth: 1},
		},
	}

	results, err := runner.RunAll(ctx, scenarios, ParallelOptions{MaxParallel: 1})
	if err != nil {
		t.Fatalf("run all: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if len(results[0].Phases) < 2 {
		t.Fatalf("expected at least 2 phase results (reference + rerun), got %d", len(results[0].Phases))
	}
	if results[0].Phases[0].Name != "reference" {
		t.Fatalf("expected first phase to be reference, got %q", results[0].Phases[0].Name)
	}
	if results[0].Phases[1].Name != "rerun" {
		t.Fatalf("expected second phase to be rerun, got %q", results[0].Phases[1].Name)
	}
}

func TestBuildUserActionInterleavingPlans_StrictlyBetweenDepths(t *testing.T) {
	path := tracecheck.ExecutionHistory{
		{
			ControllerID: tracecheck.UserControllerID,
			StepMetadata: map[string]string{
				tracecheck.UserActionIndexMetadataKey: "0",
			},
		},
		{ControllerID: "FooController"},
		{ControllerID: "FooController"},
		{ControllerID: "FooController"},
		{ControllerID: "FooController"},
		{
			ControllerID: tracecheck.UserControllerID,
			StepMetadata: map[string]string{
				tracecheck.UserActionIndexMetadataKey: "1",
			},
		},
	}
	reference := ScenarioPhaseResult{
		Result: &tracecheck.Result{
			ConvergedStates: []tracecheck.ResultState{
				{
					Paths: []tracecheck.ExecutionHistory{path},
				},
			},
		},
	}

	userInputs := []tracecheck.UserAction{
		{ID: "action-0"},
		{ID: "action-1"},
	}
	plans := buildUserActionInterleavingPlans(reference, tracecheck.ExploreConfig{}, ScenarioContext{}, userInputs)
	if len(plans) != 4 {
		t.Fatalf("expected 4 plans for depths 1..4, got %d", len(plans))
	}

	for i, expectedDepth := range []int{1, 2, 3, 4} {
		plan := plans[i]
		expectedName := fmt.Sprintf("interleave_action_1_depth_%d", expectedDepth)
		if plan.Name != expectedName {
			t.Fatalf("expected plan name %q, got %q", expectedName, plan.Name)
		}
		gotReadyDepth, ok := plan.Config.Perturbations.UserActionReadyDepths[1]
		if !ok {
			t.Fatalf("expected action ready depth for action 1")
		}
		if gotReadyDepth != expectedDepth {
			t.Fatalf("expected ready depth %d, got %d", expectedDepth, gotReadyDepth)
		}
	}
}

func TestBuildUserActionInterleavingPlans_EmptyWindowFallsBackToReferenceDepth(t *testing.T) {
	path := tracecheck.ExecutionHistory{
		{
			ControllerID: tracecheck.UserControllerID,
			StepMetadata: map[string]string{
				tracecheck.UserActionIndexMetadataKey: "0",
			},
		},
		{
			ControllerID: tracecheck.UserControllerID,
			StepMetadata: map[string]string{
				tracecheck.UserActionIndexMetadataKey: "1",
			},
		},
	}
	reference := ScenarioPhaseResult{
		Result: &tracecheck.Result{
			ConvergedStates: []tracecheck.ResultState{
				{
					Paths: []tracecheck.ExecutionHistory{path},
				},
			},
		},
	}

	userInputs := []tracecheck.UserAction{
		{ID: "action-0"},
		{ID: "action-1"},
	}
	plans := buildUserActionInterleavingPlans(reference, tracecheck.ExploreConfig{}, ScenarioContext{}, userInputs)
	if len(plans) != 1 {
		t.Fatalf("expected fallback to one plan, got %d", len(plans))
	}
	gotReadyDepth := plans[0].Config.Perturbations.UserActionReadyDepths[1]
	if gotReadyDepth != 1 {
		t.Fatalf("expected fallback ready depth 1, got %d", gotReadyDepth)
	}
}

func TestBuildUserActionInterleavingPlans_MissingSubsequentActionReturnsNoPlans(t *testing.T) {
	path := tracecheck.ExecutionHistory{
		{
			ControllerID: tracecheck.UserControllerID,
			StepMetadata: map[string]string{
				tracecheck.UserActionIndexMetadataKey: "0",
			},
		},
		{ControllerID: "FooController"},
	}
	reference := ScenarioPhaseResult{
		Result: &tracecheck.Result{
			ConvergedStates: []tracecheck.ResultState{
				{
					Paths: []tracecheck.ExecutionHistory{path},
				},
			},
		},
	}
	userInputs := []tracecheck.UserAction{
		{ID: "action-0"},
		{ID: "action-1"},
	}
	plans := buildUserActionInterleavingPlans(reference, tracecheck.ExploreConfig{}, ScenarioContext{}, userInputs)
	if len(plans) != 0 {
		t.Fatalf("expected no plans when subsequent action is missing, got %d", len(plans))
	}
}

func TestObservedReadKindsPerControllerCollectsFromObservations(t *testing.T) {
	reference := ScenarioPhaseResult{
		Result: &tracecheck.Result{
			ConvergedStates: []tracecheck.ResultState{
				{
					Paths: []tracecheck.ExecutionHistory{
						{
							{
								ControllerID: "ControllerA",
								Changes: tracecheck.Changes{
									Observations: []tracecheck.Effect{
										{Key: testCompositeKey("apps", "Deployment", "default", "web")},
										{Key: testCompositeKey("", "Service", "default", "web-svc")},
									},
								},
							},
							{
								ControllerID: "ControllerB",
								Changes: tracecheck.Changes{
									Observations: []tracecheck.Effect{
										{Key: testCompositeKey("", "Service", "default", "web-svc")},
										{Key: testCompositeKey("networking.k8s.io", "Ingress", "default", "web-ing")},
									},
								},
							},
							{
								ControllerID: tracecheck.UserControllerID,
								Changes: tracecheck.Changes{
									Observations: []tracecheck.Effect{
										{Key: testCompositeKey("", "ConfigMap", "default", "cfg")},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	readKinds := observedReadKindsPerController(reference)
	if len(readKinds) != 2 {
		t.Fatalf("expected 2 controllers (UserControllerID excluded), got %d", len(readKinds))
	}

	aKinds := readKinds["ControllerA"]
	if len(aKinds) != 2 || aKinds[0] != "apps/Deployment" || aKinds[1] != "core/Service" {
		t.Fatalf("expected ControllerA to read [apps/Deployment, core/Service], got %v", aKinds)
	}

	bKinds := readKinds["ControllerB"]
	if len(bKinds) != 2 || bKinds[0] != "core/Service" || bKinds[1] != "networking.k8s.io/Ingress" {
		t.Fatalf("expected ControllerB to read [core/Service, networking.k8s.io/Ingress], got %v", bKinds)
	}
}

func TestObservedReadKindsPerControllerReturnsNilOnNilResult(t *testing.T) {
	result := observedReadKindsPerController(ScenarioPhaseResult{})
	if result != nil {
		t.Fatalf("expected nil, got %v", result)
	}
}

func TestMaxKindFrontiers(t *testing.T) {
	path := tracecheck.ExecutionHistory{
		{
			ControllerID: "A",
			KindSeqAfter: tracecheck.KindSequences{"apps/Deployment": 1, "core/Service": 2},
		},
		{
			ControllerID: "B",
			KindSeqAfter: tracecheck.KindSequences{"apps/Deployment": 3, "core/Service": 2},
		},
		{
			ControllerID: "A",
			KindSeqAfter: tracecheck.KindSequences{"apps/Deployment": 3, "core/Service": 4},
		},
	}
	maxSeqs := maxKindFrontiers(path)
	if maxSeqs["apps/Deployment"] != 3 {
		t.Fatalf("expected maxSeq=3 for apps/Deployment, got %d", maxSeqs["apps/Deployment"])
	}
	if maxSeqs["core/Service"] != 4 {
		t.Fatalf("expected maxSeq=4 for core/Service, got %d", maxSeqs["core/Service"])
	}
}

func TestMaxKindFrontiersSkipsNilSteps(t *testing.T) {
	path := tracecheck.ExecutionHistory{
		nil,
		{ControllerID: "A", KindSeqAfter: tracecheck.KindSequences{"apps/Deployment": 2}},
	}
	maxSeqs := maxKindFrontiers(path)
	if maxSeqs["apps/Deployment"] != 2 {
		t.Fatalf("expected maxSeq=2, got %d", maxSeqs["apps/Deployment"])
	}
}

func TestBuildStalenessPerturbationPlansProducesIntervalPlans(t *testing.T) {
	// Reference: ControllerA reads apps/Deployment, frontier advances to 2.
	// Expected intervals for (ControllerA, apps/Deployment) with maxSeq=2:
	//   [0,1), [0,2), [1,2) → 3 plans
	reference := ScenarioPhaseResult{
		Result: &tracecheck.Result{
			ConvergedStates: []tracecheck.ResultState{
				{
					Paths: []tracecheck.ExecutionHistory{
						{
							{
								ControllerID: "ControllerA",
								Changes: tracecheck.Changes{
									Observations: []tracecheck.Effect{
										{Key: testCompositeKey("apps", "Deployment", "default", "web")},
									},
								},
								KindSeqAfter: tracecheck.KindSequences{"apps/Deployment": 1},
							},
							{
								ControllerID: "ControllerA",
								Changes: tracecheck.Changes{
									Observations: []tracecheck.Effect{
										{Key: testCompositeKey("apps", "Deployment", "default", "web")},
									},
								},
								KindSeqAfter: tracecheck.KindSequences{"apps/Deployment": 2},
							},
						},
					},
				},
			},
		},
	}

	base := tracecheck.ExploreConfig{MaxDepth: 20}
	ctx := ScenarioContext{Attributes: map[string]string{"input": "test"}}

	plans := buildStalenessPerturbationPlans(reference, base, ctx)
	if len(plans) != 3 {
		t.Fatalf("expected 3 interval plans, got %d", len(plans))
	}

	// Verify first plan uses StalenessIntervals, not old Staleness map.
	plan := plans[0]
	if plan.Name != "staleness_interval_0" {
		t.Fatalf("expected plan name 'staleness_interval_0', got %q", plan.Name)
	}
	if len(plan.Config.Perturbations.StalenessIntervals) != 1 {
		t.Fatalf("expected 1 staleness interval per plan, got %d", len(plan.Config.Perturbations.StalenessIntervals))
	}
	if len(plan.Config.Perturbations.Staleness) != 0 {
		t.Fatalf("expected old Staleness map to be empty, got %d entries", len(plan.Config.Perturbations.Staleness))
	}

	// Verify interval values (sorted: staleAt=0,catchUpAt=1 first).
	iv := plan.Config.Perturbations.StalenessIntervals[0]
	if iv.ReconcilerID != "ControllerA" {
		t.Fatalf("expected ReconcilerID=ControllerA, got %s", iv.ReconcilerID)
	}
	if iv.Kind != "apps/Deployment" {
		t.Fatalf("expected Kind=apps/Deployment, got %s", iv.Kind)
	}
	if iv.StaleAt != 0 || iv.CatchUpAt != 1 {
		t.Fatalf("expected first interval [0,1), got [%d,%d)", iv.StaleAt, iv.CatchUpAt)
	}
	if iv.Lag != -1 {
		t.Fatalf("expected Lag=-1 (frozen), got %d", iv.Lag)
	}

	// Verify ordering permutation is enabled for observed controllers.
	if !plan.Config.Perturbations.PermuteOrder["ControllerA"] {
		t.Fatalf("expected ordering permutation enabled for ControllerA")
	}

	// Verify context attributes.
	if plan.Context == nil {
		t.Fatalf("expected plan context")
	}
	if plan.Context.Attributes["perturbation.strategy"] != "staleness_interval" {
		t.Fatalf("expected perturbation.strategy=staleness_interval, got %q", plan.Context.Attributes["perturbation.strategy"])
	}
	if plan.Context.Attributes["perturbation.reconciler"] != "ControllerA" {
		t.Fatalf("expected perturbation.reconciler=ControllerA")
	}

	// Verify remaining intervals cover [0,2) and [1,2).
	iv1 := plans[1].Config.Perturbations.StalenessIntervals[0]
	if iv1.StaleAt != 0 || iv1.CatchUpAt != 2 {
		t.Fatalf("expected second interval [0,2), got [%d,%d)", iv1.StaleAt, iv1.CatchUpAt)
	}
	iv2 := plans[2].Config.Perturbations.StalenessIntervals[0]
	if iv2.StaleAt != 1 || iv2.CatchUpAt != 2 {
		t.Fatalf("expected third interval [1,2), got [%d,%d)", iv2.StaleAt, iv2.CatchUpAt)
	}
}

func TestBuildStalenessPerturbationPlansNoIntervalsWhenMaxSeqZero(t *testing.T) {
	// Kind frontier never advances past 0 → no intervals possible.
	reference := ScenarioPhaseResult{
		Result: &tracecheck.Result{
			ConvergedStates: []tracecheck.ResultState{
				{
					Paths: []tracecheck.ExecutionHistory{
						{
							{
								ControllerID: "ControllerA",
								Changes: tracecheck.Changes{
									Observations: []tracecheck.Effect{
										{Key: testCompositeKey("apps", "Deployment", "default", "web")},
									},
								},
								KindSeqAfter: tracecheck.KindSequences{"apps/Deployment": 0},
							},
						},
					},
				},
			},
		},
	}
	plans := buildStalenessPerturbationPlans(reference, tracecheck.ExploreConfig{MaxDepth: 10}, ScenarioContext{})
	if len(plans) != 0 {
		t.Fatalf("expected no plans when maxSeq=0, got %d", len(plans))
	}
}

func TestBuildStalenessPerturbationPlansReturnsNilOnNilResult(t *testing.T) {
	plans := buildStalenessPerturbationPlans(
		ScenarioPhaseResult{},
		tracecheck.ExploreConfig{MaxDepth: 10},
		ScenarioContext{},
	)
	if len(plans) != 0 {
		t.Fatalf("expected no plans for nil result, got %d", len(plans))
	}
}

func testCompositeKey(group, kind, namespace, name string) snapshot.CompositeKey {
	return snapshot.CompositeKey{
		IdentityKey: snapshot.IdentityKey{
			Group: group,
			Kind:  kind,
		},
		ResourceKey: snapshot.ResourceKey{
			Group:     group,
			Kind:      kind,
			Namespace: namespace,
			Name:      name,
		},
	}
}

func TestParallelRunnerAutoClosedLoopUsesInterleavingForMultiStepWorkflows(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	utilruntime.Must(foov1.AddToScheme(scheme))

	builder := tracecheck.NewExplorerBuilder(scheme)
	builder.WithMaxDepth(10)
	state, err := builder.BuildStartStateFromObjects([]ctrlclient.Object{
		&foov1.Foo{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "seed",
			},
		},
	}, nil)
	if err != nil {
		t.Fatalf("build start state: %v", err)
	}

	runner, err := NewParallelRunner(builder)
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	scenarios := []Scenario{
		{
			Name:             "auto-multistep-interleaving",
			EnvironmentState: state.Clone(),
			Config:           tracecheck.ExploreConfig{MaxDepth: 10},
			UserInputs: []tracecheck.UserAction{
				{
					ID:     "create-target",
					OpType: "CREATE",
					Payload: &foov1.Foo{
						ObjectMeta: metav1.ObjectMeta{
							Namespace: "default",
							Name:      "target",
						},
					},
				},
				{
					ID:     "update-target",
					OpType: "UPDATE",
					Payload: &foov1.Foo{
						ObjectMeta: metav1.ObjectMeta{
							Namespace: "default",
							Name:      "target",
						},
					},
				},
			},
		},
	}

	results, err := runner.RunAll(ctx, scenarios, ParallelOptions{MaxParallel: 1})
	if err != nil {
		t.Fatalf("run all: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if len(results[0].Phases) < 2 {
		t.Fatalf("expected at least reference + interleaving phase, got %d", len(results[0].Phases))
	}
	if results[0].Phases[0].Name != "reference" {
		t.Fatalf("expected first phase to be reference, got %q", results[0].Phases[0].Name)
	}
	if !strings.HasPrefix(results[0].Phases[1].Name, "interleave_action_1_depth_") {
		t.Fatalf("expected second phase name to use interleaving strategy, got %q", results[0].Phases[1].Name)
	}
	if results[0].Phases[1].Name == "rerun" {
		t.Fatalf("expected broad rerun to be replaced for multi-step workflows")
	}
}

func withPerturbFlag(t *testing.T, enabled bool) {
	t.Helper()

	oldValue := *perturbFlag
	*perturbFlag = enabled
	t.Cleanup(func() {
		*perturbFlag = oldValue
	})
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
			Name:             "closed-loop-dump",
			EnvironmentState: state.Clone(),
			Config:           tracecheck.ExploreConfig{MaxDepth: 1},
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
	invocationIDs := make(map[string]struct{})
	for _, phase := range results[0].Phases {
		if strings.TrimSpace(phase.DumpPath) == "" {
			t.Fatalf("expected dump path for phase %q", phase.Name)
		}
		if _, err := os.Stat(phase.DumpPath); err != nil {
			t.Fatalf("expected dump file for phase %q: %v", phase.Name, err)
		}
		dump, err := analysis.LoadDump(phase.DumpPath)
		if err != nil {
			t.Fatalf("load dump for phase %q: %v", phase.Name, err)
		}
		if dump.Context == nil || dump.Context.Scenario == nil {
			t.Fatalf("expected context for phase %q dump", phase.Name)
		}
		if dump.Context.Scenario.Attributes == nil {
			t.Fatalf("expected attributes for phase %q dump", phase.Name)
		}
		invocationID := strings.TrimSpace(dump.Context.Scenario.Attributes["invocation_id"])
		if invocationID == "" {
			t.Fatalf("expected non-empty invocation_id for phase %q dump", phase.Name)
		}
		invocationIDs[invocationID] = struct{}{}
		if dump.CampaignMetrics == nil {
			t.Fatalf("expected campaignMetrics for phase %q dump", phase.Name)
		}
		if dump.CampaignMetrics.TotalNodeVisits <= 0 {
			t.Fatalf("expected totalNodeVisits > 0 for phase %q dump", phase.Name)
		}
		if dump.CampaignMetrics.UniqueNodeVisits <= 0 {
			t.Fatalf("expected uniqueNodeVisits > 0 for phase %q dump", phase.Name)
		}
		if dump.CampaignMetrics.UniqueResourceStates <= 0 {
			t.Fatalf("expected uniqueResourceStates > 0 for phase %q dump", phase.Name)
		}
		if dump.CampaignMetrics.DurationNS <= 0 {
			t.Fatalf("expected durationNs > 0 for phase %q dump", phase.Name)
		}
	}
	if len(invocationIDs) != 1 {
		t.Fatalf("expected all phase dumps to share one invocation_id, got %d unique ids", len(invocationIDs))
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
			Name:             "closed-loop-prefix-hashes",
			EnvironmentState: state.Clone(),
			Config:           tracecheck.ExploreConfig{MaxDepth: 5},
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
			Name:             "x",
			EnvironmentState: state.Clone(),
			Config:           tracecheck.ExploreConfig{MaxDepth: 3},
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
			Name:             "alpha/base",
			EnvironmentState: state.Clone(),
			Config:           tracecheck.ExploreConfig{MaxDepth: 3},
		},
		{
			Name:             "alpha/single/foo",
			EnvironmentState: state.Clone(),
			Config:           tracecheck.ExploreConfig{MaxDepth: 3},
		},
		{
			Name:             "beta",
			EnvironmentState: state.Clone(),
			Config:           tracecheck.ExploreConfig{MaxDepth: 3},
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
	invocationIDs := map[string]struct{}{}
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
		seen = append(seen, req.JobIndex)
		invocationIDs[strings.TrimSpace(req.InvocationID)] = struct{}{}
		mu.Unlock()
		if req.JobIndex == 1 {
			return childProcessResult{
				JobIndex:   req.JobIndex,
				InputIndex: req.InputIndex,
				TrialIndex: req.TrialIndex,
				Err:        errors.New("exit status 1"),
				Stderr:     "boom",
			}
		}
		return childProcessResult{
			JobIndex:   req.JobIndex,
			InputIndex: req.InputIndex,
			TrialIndex: req.TrialIndex,
		}
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
		t.Fatalf("expected job indices [0 1 2], got %v", seen)
	}
	if len(invocationIDs) != 1 {
		t.Fatalf("expected one invocation id for all child jobs, got %d", len(invocationIDs))
	}
	if _, ok := invocationIDs[""]; ok {
		t.Fatalf("expected non-empty invocation id for child jobs")
	}
	if runErr == nil || !strings.Contains(runErr.Error(), "failed job indices: 1") {
		t.Fatalf("expected aggregate failure with index 1, got %v", runErr)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	if results[1].Err == nil {
		t.Fatalf("expected result error for child index 1")
	}
}

func TestBuildProcessJobsExpandsMonteCarloTrials(t *testing.T) {
	inputs := []coverage.Input{
		{Name: "alpha"},
		{Name: "beta"},
	}
	scenarios := []Scenario{
		{
			Name:   "alpha",
			Config: tracecheck.ExploreConfig{SearchMode: tracecheck.SearchModeMonteCarlo, MonteCarlo: tracecheck.MonteCarloConfig{Trials: 3}},
		},
		{
			Name:   "beta",
			Config: tracecheck.ExploreConfig{SearchMode: tracecheck.SearchModeDFS},
		},
	}

	jobs, err := buildProcessJobs(inputs, scenarios, "/tmp/inputs.json")
	if err != nil {
		t.Fatalf("buildProcessJobs() error = %v", err)
	}
	if len(jobs) != 4 {
		t.Fatalf("expected 4 jobs (3 monte carlo + 1 dfs), got %d", len(jobs))
	}

	expect := []struct {
		input int
		trial int
	}{
		{input: 0, trial: 0},
		{input: 0, trial: 1},
		{input: 0, trial: 2},
		{input: 1, trial: 0},
	}
	for i, want := range expect {
		if jobs[i].InputIndex != want.input {
			t.Fatalf("job %d expected input index %d, got %d", i, want.input, jobs[i].InputIndex)
		}
		if jobs[i].TrialIndex != want.trial {
			t.Fatalf("job %d expected trial index %d, got %d", i, want.trial, jobs[i].TrialIndex)
		}
		if jobs[i].JobIndex != i {
			t.Fatalf("job %d expected job index %d, got %d", i, i, jobs[i].JobIndex)
		}
	}
}

func TestParallelRunnerInProcessExpandsMonteCarloTrials(t *testing.T) {
	ctx := context.Background()
	builder, state := newTestBuilder(t)
	withPerturbFlag(t, false)

	runner, err := NewParallelRunner(builder)
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	scenarios := []Scenario{
		{
			Name:             "alpha",
			EnvironmentState: state.Clone(),
			Config: tracecheck.ExploreConfig{
				MaxDepth:   5,
				SearchMode: tracecheck.SearchModeMonteCarlo,
				MonteCarlo: tracecheck.MonteCarloConfig{
					Seed:   1337,
					Trials: 3,
				},
			},
		},
	}

	results, err := runner.RunAll(ctx, scenarios, ParallelOptions{MaxParallel: 1})
	if err != nil {
		t.Fatalf("run all: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected one result per Monte Carlo trial, got %d", len(results))
	}
}

func TestParallelRunnerChildModeAnnotatesMonteCarloTrialMetadata(t *testing.T) {
	ctx := context.Background()
	builder, state := newTestBuilder(t)
	withPerturbFlag(t, false)

	runner, err := NewParallelRunner(builder)
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	inputsPath := writeInputNamesFile(t, "alpha")
	withProcessModeFlags(t, true, 0, inputsPath)
	withProcessTrialFlags(t, 2, 42)

	dumpDir := t.TempDir()
	scenarios := []Scenario{
		{
			Name:             "alpha",
			EnvironmentState: state.Clone(),
			Config: tracecheck.ExploreConfig{
				MaxDepth:   5,
				SearchMode: tracecheck.SearchModeMonteCarlo,
				MonteCarlo: tracecheck.MonteCarloConfig{
					Seed:   1337,
					Trials: 5,
				},
			},
			Context: ScenarioContext{
				Workflow: "parallel-process-child",
				InputRef: inputsPath + "#alpha",
			},
		},
	}

	results, err := runner.RunAll(ctx, scenarios, ParallelOptions{DumpDir: dumpDir})
	if err != nil {
		t.Fatalf("run all: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected one child result, got %d", len(results))
	}
	if results[0].DumpPath == "" {
		t.Fatalf("expected child dump path")
	}

	dump, err := analysis.LoadDump(results[0].DumpPath)
	if err != nil {
		t.Fatalf("load dump: %v", err)
	}
	if dump.Context == nil || dump.Context.Scenario == nil {
		t.Fatalf("expected dump context metadata")
	}
	if dump.Context.Scenario.RunIndex == nil || *dump.Context.Scenario.RunIndex != 42 {
		t.Fatalf("expected child run index to use job index 42")
	}

	attrs := dump.Context.Scenario.Attributes
	if attrs["search_mode"] != string(tracecheck.SearchModeMonteCarlo) {
		t.Fatalf("expected search_mode=monte_carlo")
	}
	if attrs["mc_trial_index"] != "2" {
		t.Fatalf("expected mc_trial_index=2, got %q", attrs["mc_trial_index"])
	}
	if attrs["mc_trial_count"] != "5" {
		t.Fatalf("expected mc_trial_count=5, got %q", attrs["mc_trial_count"])
	}
	if attrs["mc_role"] != "trial" {
		t.Fatalf("expected mc_role=trial, got %q", attrs["mc_role"])
	}
	group := attrs["mc_group_id"]
	if strings.TrimSpace(group) == "" {
		t.Fatalf("expected mc_group_id to be populated")
	}
	wantSeed := tracecheck.DeriveMonteCarloSeed(1337, group, 2)
	if attrs["mc_seed"] != strconv.FormatInt(wantSeed, 10) {
		t.Fatalf("expected mc_seed=%d, got %q", wantSeed, attrs["mc_seed"])
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

func withProcessTrialFlags(t *testing.T, trialIdx int, jobIdx int) {
	t.Helper()

	oldTrialIdx := *parallelChildTrialIndexFlag
	oldJobIdx := *parallelChildJobIndexFlag
	t.Cleanup(func() {
		*parallelChildTrialIndexFlag = oldTrialIdx
		*parallelChildJobIndexFlag = oldJobIdx
	})

	*parallelChildTrialIndexFlag = trialIdx
	*parallelChildJobIndexFlag = jobIdx
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
