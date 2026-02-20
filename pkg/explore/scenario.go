package explore

import "github.com/tgoodwin/kamera/pkg/tracecheck"

type ScenarioContext struct {
	Workflow   string
	InputRef   string
	Attributes map[string]string
}

// Scenario is the unit produced by input generators and consumed by runners.
type Scenario struct {
	Name         string
	InitialState tracecheck.StateNode
	Config       tracecheck.ExploreConfig
	Invariant    func(tracecheck.StateNode) error
	Context      ScenarioContext
	// ClosedLoop enables per-scenario reference->analyze->rerun pipelines.
	// The scenario's Config is used for the initial reference phase.
	ClosedLoop *ClosedLoopSpec
}

// ClosedLoopSpec defines how to produce follow-up phases from a reference run.
type ClosedLoopSpec struct {
	// Plan receives the completed reference phase result and returns zero or more
	// follow-up phases to execute immediately for this same scenario/input.
	Plan func(reference ScenarioPhaseResult) ([]ScenarioPhasePlan, error)
}

// ScenarioPhasePlan describes one follow-up phase emitted by a closed-loop planner.
type ScenarioPhasePlan struct {
	Name string
	// Config to use for this phase.
	Config tracecheck.ExploreConfig
	// Seed overrides the default scenario initial state when provided. This enables
	// checkpoint restarts from intermediate reference states.
	Seed *tracecheck.RestartSeed
	// Prefix optionally prepends reference history when restarting from checkpoints.
	Prefix tracecheck.ExecutionHistory
	// Context overrides scenario context for this phase when set.
	Context *ScenarioContext
}

// ScenarioPhaseResult captures one phase execution in a scenario pipeline.
type ScenarioPhaseResult struct {
	Name           string
	Result         *tracecheck.Result
	VersionManager tracecheck.VersionManager
	Stats          *tracecheck.ExploreStats
	DumpPath       string
	InvariantError error
	Err            error
}

// ScenarioResult captures scenario outcomes and optional artifacts.
type ScenarioResult struct {
	Name           string
	Result         *tracecheck.Result
	VersionManager tracecheck.VersionManager
	Stats          *tracecheck.ExploreStats
	DumpPath       string
	InvariantError error
	Err            error
	// Phases captures reference and follow-up runs for closed-loop scenarios.
	Phases []ScenarioPhaseResult
}
