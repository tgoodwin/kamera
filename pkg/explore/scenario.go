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
}
