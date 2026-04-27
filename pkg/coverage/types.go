package coverage

import (
	"github.com/tgoodwin/kamera/pkg/event"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Input captures the starting cluster state and declarative inputs for a scenario.
type Input struct {
	Name             string           `json:"name"`
	EnvironmentState EnvironmentState `json:"environmentState"`
	ExternalInputs   []ExternalInput   `json:"externalInputs"`
	Tuning           InputTuning      `json:"tuning"`
}

// EnvironmentState captures baseline objects present before user actions.
type EnvironmentState struct {
	Objects []*unstructured.Unstructured `json:"objects"`
}

// ExternalInputSource distinguishes the origin of an external input.
type ExternalInputSource string

const (
	// ExternalSourceUserAction represents a deliberate user/operator action
	// (e.g., applying a new manifest, deleting a resource).
	ExternalSourceUserAction ExternalInputSource = "UserAction"
	// ExternalSourceEnvironmentEvent represents an infrastructure or
	// environment change outside the controller system (e.g., cloud provider
	// API failure, AMI deletion, kube-scheduler pod binding).
	ExternalSourceEnvironmentEvent ExternalInputSource = "EnvironmentEvent"
)

// ExternalInput models a declarative state change originating outside the
// controller system — either a user/operator action or an environment event.
type ExternalInput struct {
	ID     string                     `json:"id"`
	OpType event.OperationType        `json:"opType"`
	Source ExternalInputSource        `json:"source,omitempty"`
	Object *unstructured.Unstructured `json:"object"`
}

// Deprecated: UserInput is an alias for ExternalInput for backward compatibility.
type UserInput = ExternalInput

// InputStalenessInterval is the JSON-facing staleness interval configuration.
type InputStalenessInterval struct {
	Reconciler string `json:"reconciler"`
	Kind       string `json:"kind"`
	StaleAt    int64  `json:"staleAt"`
	CatchUpAt  int64  `json:"catchUpAt"`
	Lag        int64  `json:"lag"`
}

// InputDepthRange constrains permutation expansion to a specific depth window.
type InputDepthRange struct {
	Min int `json:"min"`
	Max int `json:"max"`
}

// InputPermuteEvent specifies an event that must occur before permutation begins.
type InputPermuteEvent struct {
	OpType string `json:"opType"`
	Kind   string `json:"kind"`
}

// InputTuning carries compact hints for later ExploreConfig construction.
type InputTuning struct {
	MaxDepth           int                `json:"maxDepth"`
	PermuteControllers []string           `json:"permuteControllers"`
	PermuteDepthRange  *InputDepthRange   `json:"permuteDepthRange,omitempty"`
	PermuteAfterEvent  *InputPermuteEvent `json:"permuteAfterEvent,omitempty"`
	// TODO cleanup in favor of StalenessIntervals
	StaleReads            map[string][]string      `json:"staleReads"`
	StaleLookback         map[string]int           `json:"staleLookback"`
	UserActionReadyDepths map[string]int           `json:"userActionReadyDepths,omitempty"`
	StalenessIntervals    []InputStalenessInterval `json:"stalenessIntervals"`
	FaultInjection        []InputFaultInjection    `json:"faultInjection,omitempty"`
	Search                InputSearchTuning        `json:"search"`
}

// InputFaultInjection specifies a mid-reconcile crash for a specific reconciler.
type InputFaultInjection struct {
	Reconciler       string `json:"reconciler"`
	CrashAfterEffect int    `json:"crashAfterEffect"`
	RecoverAtDepth   int    `json:"recoverAtDepth,omitempty"`
	TriggerOnce      bool   `json:"triggerOnce,omitempty"`
}

// InputSearchTuning carries optional per-input search-mode overrides.
type InputSearchTuning struct {
	Mode       string                `json:"mode"`
	MonteCarlo InputMonteCarloTuning `json:"monteCarlo"`
}

// InputMonteCarloTuning carries optional per-input monte-carlo settings.
type InputMonteCarloTuning struct {
	Seed          *int64  `json:"seed,omitempty"`
	Trials        *int    `json:"trials,omitempty"`
	TrialIndex    *int    `json:"trialIndex,omitempty"`
	ScenarioGroup *string `json:"scenarioGroup,omitempty"`
}

// InputMap is the on-disk schema seed mapping from GVK to a single template object.
type InputMap struct {
	Mapping map[string][]InputTemplate `json:"mapping"`
}

// InputTemplate is a single named template for a GVK.
type InputTemplate struct {
	Name   string                     `json:"name"`
	Object *unstructured.Unstructured `json:"object"`
}
