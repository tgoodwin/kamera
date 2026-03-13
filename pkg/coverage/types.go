package coverage

import (
	"github.com/tgoodwin/kamera/pkg/event"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Input captures the starting cluster state and declarative inputs for a scenario.
type Input struct {
	Name             string           `json:"name"`
	EnvironmentState EnvironmentState `json:"environmentState"`
	UserInputs       []UserInput      `json:"userInputs"`
	Tuning           InputTuning      `json:"tuning"`
}

// EnvironmentState captures baseline objects present before user actions.
type EnvironmentState struct {
	Objects []*unstructured.Unstructured `json:"objects"`
}

// UserInput models a declarative state change performed by the user
type UserInput struct {
	ID     string                     `json:"id"`
	Type   event.OperationType        `json:"type"`
	Object *unstructured.Unstructured `json:"object"`
}

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
	Search                InputSearchTuning        `json:"search"`
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
