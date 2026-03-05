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

// InputTuning carries compact hints for later ExploreConfig construction.
type InputTuning struct {
	MaxDepth           int                 `json:"maxDepth"`
	PermuteControllers []string            `json:"permuteControllers"`
	StaleReads         map[string][]string `json:"staleReads"`
	StaleLookback      map[string]int      `json:"staleLookback"`
	Search             InputSearchTuning   `json:"search"`
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
