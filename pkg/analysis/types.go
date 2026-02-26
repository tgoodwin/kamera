// Package analysis provides types and utilities for analyzing kamera dump files.
package analysis

import (
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
)

// Dump represents the top-level structure of a kamera dump file.
// It contains all objects referenced by hash and the result states from exploration.
type Dump struct {
	Context *DumpContext             `json:"context,omitempty"`
	Stats   *tracecheck.ExploreStats `json:"stats,omitempty"`
	Objects []DumpObject             `json:"objects"`
	States  []DumpResultState        `json:"states"`
}

// DumpContext carries optional metadata describing how the dump was produced.
type DumpContext struct {
	Scenario *DumpScenarioContext `json:"scenario,omitempty"`
}

// DumpScenarioContext describes the scenario/workflow that produced this dump.
type DumpScenarioContext struct {
	Name       string            `json:"name,omitempty"`
	RunIndex   *int              `json:"runIndex,omitempty"`
	Workflow   string            `json:"workflow,omitempty"`
	InputRef   string            `json:"inputRef,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// DumpObject represents a single object version stored in the dump.
// The Hash uniquely identifies this version of the object.
type DumpObject struct {
	Hash   snapshot.VersionHash   `json:"hash"`
	Object map[string]interface{} `json:"object"`
}

// DumpResultState represents the result of exploring from a particular initial state.
type DumpResultState struct {
	ID              string                  `json:"id"`
	Error           string                  `json:"error,omitempty"`
	DivergencePoint string                  `json:"divergencePoint"`
	State           DumpStateNode           `json:"state"`
	Paths           [][]DumpReconcileResult `json:"paths"`
}

// DumpStateNode wraps the state snapshot contents.
type DumpStateNode struct {
	Contents DumpStateSnapshot `json:"contents"`
}

// DumpStateSnapshot represents a point-in-time snapshot of the system state.
type DumpStateSnapshot struct {
	Objects           []DumpObjectVersion      `json:"objects"`
	KindSequences     tracecheck.KindSequences `json:"kindSequences"`
	PendingReconciles []DumpPendingReconcile   `json:"pendingReconciles,omitempty"`
}

// DumpObjectVersion associates a composite key with a specific version hash.
type DumpObjectVersion struct {
	Key  snapshot.CompositeKey `json:"key"`
	Hash snapshot.VersionHash  `json:"hash"`
}

// DumpReconcileResult represents the outcome of a single reconcile step.
type DumpReconcileResult struct {
	ControllerID      string                   `json:"controllerId"`
	ContentsHashAfter string                   `json:"contentsHashAfter,omitempty"`
	FrameID           string                   `json:"frameId"`
	FrameType         tracecheck.FrameType     `json:"frameType"`
	StepMetadata      map[string]string        `json:"stepMetadata,omitempty"`
	Changes           DumpChanges              `json:"changes"`
	Error             string                   `json:"error,omitempty"`
	Deltas            []DumpDelta              `json:"deltas,omitempty"`
	StateBefore       []DumpObjectVersion      `json:"stateBefore,omitempty"`
	StateAfter        []DumpObjectVersion      `json:"stateAfter,omitempty"`
	KindSeqBefore     tracecheck.KindSequences `json:"kindSeqBefore,omitempty"`
	KindSeqAfter      tracecheck.KindSequences `json:"kindSeqAfter,omitempty"`
	Pending           []DumpPendingReconcile   `json:"pendingReconciles,omitempty"`
}

// DumpChanges represents the changes made during a reconcile step.
type DumpChanges struct {
	ObjectVersions []DumpObjectVersion `json:"objectVersions"`
	Effects        []tracecheck.Effect `json:"effects"`
	Observations   []tracecheck.Effect `json:"observations,omitempty"`
}

// DumpDelta represents a delta (diff) for a specific object key.
type DumpDelta struct {
	Key  snapshot.CompositeKey `json:"key"`
	Hash snapshot.VersionHash  `json:"hash,omitempty"`
	Val  string                `json:"value"`
}

// DumpPendingReconcile represents a pending reconcile request.
type DumpPendingReconcile struct {
	ReconcilerID string                            `json:"reconcilerId"`
	Namespace    string                            `json:"namespace"`
	Name         string                            `json:"name"`
	Source       tracecheck.PendingReconcileSource `json:"source"`
}
