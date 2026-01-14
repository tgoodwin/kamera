// Package analysis provides types and utilities for analyzing kamera dump files.
package analysis

import (
	"github.com/tgoodwin/kamera/pkg/snapshot"
)

// ObjectLifecycleResult represents the analysis of where an object takes on
// a specific hash value throughout a path in the exploration.
type ObjectLifecycleResult struct {
	Object      snapshot.CompositeKey
	TargetHash  snapshot.VersionHash
	StateIndex  int
	PathIndex   int
	Found       bool
	Appearances []StepInfo
}

// StepInfo identifies a step where an object had a specific hash value.
type StepInfo struct {
	StepIndex    int
	ControllerId string
}

// AnalyzeObjectLifecycle finds all steps where an object had a specific hash value.
// This is Module 2 of the backward-trace analysis framework.
//
// It answers "Does this object take on this value at some point in this path?"
// - crucial for determining if missing state is a timing issue vs never existing.
//
// Algorithm:
// 1. Get the specified state and path from the dump
// 2. Walk through all steps in the path
// 3. For each step, check if the object has the target hash in StateAfter
// 4. If found, record the step index and controller ID
func AnalyzeObjectLifecycle(dump *Dump, stateIdx, pathIdx int, key snapshot.CompositeKey, targetHash snapshot.VersionHash) *ObjectLifecycleResult {
	result := &ObjectLifecycleResult{
		Object:      key,
		TargetHash:  targetHash,
		StateIndex:  stateIdx,
		PathIndex:   pathIdx,
		Found:       false,
		Appearances: make([]StepInfo, 0),
	}

	if dump == nil {
		return result
	}

	// Validate state index
	if stateIdx < 0 || stateIdx >= len(dump.States) {
		return result
	}

	state := dump.States[stateIdx]

	// Validate path index
	if pathIdx < 0 || pathIdx >= len(state.Paths) {
		return result
	}

	path := state.Paths[pathIdx]

	// Walk through all steps in the path
	for stepIdx, step := range path {
		// Check if this step's StateAfter contains the target hash for our key
		hash, found := findObjectHash(step.StateAfter, key)
		if found && hash.Value == targetHash.Value {
			result.Found = true
			result.Appearances = append(result.Appearances, StepInfo{
				StepIndex:    stepIdx,
				ControllerId: step.ControllerID,
			})
		}
	}

	return result
}
