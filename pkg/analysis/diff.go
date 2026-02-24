// Package analysis provides types and utilities for analyzing kamera dump files.
package analysis

import (
	"github.com/tgoodwin/kamera/pkg/snapshot"
)

// ConvergedStateDiff represents the result of comparing converged states across
// multiple exploration paths. It identifies which objects differ between states.
type ConvergedStateDiff struct {
	NumStates        int          // Number of states compared
	DifferingObjects []ObjectDiff // Objects that differ across states
	IdenticalCount   int          // Number of objects identical across all states
}

// ObjectDiff captures how a single object differs across states.
// ByState maps each state ID to the object's hash in that state.
// An empty hash indicates the object is missing in that state.
type ObjectDiff struct {
	Key     snapshot.CompositeKey            // The object's identity
	ByState map[string]snapshot.VersionHash  // stateID -> hash (empty if missing)
}

// DiffConvergedStates compares the final converged states from all exploration paths
// and identifies which objects differ between them. This is the entry point for
// divergence analysis (Module 0).
//
// Returns nil if dump is nil or has fewer than 2 states (no diff possible).
func DiffConvergedStates(dump *Dump) *ConvergedStateDiff {
	// Early return for nil or insufficient states
	if dump == nil || len(dump.States) < 2 {
		return &ConvergedStateDiff{
			NumStates:        len(statesOrZero(dump)),
			DifferingObjects: nil,
			IdenticalCount:   0,
		}
	}

	// Build object->hash map for each state
	// stateObjects maps: stateID -> (CompositeKey -> VersionHash)
	stateObjects := make(map[string]map[snapshot.CompositeKey]snapshot.VersionHash)

	for _, state := range dump.States {
		objMap := make(map[snapshot.CompositeKey]snapshot.VersionHash)
		for _, obj := range state.State.Contents.Objects {
			objMap[obj.Key] = obj.Hash
		}
		stateObjects[state.ID] = objMap
	}

	// Collect all unique keys across all states
	allKeys := make(map[snapshot.CompositeKey]struct{})
	for _, objMap := range stateObjects {
		for key := range objMap {
			allKeys[key] = struct{}{}
		}
	}

	// Compare hashes across states for each key
	var differingObjects []ObjectDiff
	identicalCount := 0

	for key := range allKeys {
		byState := make(map[string]snapshot.VersionHash)
		var baselineHash snapshot.VersionHash
		baselineSet := false
		differs := false
		presentCount := 0

		// Iterate states in dump order (not map order) for deterministic behavior.
		for _, state := range dump.States {
			stateID := state.ID
			objMap := stateObjects[stateID]
			hash, exists := objMap[key]
			if exists {
				byState[stateID] = hash
				presentCount++
				if !baselineSet {
					baselineHash = hash
					baselineSet = true
				} else if hash.Value != baselineHash.Value {
					differs = true
				}
			} else {
				byState[stateID] = snapshot.VersionHash{} // empty hash
			}
		}

		// A key is differing if it is missing in at least one state.
		// allKeys is built from a union of state objects, so presentCount will be >0.
		if presentCount != len(dump.States) {
			differs = true
		}

		if differs {
			differingObjects = append(differingObjects, ObjectDiff{
				Key:     key,
				ByState: byState,
			})
		} else {
			identicalCount++
		}
	}

	return &ConvergedStateDiff{
		NumStates:        len(dump.States),
		DifferingObjects: differingObjects,
		IdenticalCount:   identicalCount,
	}
}

// statesOrZero returns the number of states in the dump, or 0 if dump is nil
func statesOrZero(dump *Dump) []DumpResultState {
	if dump == nil {
		return nil
	}
	return dump.States
}
