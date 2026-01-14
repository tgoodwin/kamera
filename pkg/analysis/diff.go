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
		var firstHash snapshot.VersionHash
		firstSet := false
		differs := false

		for stateID, objMap := range stateObjects {
			hash, exists := objMap[key]
			if exists {
				byState[stateID] = hash
				if !firstSet {
					firstHash = hash
					firstSet = true
				} else if hash.Value != firstHash.Value {
					differs = true
				}
			} else {
				// Object missing in this state - represents a difference
				byState[stateID] = snapshot.VersionHash{} // empty hash
				if firstSet {
					differs = true
				}
			}
		}

		// If we never set firstHash but have entries, that means all states
		// are missing this key (shouldn't happen, but handle it)
		if !firstSet && len(byState) > 0 {
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
