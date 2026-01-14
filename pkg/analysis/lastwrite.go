// Package analysis provides types and utilities for analyzing kamera dump files.
package analysis

import (
	"github.com/tgoodwin/kamera/pkg/snapshot"
)

// LastWriteAnalysis represents the analysis of which reconciler last wrote
// an object's final value across all paths in a dump.
type LastWriteAnalysis struct {
	Object snapshot.CompositeKey
	ByPath []PathLastWrite
}

// PathLastWrite captures the last write information for a specific path.
type PathLastWrite struct {
	PathIndex     int
	StateID       string
	FinalHash     snapshot.VersionHash
	LastWriteStep LastWriteStep
}

// LastWriteStep identifies the step that produced the final value for an object.
type LastWriteStep struct {
	StepIndex    int
	ControllerId string
	StateBefore  []DumpObjectVersion // what the reconciler saw
}

// AnalyzeLastWrite finds the step that produced the final value for an object
// in each path. This is Module 1 of the backward-trace analysis framework.
//
// Algorithm (per state, per path):
// 1. Find the final hash for the object in the converged state
// 2. Walk backwards through the path's steps
// 3. Find the first step (from the end) where the object has the final hash in StateAfter
// 4. That step's ControllerID is the "last writer"
// 5. Capture StateBefore from that step
func AnalyzeLastWrite(dump *Dump, key snapshot.CompositeKey) *LastWriteAnalysis {
	if dump == nil {
		return &LastWriteAnalysis{
			Object: key,
			ByPath: nil,
		}
	}

	result := &LastWriteAnalysis{
		Object: key,
		ByPath: make([]PathLastWrite, 0),
	}

	// Process each state and its paths
	for _, state := range dump.States {
		// Find the final hash for the object in the converged state
		finalHash, found := findObjectHash(state.State.Contents.Objects, key)
		if !found {
			// Object not present in this state's final contents, skip
			continue
		}

		// Process each path for this state
		for pathIdx, path := range state.Paths {
			lastWrite := findLastWriteInPath(path, key, finalHash)
			if lastWrite != nil {
				result.ByPath = append(result.ByPath, PathLastWrite{
					PathIndex:     pathIdx,
					StateID:       state.ID,
					FinalHash:     finalHash,
					LastWriteStep: *lastWrite,
				})
			}
		}
	}

	return result
}

// findObjectHash looks up an object's hash in a list of object versions.
func findObjectHash(objects []DumpObjectVersion, key snapshot.CompositeKey) (snapshot.VersionHash, bool) {
	for _, obj := range objects {
		if obj.Key == key {
			return obj.Hash, true
		}
	}
	return snapshot.VersionHash{}, false
}

// findLastWriteInPath walks backwards through a path's steps to find the step
// that produced the final hash value for the object.
func findLastWriteInPath(path []DumpReconcileResult, key snapshot.CompositeKey, finalHash snapshot.VersionHash) *LastWriteStep {
	// Walk backwards through the path
	for i := len(path) - 1; i >= 0; i-- {
		step := path[i]

		// Check if this step's StateAfter contains the final hash for our key
		hash, found := findObjectHash(step.StateAfter, key)
		if found && hash.Value == finalHash.Value {
			return &LastWriteStep{
				StepIndex:    i,
				ControllerId: step.ControllerID,
				StateBefore:  step.StateBefore,
			}
		}
	}

	return nil
}
