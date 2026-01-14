// Package analysis provides types and utilities for analyzing kamera dump files.
package analysis

import (
	"fmt"
	"strings"

	"github.com/tgoodwin/kamera/pkg/snapshot"
)

// formatKey formats a CompositeKey as Kind/Namespace/Name.
func formatKey(key snapshot.CompositeKey) string {
	return fmt.Sprintf("%s/%s/%s", key.ResourceKey.Kind, key.ResourceKey.Namespace, key.ResourceKey.Name)
}

// shortenHash returns a shortened version of a hash for display.
// Returns the first 7 characters, or "(missing)" if empty.
func shortenHash(hash snapshot.VersionHash) string {
	if hash.Value == "" {
		return "(missing)"
	}
	if len(hash.Value) > 7 {
		return hash.Value[:7]
	}
	return hash.Value
}

// FormatConvergedStateDiff produces a human-readable summary of state differences.
//
// Example output:
//
//	2 converged states with 1 differing object(s), 5 identical
//
//	  Endpoints/ns/ep1:
//	    State state1: abc123
//	    State state2: def456
func FormatConvergedStateDiff(diff *ConvergedStateDiff) string {
	if diff == nil {
		return "No diff available"
	}

	var sb strings.Builder

	// Header line
	sb.WriteString(fmt.Sprintf("%d converged states with %d differing object(s), %d identical\n",
		diff.NumStates, len(diff.DifferingObjects), diff.IdenticalCount))

	// List each differing object
	for _, objDiff := range diff.DifferingObjects {
		sb.WriteString(fmt.Sprintf("\n  %s:\n", formatKey(objDiff.Key)))
		for stateID, hash := range objDiff.ByState {
			sb.WriteString(fmt.Sprintf("    State %s: %s\n", stateID, shortenHash(hash)))
		}
	}

	return sb.String()
}

// FormatLastWriteAnalysis produces a human-readable summary of last write analysis.
//
// Example output:
//
//	Last write analysis for Endpoints/ns/ep1:
//
//	  Path 0 (-> state state1):
//	    Last write: step 25 by EndpointsController
//	    Final hash: abc123
func FormatLastWriteAnalysis(result *LastWriteAnalysis) string {
	if result == nil {
		return "No last write analysis available"
	}

	var sb strings.Builder

	// Header
	sb.WriteString(fmt.Sprintf("Last write analysis for %s:\n", formatKey(result.Object)))

	if len(result.ByPath) == 0 {
		sb.WriteString("\n  No paths found\n")
		return sb.String()
	}

	// List each path's last write
	for _, pathWrite := range result.ByPath {
		sb.WriteString(fmt.Sprintf("\n  Path %d (-> state %s):\n", pathWrite.PathIndex, pathWrite.StateID))
		sb.WriteString(fmt.Sprintf("    Last write: step %d by %s\n",
			pathWrite.LastWriteStep.StepIndex, pathWrite.LastWriteStep.ControllerId))
		sb.WriteString(fmt.Sprintf("    Final hash: %s\n", shortenHash(pathWrite.FinalHash)))
	}

	return sb.String()
}

// FormatObjectLifecycle produces a human-readable summary of object lifecycle analysis.
//
// Example output:
//
//	Object: Pod/ns/pod1
//	Target hash: xyz789
//	State 0, Path 0
//
//	  Found 2 appearance(s):
//	    - step 38: PodLifecycleController
//	    - step 45: PodLifecycleController
func FormatObjectLifecycle(result *ObjectLifecycleResult) string {
	if result == nil {
		return "No lifecycle analysis available"
	}

	var sb strings.Builder

	// Header
	sb.WriteString(fmt.Sprintf("Object: %s\n", formatKey(result.Object)))
	sb.WriteString(fmt.Sprintf("Target hash: %s\n", shortenHash(result.TargetHash)))
	sb.WriteString(fmt.Sprintf("State %d, Path %d\n", result.StateIndex, result.PathIndex))

	if !result.Found || len(result.Appearances) == 0 {
		sb.WriteString("\n  No appearances found\n")
		return sb.String()
	}

	// List appearances
	sb.WriteString(fmt.Sprintf("\n  Found %d appearance(s):\n", len(result.Appearances)))
	for _, appearance := range result.Appearances {
		sb.WriteString(fmt.Sprintf("    - step %d: %s\n", appearance.StepIndex, appearance.ControllerId))
	}

	return sb.String()
}
