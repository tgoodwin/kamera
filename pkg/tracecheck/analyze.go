package tracecheck

import (
	"fmt"

	"github.com/tgoodwin/sleeve/pkg/snapshot"
)

// DiffResult represents the differences between two ObjectVersions maps
type DiffResult struct {
	BaseID         string
	OtherID        string
	UniqueElements map[string][]snapshot.IdentityKey
	// Objects present in the first map but not in the second
	OnlyInFirst []snapshot.IdentityKey
	// Objects present in the second map but not in the first
	OnlyInSecond []snapshot.IdentityKey
	// Objects present in both maps but with different versions
	Modified map[snapshot.IdentityKey][]string // key -> deltas
}

// DiffObjectVersions compares two ObjectVersions maps and returns their differences
func (tc *TraceChecker) DiffObjectVersions(first, second ConvergedState) DiffResult {
	result := DiffResult{
		BaseID:         first.ID,
		OtherID:        second.ID,
		UniqueElements: make(map[string][]snapshot.IdentityKey),
		OnlyInFirst:    make([]snapshot.IdentityKey, 0),
		OnlyInSecond:   make([]snapshot.IdentityKey, 0),
		Modified:       make(map[snapshot.IdentityKey][]string),
	}

	firstWorld := first.State.ObjectVersions
	secondWorld := second.State.ObjectVersions

	// Find keys only in first and modified objects
	for key, firstHash := range firstWorld {
		if secondHash, exists := secondWorld[key]; !exists {
			result.OnlyInFirst = append(result.OnlyInFirst, key)
			result.UniqueElements[first.ID] = append(result.UniqueElements[first.ID], key)
		} else if firstHash != secondHash {
			// Objects exist in both maps but have different versions
			deltas := tc.manager.Diff(&firstHash, &secondHash)
			if len(deltas) > 0 {
				result.Modified[key] = append(result.Modified[key], deltas)
			}
		}
	}

	// Find keys only in second
	for key := range secondWorld {
		if _, exists := firstWorld[key]; !exists {
			result.OnlyInSecond = append(result.OnlyInSecond, key)
			result.UniqueElements[second.ID] = append(result.UniqueElements[second.ID], key)
		}
	}

	return result
}

func (tc *TraceChecker) DiffResults(result *Result) {
	convergedStates := result.ConvergedStates
	for i, state := range convergedStates {
		for j, otherState := range convergedStates {
			if i == j {
				continue
			}
			diff := tc.DiffObjectVersions(state, otherState)
			diff.Print()
			// tc.writeDiffSummary(diff, i, j)
		}
	}

}

func (dr DiffResult) Print() {
	fmt.Printf("Diff between %s and %s\n", dr.BaseID, dr.OtherID)
	fmt.Printf("Only in %s: %v\n", dr.BaseID, dr.OnlyInFirst)
	fmt.Printf("Only in %s: %v\n", dr.OtherID, dr.OnlyInSecond)
	fmt.Printf("First to Second delta:\n")
	for key, deltas := range dr.Modified {
		fmt.Printf("\t%s: %v\n", key, deltas)
	}
}
