package tracecheck

import (
	"fmt"

	"github.com/samber/lo"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
)

// DiffResult represents the differences between two ObjectVersions maps
type DiffResult struct {
	BaseID         string
	OtherID        string
	UniqueElements map[string][]string
	// Objects present in the first map but not in the second
	OnlyInFirst []snapshot.IdentityKey
	// Objects present in the second map but not in the first
	OnlyInSecond []snapshot.IdentityKey
	// Objects present in both maps but with different versions
	Modified map[string]string // key -> deltas
}

// DiffObjectVersions compares two ObjectVersions maps and returns their differences
func (tc *TraceChecker) DiffObjectVersions(first, second ConvergedState) DiffResult {
	result := DiffResult{
		BaseID:         first.ID,
		OtherID:        second.ID,
		UniqueElements: make(map[string][]string),
		// OnlyInFirst:    make([]snapshot.IdentityKey, 0),
		// OnlyInSecond:   make([]snapshot.IdentityKey, 0),
		Modified: make(map[string]string),
	}

	firstWorld := first.State.Objects()
	secondWorld := second.State.Objects()

	firstNormalized := lo.MapValues(firstWorld, func(v snapshot.VersionHash, _ snapshot.CompositeKey) snapshot.NormalizedObject {
		return snapshot.NormalizeObject(v)
	})
	secondNormalized := lo.MapValues(secondWorld, func(v snapshot.VersionHash, _ snapshot.CompositeKey) snapshot.NormalizedObject {
		return snapshot.NormalizeObject(v)
	})

	fn := lo.MapEntries(firstNormalized, func(k snapshot.CompositeKey, v snapshot.NormalizedObject) (string, snapshot.NormalizedObject) {
		return v.Identity(), v
	})

	sn := lo.MapEntries(secondNormalized, func(k snapshot.CompositeKey, v snapshot.NormalizedObject) (string, snapshot.NormalizedObject) {
		return v.Identity(), v
	})

	for nkey, nval := range fn {
		if _, exists := sn[nkey]; !exists {
			result.UniqueElements[first.ID] = append(result.UniqueElements[first.ID], nval.Identity())
		} else {
			nDelta, err := nval.Diff(sn[nkey])
			if err != nil {
				fmt.Printf("Error diffing normalized objects: %v\n", err)
			}
			if len(nDelta.SpecDiff) > 0 || len(nDelta.StatusDiff) > 0 {
				result.Modified[nkey] = fmt.Sprintf("Spec: %s\nStatus: %s", nDelta.SpecDiff, nDelta.StatusDiff)
			}
		}
	}

	for nkey, nval := range sn {
		if _, exists := fn[nkey]; !exists {
			result.UniqueElements[second.ID] = append(result.UniqueElements[second.ID], nval.Identity())
		}
	}

	return result
}

func (tc *TraceChecker) DiffResults(result *Result) {
	convergedStates := result.ConvergedStates
	for i := 0; i < len(convergedStates); i++ {
		for j := i + 1; j < len(convergedStates); j++ {
			diff := tc.DiffObjectVersions(convergedStates[i], convergedStates[j])
			diff.Print()
			// tc.writeDiffSummary(diff, i, j)
		}
	}
}

func (dr DiffResult) Print() {
	if len(dr.UniqueElements[dr.BaseID]) == 0 && len(dr.UniqueElements[dr.OtherID]) == 0 && len(dr.Modified) == 0 {
		fmt.Printf("No differences between %s and %s\n", dr.BaseID, dr.OtherID)
		return
	}
	fmt.Printf("Diff between %s and %s\n", dr.BaseID, dr.OtherID)
	fmt.Printf("Only in %s: %v\n", dr.BaseID, dr.UniqueElements[dr.BaseID])
	fmt.Printf("Only in %s: %v\n", dr.OtherID, dr.UniqueElements[dr.OtherID])
	fmt.Printf("%s to %s delta:\n", dr.BaseID, dr.OtherID)
	for key, deltas := range dr.Modified {
		fmt.Printf("Delta for %s:\n%v\n", key, deltas)
	}
}
