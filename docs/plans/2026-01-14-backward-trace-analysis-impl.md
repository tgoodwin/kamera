# Backward-Trace Divergence Analysis Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build composable analysis modules in `pkg/analysis` to trace divergence from final state differences back to root cause.

**Architecture:** Three modules (Diff → LastWrite → Lifecycle) that chain together. Each takes a dump file and produces structured output that can feed the next module or be rendered as human-readable text.

**Tech Stack:** Go, uses existing `pkg/snapshot` and `pkg/tracecheck` types. Test with trial-1 dump fixture.

---

## Task 1: Create pkg/analysis with dump types

**Files:**
- Create: `pkg/analysis/types.go`
- Create: `pkg/analysis/dump.go`
- Modify: `pkg/interactive/inspector_dump.go`

**Step 1: Create the analysis package directory**

```bash
mkdir -p pkg/analysis
```

**Step 2: Create types.go with dump types moved from interactive**

Create `pkg/analysis/types.go` with the dump struct types. These are the JSON serialization types:
- `Dump` (renamed from `inspectorDump`)
- `DumpObject`
- `DumpResultState`
- `DumpStateNode`
- `DumpStateSnapshot`
- `DumpObjectVersion`
- `DumpReconcileResult`
- `DumpChanges`
- `DumpDelta`
- `DumpPendingReconcile`

Export all types (capitalized names).

**Step 3: Create dump.go with LoadDump function**

Create `pkg/analysis/dump.go` with:
```go
func LoadDump(path string) (*Dump, error)
```

This is a simplified loader that just unmarshals the JSON without converting to tracecheck types.

**Step 4: Update pkg/interactive/inspector_dump.go to import from analysis**

Update imports and use `analysis.Dump` internally, keeping the existing `LoadInspectorDump` function that converts to tracecheck types.

**Step 5: Run tests to verify no regressions**

```bash
go test ./pkg/interactive/... ./pkg/analysis/...
```

**Step 6: Commit**

```bash
git add pkg/analysis/ pkg/interactive/
git commit -m "refactor: move dump types to pkg/analysis"
```

---

## Task 2: Implement Module 0 - Converged State Diff

**Files:**
- Create: `pkg/analysis/diff.go`
- Create: `pkg/analysis/diff_test.go`

**Step 1: Write the test file with test for single-state dump (no diff)**

Create `pkg/analysis/diff_test.go`:
```go
package analysis

import (
	"testing"
)

func TestDiffConvergedStates_SingleState(t *testing.T) {
	dump := &Dump{
		States: []DumpResultState{
			{ID: "state1", State: DumpStateNode{Contents: DumpStateSnapshot{
				Objects: []DumpObjectVersion{
					{Key: testKey("Pod", "ns", "pod1"), Hash: testHash("hash1")},
				},
			}}},
		},
	}

	diff := DiffConvergedStates(dump)

	if diff.NumStates != 1 {
		t.Errorf("expected 1 state, got %d", diff.NumStates)
	}
	if len(diff.DifferingObjects) != 0 {
		t.Errorf("expected no differing objects for single state, got %d", len(diff.DifferingObjects))
	}
}

// Test helpers
func testKey(kind, ns, name string) snapshot.CompositeKey {
	return snapshot.NewCompositeKey(kind, ns, name, "")
}

func testHash(val string) snapshot.VersionHash {
	return snapshot.VersionHash{Value: val, Strategy: "test"}
}
```

**Step 2: Run test to verify it fails**

```bash
go test ./pkg/analysis/... -run TestDiffConvergedStates_SingleState -v
```

Expected: FAIL - `DiffConvergedStates` not defined

**Step 3: Write minimal diff.go implementation**

Create `pkg/analysis/diff.go`:
```go
package analysis

import "github.com/tgoodwin/kamera/pkg/snapshot"

// ConvergedStateDiff represents differences between converged states
type ConvergedStateDiff struct {
	NumStates        int
	DifferingObjects []ObjectDiff
	IdenticalCount   int
}

// ObjectDiff shows how an object differs across states
type ObjectDiff struct {
	Key     snapshot.CompositeKey
	ByState map[string]snapshot.VersionHash // stateID → hash (empty if missing)
}

// DiffConvergedStates compares all converged states and identifies differing objects
func DiffConvergedStates(dump *Dump) *ConvergedStateDiff {
	if dump == nil || len(dump.States) == 0 {
		return &ConvergedStateDiff{}
	}

	result := &ConvergedStateDiff{
		NumStates: len(dump.States),
	}

	if len(dump.States) == 1 {
		return result
	}

	// Build object→hash map for each state
	stateMaps := make([]map[string]snapshot.VersionHash, len(dump.States))
	allKeys := make(map[string]snapshot.CompositeKey)

	for i, state := range dump.States {
		stateMaps[i] = make(map[string]snapshot.VersionHash)
		// Use final stateAfter from last path step, or state.State.Contents.Objects
		objects := state.State.Contents.Objects
		for _, obj := range objects {
			keyStr := obj.Key.String()
			stateMaps[i][keyStr] = obj.Hash
			allKeys[keyStr] = obj.Key
		}
	}

	// Find differing objects
	for keyStr, key := range allKeys {
		byState := make(map[string]snapshot.VersionHash)
		var firstHash snapshot.VersionHash
		differs := false

		for i, state := range dump.States {
			hash := stateMaps[i][keyStr]
			byState[state.ID] = hash
			if i == 0 {
				firstHash = hash
			} else if hash != firstHash {
				differs = true
			}
		}

		if differs {
			result.DifferingObjects = append(result.DifferingObjects, ObjectDiff{
				Key:     key,
				ByState: byState,
			})
		} else {
			result.IdenticalCount++
		}
	}

	return result
}
```

**Step 4: Run test to verify it passes**

```bash
go test ./pkg/analysis/... -run TestDiffConvergedStates_SingleState -v
```

Expected: PASS

**Step 5: Add test for two states with one differing object**

Add to `diff_test.go`:
```go
func TestDiffConvergedStates_TwoStatesOneDiff(t *testing.T) {
	dump := &Dump{
		States: []DumpResultState{
			{ID: "state1", State: DumpStateNode{Contents: DumpStateSnapshot{
				Objects: []DumpObjectVersion{
					{Key: testKey("Pod", "ns", "pod1"), Hash: testHash("hash1")},
					{Key: testKey("Endpoints", "ns", "ep1"), Hash: testHash("ep-hash-a")},
				},
			}}},
			{ID: "state2", State: DumpStateNode{Contents: DumpStateSnapshot{
				Objects: []DumpObjectVersion{
					{Key: testKey("Pod", "ns", "pod1"), Hash: testHash("hash1")},
					{Key: testKey("Endpoints", "ns", "ep1"), Hash: testHash("ep-hash-b")},
				},
			}}},
		},
	}

	diff := DiffConvergedStates(dump)

	if diff.NumStates != 2 {
		t.Errorf("expected 2 states, got %d", diff.NumStates)
	}
	if len(diff.DifferingObjects) != 1 {
		t.Fatalf("expected 1 differing object, got %d", len(diff.DifferingObjects))
	}
	if diff.DifferingObjects[0].Key.ResourceKey.Kind != "Endpoints" {
		t.Errorf("expected Endpoints to differ, got %s", diff.DifferingObjects[0].Key.ResourceKey.Kind)
	}
	if diff.IdenticalCount != 1 {
		t.Errorf("expected 1 identical object, got %d", diff.IdenticalCount)
	}
}
```

**Step 6: Run all diff tests**

```bash
go test ./pkg/analysis/... -run TestDiffConvergedStates -v
```

Expected: PASS

**Step 7: Add test using real trial-1 dump**

Add integration test:
```go
func TestDiffConvergedStates_Trial1(t *testing.T) {
	dump, err := LoadDump("../../analysis/nondeterminism-verification/trials-study1-both/trial-1/dump.jsonl")
	if err != nil {
		t.Skipf("trial-1 dump not available: %v", err)
	}

	diff := DiffConvergedStates(dump)

	if diff.NumStates != 2 {
		t.Errorf("expected 2 converged states, got %d", diff.NumStates)
	}
	if len(diff.DifferingObjects) == 0 {
		t.Error("expected at least one differing object")
	}

	// Log for manual verification
	t.Logf("Found %d differing objects:", len(diff.DifferingObjects))
	for _, obj := range diff.DifferingObjects {
		t.Logf("  %s", obj.Key.String())
	}
}
```

**Step 8: Run integration test**

```bash
go test ./pkg/analysis/... -run TestDiffConvergedStates_Trial1 -v
```

**Step 9: Commit**

```bash
git add pkg/analysis/diff.go pkg/analysis/diff_test.go
git commit -m "feat(analysis): add Module 0 - converged state diff"
```

---

## Task 3: Implement Module 1 - Last Write Analysis

**Files:**
- Create: `pkg/analysis/lastwrite.go`
- Create: `pkg/analysis/lastwrite_test.go`

**Step 1: Write test for finding last write in single path**

Create `pkg/analysis/lastwrite_test.go`:
```go
package analysis

import (
	"testing"
)

func TestAnalyzeLastWrite_FindsLastWrite(t *testing.T) {
	// Create a dump with one state, one path, multiple steps
	// Object changes from hash1 → hash2 → hash3
	dump := &Dump{
		States: []DumpResultState{{
			ID: "state1",
			State: DumpStateNode{Contents: DumpStateSnapshot{
				Objects: []DumpObjectVersion{
					{Key: testKey("Endpoints", "ns", "ep1"), Hash: testHash("hash3")},
				},
			}},
			Paths: [][]DumpReconcileResult{{
				{
					ControllerID: "Controller1",
					StateAfter: []DumpObjectVersion{
						{Key: testKey("Endpoints", "ns", "ep1"), Hash: testHash("hash1")},
					},
				},
				{
					ControllerID: "Controller2",
					StateAfter: []DumpObjectVersion{
						{Key: testKey("Endpoints", "ns", "ep1"), Hash: testHash("hash2")},
					},
				},
				{
					ControllerID: "EndpointsController",
					StateAfter: []DumpObjectVersion{
						{Key: testKey("Endpoints", "ns", "ep1"), Hash: testHash("hash3")},
					},
				},
			}},
		}},
	}

	key := testKey("Endpoints", "ns", "ep1")
	result := AnalyzeLastWrite(dump, key)

	if len(result.ByPath) != 1 {
		t.Fatalf("expected 1 path result, got %d", len(result.ByPath))
	}
	pathResult := result.ByPath[0]
	if pathResult.LastWriteStep.StepIndex != 2 {
		t.Errorf("expected last write at step 2, got %d", pathResult.LastWriteStep.StepIndex)
	}
	if pathResult.LastWriteStep.ControllerId != "EndpointsController" {
		t.Errorf("expected EndpointsController, got %s", pathResult.LastWriteStep.ControllerId)
	}
}
```

**Step 2: Run test to verify it fails**

```bash
go test ./pkg/analysis/... -run TestAnalyzeLastWrite -v
```

Expected: FAIL - `AnalyzeLastWrite` not defined

**Step 3: Write minimal lastwrite.go implementation**

Create `pkg/analysis/lastwrite.go`:
```go
package analysis

import "github.com/tgoodwin/kamera/pkg/snapshot"

// LastWriteAnalysis shows which step produced the final value for an object
type LastWriteAnalysis struct {
	Object snapshot.CompositeKey
	ByPath []PathLastWrite
}

// PathLastWrite contains last-write info for one path
type PathLastWrite struct {
	PathIndex     int
	StateID       string
	FinalHash     snapshot.VersionHash
	LastWriteStep LastWriteStep
}

// LastWriteStep identifies the step that produced the final value
type LastWriteStep struct {
	StepIndex    int
	ControllerId string
	StateBefore  []DumpObjectVersion // what the reconciler saw
}

// AnalyzeLastWrite finds the step that produced the final hash for each path
func AnalyzeLastWrite(dump *Dump, key snapshot.CompositeKey) *LastWriteAnalysis {
	result := &LastWriteAnalysis{
		Object: key,
	}

	if dump == nil || len(dump.States) == 0 {
		return result
	}

	keyStr := key.String()

	for stateIdx, state := range dump.States {
		// Find final hash for this object in this state
		var finalHash snapshot.VersionHash
		for _, obj := range state.State.Contents.Objects {
			if obj.Key.String() == keyStr {
				finalHash = obj.Hash
				break
			}
		}

		for pathIdx, path := range state.Paths {
			if len(path) == 0 {
				continue
			}

			// Walk backwards to find first step where object has final hash
			lastWriteIdx := -1
			for i := len(path) - 1; i >= 0; i-- {
				step := path[i]
				for _, obj := range step.StateAfter {
					if obj.Key.String() == keyStr && obj.Hash == finalHash {
						lastWriteIdx = i
					}
				}
			}

			if lastWriteIdx >= 0 {
				step := path[lastWriteIdx]
				result.ByPath = append(result.ByPath, PathLastWrite{
					PathIndex: pathIdx,
					StateID:   state.ID,
					FinalHash: finalHash,
					LastWriteStep: LastWriteStep{
						StepIndex:    lastWriteIdx,
						ControllerId: step.ControllerID,
						StateBefore:  step.StateBefore,
					},
				})
			} else if stateIdx == 0 && pathIdx == 0 {
				// Object might exist in initial state, record that
				result.ByPath = append(result.ByPath, PathLastWrite{
					PathIndex: pathIdx,
					StateID:   state.ID,
					FinalHash: finalHash,
					LastWriteStep: LastWriteStep{
						StepIndex:    -1, // indicates initial state
						ControllerId: "(initial)",
					},
				})
			}
		}
	}

	return result
}
```

**Step 4: Run test to verify it passes**

```bash
go test ./pkg/analysis/... -run TestAnalyzeLastWrite -v
```

**Step 5: Add test comparing StateBefore across paths**

Add to `lastwrite_test.go`:
```go
func TestAnalyzeLastWrite_CapturesStateBefore(t *testing.T) {
	// The key insight: we want to see what the reconciler saw when it wrote
	dump := &Dump{
		States: []DumpResultState{{
			ID: "state1",
			State: DumpStateNode{Contents: DumpStateSnapshot{
				Objects: []DumpObjectVersion{
					{Key: testKey("Endpoints", "ns", "ep1"), Hash: testHash("final")},
				},
			}},
			Paths: [][]DumpReconcileResult{{
				{
					ControllerID: "EndpointsController",
					StateBefore: []DumpObjectVersion{
						{Key: testKey("Pod", "ns", "pod1"), Hash: testHash("pod-ready-false")},
					},
					StateAfter: []DumpObjectVersion{
						{Key: testKey("Endpoints", "ns", "ep1"), Hash: testHash("final")},
					},
				},
			}},
		}},
	}

	key := testKey("Endpoints", "ns", "ep1")
	result := AnalyzeLastWrite(dump, key)

	if len(result.ByPath[0].LastWriteStep.StateBefore) != 1 {
		t.Fatalf("expected 1 object in StateBefore")
	}
	podBefore := result.ByPath[0].LastWriteStep.StateBefore[0]
	if podBefore.Hash.Value != "pod-ready-false" {
		t.Errorf("expected pod-ready-false, got %s", podBefore.Hash.Value)
	}
}
```

**Step 6: Run all lastwrite tests**

```bash
go test ./pkg/analysis/... -run TestAnalyzeLastWrite -v
```

**Step 7: Add integration test with trial-1**

```go
func TestAnalyzeLastWrite_Trial1(t *testing.T) {
	dump, err := LoadDump("../../analysis/nondeterminism-verification/trials-study1-both/trial-1/dump.jsonl")
	if err != nil {
		t.Skipf("trial-1 dump not available: %v", err)
	}

	// First find differing objects
	diff := DiffConvergedStates(dump)
	if len(diff.DifferingObjects) == 0 {
		t.Skip("no differing objects found")
	}

	// Analyze last write for first differing object
	key := diff.DifferingObjects[0].Key
	result := AnalyzeLastWrite(dump, key)

	t.Logf("Object: %s", key.String())
	for _, pathResult := range result.ByPath {
		t.Logf("  Path %d (state %s): last write at step %d by %s",
			pathResult.PathIndex,
			pathResult.StateID,
			pathResult.LastWriteStep.StepIndex,
			pathResult.LastWriteStep.ControllerId)
	}
}
```

**Step 8: Run integration test**

```bash
go test ./pkg/analysis/... -run TestAnalyzeLastWrite_Trial1 -v
```

**Step 9: Commit**

```bash
git add pkg/analysis/lastwrite.go pkg/analysis/lastwrite_test.go
git commit -m "feat(analysis): add Module 1 - last write analysis"
```

---

## Task 4: Implement Module 2 - Object Lifecycle Analysis

**Files:**
- Create: `pkg/analysis/lifecycle.go`
- Create: `pkg/analysis/lifecycle_test.go`

**Step 1: Write test for finding hash appearances**

Create `pkg/analysis/lifecycle_test.go`:
```go
package analysis

import (
	"testing"
)

func TestAnalyzeObjectLifecycle_FindsAppearances(t *testing.T) {
	dump := &Dump{
		States: []DumpResultState{{
			ID: "state1",
			Paths: [][]DumpReconcileResult{{
				{
					ControllerID: "Controller1",
					StateAfter: []DumpObjectVersion{
						{Key: testKey("Pod", "ns", "pod1"), Hash: testHash("not-ready")},
					},
				},
				{
					ControllerID: "Controller2",
					StateAfter: []DumpObjectVersion{
						{Key: testKey("Pod", "ns", "pod1"), Hash: testHash("ready")},
					},
				},
				{
					ControllerID: "Controller3",
					StateAfter: []DumpObjectVersion{
						{Key: testKey("Pod", "ns", "pod1"), Hash: testHash("ready")},
					},
				},
			}},
		}},
	}

	key := testKey("Pod", "ns", "pod1")
	targetHash := testHash("ready")
	result := AnalyzeObjectLifecycle(dump, 0, 0, key, targetHash)

	if len(result.Appearances) != 2 {
		t.Fatalf("expected 2 appearances of 'ready' hash, got %d", len(result.Appearances))
	}
	if result.Appearances[0].StepIndex != 1 {
		t.Errorf("expected first appearance at step 1, got %d", result.Appearances[0].StepIndex)
	}
	if result.Appearances[0].ControllerId != "Controller2" {
		t.Errorf("expected Controller2, got %s", result.Appearances[0].ControllerId)
	}
}
```

**Step 2: Run test to verify it fails**

```bash
go test ./pkg/analysis/... -run TestAnalyzeObjectLifecycle -v
```

Expected: FAIL - `AnalyzeObjectLifecycle` not defined

**Step 3: Write minimal lifecycle.go implementation**

Create `pkg/analysis/lifecycle.go`:
```go
package analysis

import "github.com/tgoodwin/kamera/pkg/snapshot"

// ObjectLifecycleResult shows when an object had a specific hash value
type ObjectLifecycleResult struct {
	Object      snapshot.CompositeKey
	TargetHash  snapshot.VersionHash
	StateIndex  int
	PathIndex   int
	Found       bool
	Appearances []StepInfo
}

// StepInfo identifies a step in the exploration
type StepInfo struct {
	StepIndex    int
	ControllerId string
}

// AnalyzeObjectLifecycle finds all steps where an object had the target hash
func AnalyzeObjectLifecycle(dump *Dump, stateIdx, pathIdx int, key snapshot.CompositeKey, targetHash snapshot.VersionHash) *ObjectLifecycleResult {
	result := &ObjectLifecycleResult{
		Object:     key,
		TargetHash: targetHash,
		StateIndex: stateIdx,
		PathIndex:  pathIdx,
	}

	if dump == nil || stateIdx >= len(dump.States) {
		return result
	}

	state := dump.States[stateIdx]
	if pathIdx >= len(state.Paths) {
		return result
	}

	path := state.Paths[pathIdx]
	keyStr := key.String()

	for i, step := range path {
		for _, obj := range step.StateAfter {
			if obj.Key.String() == keyStr && obj.Hash == targetHash {
				result.Found = true
				result.Appearances = append(result.Appearances, StepInfo{
					StepIndex:    i,
					ControllerId: step.ControllerID,
				})
				break
			}
		}
	}

	return result
}
```

**Step 4: Run test to verify it passes**

```bash
go test ./pkg/analysis/... -run TestAnalyzeObjectLifecycle -v
```

**Step 5: Add test for hash not found**

```go
func TestAnalyzeObjectLifecycle_NotFound(t *testing.T) {
	dump := &Dump{
		States: []DumpResultState{{
			ID: "state1",
			Paths: [][]DumpReconcileResult{{
				{
					ControllerID: "Controller1",
					StateAfter: []DumpObjectVersion{
						{Key: testKey("Pod", "ns", "pod1"), Hash: testHash("not-ready")},
					},
				},
			}},
		}},
	}

	key := testKey("Pod", "ns", "pod1")
	targetHash := testHash("ready") // This hash never appears
	result := AnalyzeObjectLifecycle(dump, 0, 0, key, targetHash)

	if result.Found {
		t.Error("expected Found=false for non-existent hash")
	}
	if len(result.Appearances) != 0 {
		t.Errorf("expected 0 appearances, got %d", len(result.Appearances))
	}
}
```

**Step 6: Run all lifecycle tests**

```bash
go test ./pkg/analysis/... -run TestAnalyzeObjectLifecycle -v
```

**Step 7: Commit**

```bash
git add pkg/analysis/lifecycle.go pkg/analysis/lifecycle_test.go
git commit -m "feat(analysis): add Module 2 - object lifecycle analysis"
```

---

## Task 5: Add human-readable output formatters

**Files:**
- Create: `pkg/analysis/format.go`
- Create: `pkg/analysis/format_test.go`

**Step 1: Write test for diff formatter**

Create `pkg/analysis/format_test.go`:
```go
package analysis

import (
	"strings"
	"testing"
)

func TestFormatConvergedStateDiff(t *testing.T) {
	diff := &ConvergedStateDiff{
		NumStates: 2,
		DifferingObjects: []ObjectDiff{{
			Key: testKey("Endpoints", "ns", "ep1"),
			ByState: map[string]snapshot.VersionHash{
				"state1": testHash("hash-a"),
				"state2": testHash("hash-b"),
			},
		}},
		IdenticalCount: 5,
	}

	output := FormatConvergedStateDiff(diff)

	if !strings.Contains(output, "2 converged states") {
		t.Error("expected '2 converged states' in output")
	}
	if !strings.Contains(output, "1 differing") {
		t.Error("expected '1 differing' in output")
	}
	if !strings.Contains(output, "Endpoints") {
		t.Error("expected 'Endpoints' in output")
	}
}
```

**Step 2: Run test to verify it fails**

```bash
go test ./pkg/analysis/... -run TestFormat -v
```

**Step 3: Write format.go with formatters**

Create `pkg/analysis/format.go`:
```go
package analysis

import (
	"fmt"
	"strings"
)

// FormatConvergedStateDiff returns human-readable diff output
func FormatConvergedStateDiff(diff *ConvergedStateDiff) string {
	if diff == nil {
		return "No diff data"
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%d converged states with %d differing object(s), %d identical\n\n",
		diff.NumStates, len(diff.DifferingObjects), diff.IdenticalCount))

	for _, obj := range diff.DifferingObjects {
		sb.WriteString(fmt.Sprintf("  %s:\n", formatKey(obj.Key)))
		for stateID, hash := range obj.ByState {
			hashStr := hash.Value
			if hashStr == "" {
				hashStr = "(missing)"
			}
			sb.WriteString(fmt.Sprintf("    State %s: %s\n", stateID, hashStr))
		}
		sb.WriteString("\n")
	}

	return sb.String()
}

// FormatLastWriteAnalysis returns human-readable last-write output
func FormatLastWriteAnalysis(result *LastWriteAnalysis) string {
	if result == nil {
		return "No last-write data"
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Last write analysis for %s:\n\n", formatKey(result.Object)))

	for _, path := range result.ByPath {
		sb.WriteString(fmt.Sprintf("  Path %d (→ state %s):\n", path.PathIndex, path.StateID))
		if path.LastWriteStep.StepIndex < 0 {
			sb.WriteString("    Last write: initial state\n")
		} else {
			sb.WriteString(fmt.Sprintf("    Last write: step %d by %s\n",
				path.LastWriteStep.StepIndex, path.LastWriteStep.ControllerId))
		}
		sb.WriteString(fmt.Sprintf("    Final hash: %s\n", path.FinalHash.Value))
		sb.WriteString("\n")
	}

	return sb.String()
}

// FormatObjectLifecycle returns human-readable lifecycle output
func FormatObjectLifecycle(result *ObjectLifecycleResult) string {
	if result == nil {
		return "No lifecycle data"
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Object: %s\n", formatKey(result.Object)))
	sb.WriteString(fmt.Sprintf("Target hash: %s\n", result.TargetHash.Value))
	sb.WriteString(fmt.Sprintf("State %d, Path %d\n\n", result.StateIndex, result.PathIndex))

	if !result.Found {
		sb.WriteString("  Not found in this path\n")
	} else {
		sb.WriteString(fmt.Sprintf("  Found %d appearance(s):\n", len(result.Appearances)))
		for _, app := range result.Appearances {
			sb.WriteString(fmt.Sprintf("    - step %d: %s\n", app.StepIndex, app.ControllerId))
		}
	}

	return sb.String()
}

func formatKey(key snapshot.CompositeKey) string {
	return fmt.Sprintf("%s/%s/%s", key.ResourceKey.Kind, key.ResourceKey.Namespace, key.ResourceKey.Name)
}
```

**Step 4: Run tests**

```bash
go test ./pkg/analysis/... -run TestFormat -v
```

**Step 5: Commit**

```bash
git add pkg/analysis/format.go pkg/analysis/format_test.go
git commit -m "feat(analysis): add human-readable output formatters"
```

---

## Task 6: Create cmd/kamera-analyze CLI

**Files:**
- Create: `cmd/kamera-analyze/main.go`

**Step 1: Create CLI with subcommands**

Create `cmd/kamera-analyze/main.go`:
```go
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/tgoodwin/kamera/pkg/analysis"
)

func main() {
	if len(os.Args) < 3 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	dumpPath := os.Args[2]

	dump, err := analysis.LoadDump(dumpPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading dump: %v\n", err)
		os.Exit(1)
	}

	switch cmd {
	case "diff":
		runDiff(dump)
	case "report":
		runReport(dump)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: kamera-analyze <command> <dump.jsonl>")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  diff    Show differences between converged states")
	fmt.Println("  report  Full backward-trace analysis report")
}

func runDiff(dump *analysis.Dump) {
	diff := analysis.DiffConvergedStates(dump)
	fmt.Print(analysis.FormatConvergedStateDiff(diff))
}

func runReport(dump *analysis.Dump) {
	// Module 0: Diff
	diff := analysis.DiffConvergedStates(dump)
	fmt.Println("=== Converged State Diff ===")
	fmt.Print(analysis.FormatConvergedStateDiff(diff))

	if len(diff.DifferingObjects) == 0 {
		fmt.Println("No differing objects - states are identical")
		return
	}

	// Module 1: Last Write for each differing object
	fmt.Println("=== Last Write Analysis ===")
	for _, objDiff := range diff.DifferingObjects {
		result := analysis.AnalyzeLastWrite(dump, objDiff.Key)
		fmt.Print(analysis.FormatLastWriteAnalysis(result))
	}
}
```

**Step 2: Build and test**

```bash
go build -o bin/kamera-analyze ./cmd/kamera-analyze
./bin/kamera-analyze diff analysis/nondeterminism-verification/trials-study1-both/trial-1/dump.jsonl
```

**Step 3: Commit**

```bash
git add cmd/kamera-analyze/
git commit -m "feat: add kamera-analyze CLI tool"
```

---

## Task 7: End-to-end validation with trial-1

**Step 1: Run full report on trial-1**

```bash
./bin/kamera-analyze report analysis/nondeterminism-verification/trials-study1-both/trial-1/dump.jsonl
```

**Step 2: Verify output identifies Endpoints divergence**

Expected output should show:
- 2 converged states
- Endpoints as a differing object
- Different last-write steps and controllers for each path

**Step 3: Document results**

Update `analysis/nondeterminism-analysis-summary.md` with tool output.

**Step 4: Final commit**

```bash
git add -A
git commit -m "docs: validate backward-trace analysis with trial-1 data"
```

---

## Summary

| Task | Description | Est. Steps |
|------|-------------|------------|
| 1 | Create pkg/analysis with dump types | 6 |
| 2 | Module 0 - Converged State Diff | 9 |
| 3 | Module 1 - Last Write Analysis | 9 |
| 4 | Module 2 - Object Lifecycle | 7 |
| 5 | Human-readable formatters | 5 |
| 6 | CLI tool | 3 |
| 7 | End-to-end validation | 4 |

Total: ~43 steps

## Files Created/Modified

**Created:**
- `pkg/analysis/types.go`
- `pkg/analysis/dump.go`
- `pkg/analysis/diff.go`
- `pkg/analysis/diff_test.go`
- `pkg/analysis/lastwrite.go`
- `pkg/analysis/lastwrite_test.go`
- `pkg/analysis/lifecycle.go`
- `pkg/analysis/lifecycle_test.go`
- `pkg/analysis/format.go`
- `pkg/analysis/format_test.go`
- `cmd/kamera-analyze/main.go`

**Modified:**
- `pkg/interactive/inspector_dump.go`
