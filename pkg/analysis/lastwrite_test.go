package analysis

import (
	"testing"
)

func TestAnalyzeLastWrite_FindsLastWrite(t *testing.T) {
	// Object changes hash1 -> hash2 -> hash3, should find step with hash3
	targetKey := testKey("ConfigMap", "default", "config")

	dump := &Dump{
		States: []DumpResultState{
			{
				ID: "state-1",
				State: DumpStateNode{
					Contents: DumpStateSnapshot{
						Objects: []DumpObjectVersion{
							{Key: targetKey, Hash: testHash("hash3")}, // final hash
						},
					},
				},
				Paths: [][]DumpReconcileResult{
					{
						// Step 0: object goes from nothing to hash1
						{
							ControllerID: "controller-a",
							StateAfter: []DumpObjectVersion{
								{Key: targetKey, Hash: testHash("hash1")},
							},
							StateBefore: []DumpObjectVersion{},
						},
						// Step 1: object goes from hash1 to hash2
						{
							ControllerID: "controller-b",
							StateAfter: []DumpObjectVersion{
								{Key: targetKey, Hash: testHash("hash2")},
							},
							StateBefore: []DumpObjectVersion{
								{Key: targetKey, Hash: testHash("hash1")},
							},
						},
						// Step 2: object goes from hash2 to hash3 (final)
						{
							ControllerID: "controller-c",
							StateAfter: []DumpObjectVersion{
								{Key: targetKey, Hash: testHash("hash3")},
							},
							StateBefore: []DumpObjectVersion{
								{Key: targetKey, Hash: testHash("hash2")},
							},
						},
					},
				},
			},
		},
	}

	result := AnalyzeLastWrite(dump, targetKey)

	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.Object != targetKey {
		t.Errorf("expected Object=%v, got %v", targetKey, result.Object)
	}
	if len(result.ByPath) != 1 {
		t.Fatalf("expected 1 path result, got %d", len(result.ByPath))
	}

	pathResult := result.ByPath[0]
	if pathResult.PathIndex != 0 {
		t.Errorf("expected PathIndex=0, got %d", pathResult.PathIndex)
	}
	if pathResult.StateID != "state-1" {
		t.Errorf("expected StateID='state-1', got '%s'", pathResult.StateID)
	}
	if pathResult.FinalHash.Value != "hash3" {
		t.Errorf("expected FinalHash.Value='hash3', got '%s'", pathResult.FinalHash.Value)
	}
	if pathResult.LastWriteStep.StepIndex != 2 {
		t.Errorf("expected LastWriteStep.StepIndex=2, got %d", pathResult.LastWriteStep.StepIndex)
	}
	if pathResult.LastWriteStep.ControllerId != "controller-c" {
		t.Errorf("expected LastWriteStep.ControllerId='controller-c', got '%s'", pathResult.LastWriteStep.ControllerId)
	}
}

func TestAnalyzeLastWrite_CapturesStateBefore(t *testing.T) {
	// Verify that StateBefore is captured correctly from the last write step
	targetKey := testKey("Deployment", "default", "app")
	otherKey := testKey("ConfigMap", "default", "config")

	dump := &Dump{
		States: []DumpResultState{
			{
				ID: "state-1",
				State: DumpStateNode{
					Contents: DumpStateSnapshot{
						Objects: []DumpObjectVersion{
							{Key: targetKey, Hash: testHash("final-hash")},
							{Key: otherKey, Hash: testHash("config-hash")},
						},
					},
				},
				Paths: [][]DumpReconcileResult{
					{
						// Step 0: initial state
						{
							ControllerID: "controller-a",
							StateAfter: []DumpObjectVersion{
								{Key: targetKey, Hash: testHash("initial-hash")},
								{Key: otherKey, Hash: testHash("config-hash")},
							},
							StateBefore: []DumpObjectVersion{},
						},
						// Step 1: last write step - this is what we want to capture
						{
							ControllerID: "controller-b",
							StateAfter: []DumpObjectVersion{
								{Key: targetKey, Hash: testHash("final-hash")},
								{Key: otherKey, Hash: testHash("config-hash")},
							},
							StateBefore: []DumpObjectVersion{
								{Key: targetKey, Hash: testHash("initial-hash")},
								{Key: otherKey, Hash: testHash("config-hash")},
							},
						},
					},
				},
			},
		},
	}

	result := AnalyzeLastWrite(dump, targetKey)

	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if len(result.ByPath) != 1 {
		t.Fatalf("expected 1 path result, got %d", len(result.ByPath))
	}

	pathResult := result.ByPath[0]

	// Verify StateBefore was captured from the last write step
	stateBefore := pathResult.LastWriteStep.StateBefore
	if len(stateBefore) != 2 {
		t.Fatalf("expected StateBefore to have 2 objects, got %d", len(stateBefore))
	}

	// Find the target object in StateBefore
	var foundTarget, foundOther bool
	for _, obj := range stateBefore {
		if obj.Key == targetKey {
			foundTarget = true
			if obj.Hash.Value != "initial-hash" {
				t.Errorf("expected target object in StateBefore to have hash 'initial-hash', got '%s'", obj.Hash.Value)
			}
		}
		if obj.Key == otherKey {
			foundOther = true
			if obj.Hash.Value != "config-hash" {
				t.Errorf("expected other object in StateBefore to have hash 'config-hash', got '%s'", obj.Hash.Value)
			}
		}
	}

	if !foundTarget {
		t.Error("target object not found in StateBefore")
	}
	if !foundOther {
		t.Error("other object not found in StateBefore")
	}
}

func TestAnalyzeLastWrite_Trial1(t *testing.T) {
	// Integration test using DiffConvergedStates to find a differing object,
	// then analyze its last write
	dumpPath := "../../analysis/nondeterminism-verification/trials-study1-both/trial-1/dump.jsonl"

	dump, err := LoadDump(dumpPath)
	if err != nil {
		t.Skipf("skipping integration test: could not load dump file: %v", err)
	}

	// First, find a differing object using DiffConvergedStates
	diff := DiffConvergedStates(dump)
	if diff == nil {
		t.Fatal("expected non-nil diff result")
	}

	t.Logf("NumStates: %d", diff.NumStates)
	t.Logf("DifferingObjects: %d", len(diff.DifferingObjects))

	if len(diff.DifferingObjects) == 0 {
		t.Skip("no differing objects found in trial-1 dump, skipping last write analysis")
	}

	// Take the first differing object and analyze its last write
	differingObj := diff.DifferingObjects[0]
	t.Logf("Analyzing last write for: %s/%s/%s",
		differingObj.Key.ResourceKey.Kind,
		differingObj.Key.ResourceKey.Namespace,
		differingObj.Key.ResourceKey.Name)

	result := AnalyzeLastWrite(dump, differingObj.Key)

	if result == nil {
		t.Fatal("expected non-nil last write analysis result")
	}

	t.Logf("Found %d path results", len(result.ByPath))

	// Log details for inspection
	for i, pathResult := range result.ByPath {
		t.Logf("Path %d:", i)
		t.Logf("  PathIndex: %d", pathResult.PathIndex)
		t.Logf("  StateID: %s", pathResult.StateID)
		hashVal := pathResult.FinalHash.Value
		if len(hashVal) > 16 {
			hashVal = hashVal[:16] + "..."
		}
		t.Logf("  FinalHash: %s", hashVal)
		t.Logf("  LastWriteStep.StepIndex: %d", pathResult.LastWriteStep.StepIndex)
		t.Logf("  LastWriteStep.ControllerId: %s", pathResult.LastWriteStep.ControllerId)
		t.Logf("  LastWriteStep.StateBefore count: %d", len(pathResult.LastWriteStep.StateBefore))
	}

	// Basic validation: we should have found at least one path with a last write
	if len(result.ByPath) == 0 {
		t.Error("expected at least one path with last write information")
	}
}

func TestAnalyzeLastWrite_NilDump(t *testing.T) {
	key := testKey("ConfigMap", "default", "config")
	result := AnalyzeLastWrite(nil, key)

	if result == nil {
		t.Fatal("expected non-nil result for nil dump")
	}
	if result.Object != key {
		t.Errorf("expected Object=%v, got %v", key, result.Object)
	}
	if result.ByPath != nil {
		t.Errorf("expected nil ByPath for nil dump, got %v", result.ByPath)
	}
}

func TestAnalyzeLastWrite_ObjectNotInState(t *testing.T) {
	// Object we're looking for is not in the converged state
	targetKey := testKey("ConfigMap", "default", "missing")
	existingKey := testKey("ConfigMap", "default", "existing")

	dump := &Dump{
		States: []DumpResultState{
			{
				ID: "state-1",
				State: DumpStateNode{
					Contents: DumpStateSnapshot{
						Objects: []DumpObjectVersion{
							{Key: existingKey, Hash: testHash("hash1")}, // different object
						},
					},
				},
				Paths: [][]DumpReconcileResult{
					{
						{
							ControllerID: "controller-a",
							StateAfter: []DumpObjectVersion{
								{Key: existingKey, Hash: testHash("hash1")},
							},
							StateBefore: []DumpObjectVersion{},
						},
					},
				},
			},
		},
	}

	result := AnalyzeLastWrite(dump, targetKey)

	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if len(result.ByPath) != 0 {
		t.Errorf("expected no path results for missing object, got %d", len(result.ByPath))
	}
}

func TestAnalyzeLastWrite_MultiplePaths(t *testing.T) {
	// Test with multiple paths that might have different last writers
	targetKey := testKey("Endpoints", "default", "svc")

	dump := &Dump{
		States: []DumpResultState{
			{
				ID: "state-1",
				State: DumpStateNode{
					Contents: DumpStateSnapshot{
						Objects: []DumpObjectVersion{
							{Key: targetKey, Hash: testHash("final-hash")},
						},
					},
				},
				Paths: [][]DumpReconcileResult{
					// Path 0: controller-a is last writer
					{
						{
							ControllerID: "controller-a",
							StateAfter: []DumpObjectVersion{
								{Key: targetKey, Hash: testHash("final-hash")},
							},
							StateBefore: []DumpObjectVersion{},
						},
					},
					// Path 1: controller-b is last writer
					{
						{
							ControllerID: "controller-a",
							StateAfter: []DumpObjectVersion{
								{Key: targetKey, Hash: testHash("intermediate")},
							},
							StateBefore: []DumpObjectVersion{},
						},
						{
							ControllerID: "controller-b",
							StateAfter: []DumpObjectVersion{
								{Key: targetKey, Hash: testHash("final-hash")},
							},
							StateBefore: []DumpObjectVersion{
								{Key: targetKey, Hash: testHash("intermediate")},
							},
						},
					},
				},
			},
		},
	}

	result := AnalyzeLastWrite(dump, targetKey)

	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if len(result.ByPath) != 2 {
		t.Fatalf("expected 2 path results, got %d", len(result.ByPath))
	}

	// Path 0 should have controller-a as last writer at step 0
	if result.ByPath[0].PathIndex != 0 {
		t.Errorf("expected first result PathIndex=0, got %d", result.ByPath[0].PathIndex)
	}
	if result.ByPath[0].LastWriteStep.ControllerId != "controller-a" {
		t.Errorf("expected first result ControllerId='controller-a', got '%s'", result.ByPath[0].LastWriteStep.ControllerId)
	}
	if result.ByPath[0].LastWriteStep.StepIndex != 0 {
		t.Errorf("expected first result StepIndex=0, got %d", result.ByPath[0].LastWriteStep.StepIndex)
	}

	// Path 1 should have controller-b as last writer at step 1
	if result.ByPath[1].PathIndex != 1 {
		t.Errorf("expected second result PathIndex=1, got %d", result.ByPath[1].PathIndex)
	}
	if result.ByPath[1].LastWriteStep.ControllerId != "controller-b" {
		t.Errorf("expected second result ControllerId='controller-b', got '%s'", result.ByPath[1].LastWriteStep.ControllerId)
	}
	if result.ByPath[1].LastWriteStep.StepIndex != 1 {
		t.Errorf("expected second result StepIndex=1, got %d", result.ByPath[1].LastWriteStep.StepIndex)
	}
}
