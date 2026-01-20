package analysis

import (
	"testing"
)

func TestAnalyzeObjectLifecycle_FindsAppearances(t *testing.T) {
	// Object takes on target hash at multiple steps
	targetKey := testKey("ConfigMap", "default", "config")

	dump := &Dump{
		States: []DumpResultState{
			{
				ID: "state-1",
				State: DumpStateNode{
					Contents: DumpStateSnapshot{
						Objects: []DumpObjectVersion{
							{Key: targetKey, Hash: testHash("hash3")},
						},
					},
				},
				Paths: [][]DumpReconcileResult{
					{
						// Step 0: object has hash1
						{
							ControllerID: "controller-a",
							StateAfter: []DumpObjectVersion{
								{Key: targetKey, Hash: testHash("hash1")},
							},
							StateBefore: []DumpObjectVersion{},
						},
						// Step 1: object has hash2 (target)
						{
							ControllerID: "controller-b",
							StateAfter: []DumpObjectVersion{
								{Key: targetKey, Hash: testHash("hash2")},
							},
							StateBefore: []DumpObjectVersion{
								{Key: targetKey, Hash: testHash("hash1")},
							},
						},
						// Step 2: object has hash3
						{
							ControllerID: "controller-c",
							StateAfter: []DumpObjectVersion{
								{Key: targetKey, Hash: testHash("hash3")},
							},
							StateBefore: []DumpObjectVersion{
								{Key: targetKey, Hash: testHash("hash2")},
							},
						},
						// Step 3: object back to hash2 (target appears again)
						{
							ControllerID: "controller-d",
							StateAfter: []DumpObjectVersion{
								{Key: targetKey, Hash: testHash("hash2")},
							},
							StateBefore: []DumpObjectVersion{
								{Key: targetKey, Hash: testHash("hash3")},
							},
						},
					},
				},
			},
		},
	}

	result := AnalyzeObjectLifecycle(dump, 0, 0, targetKey, testHash("hash2"))

	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.Object != targetKey {
		t.Errorf("expected Object=%v, got %v", targetKey, result.Object)
	}
	if result.TargetHash.Value != "hash2" {
		t.Errorf("expected TargetHash.Value='hash2', got '%s'", result.TargetHash.Value)
	}
	if result.StateIndex != 0 {
		t.Errorf("expected StateIndex=0, got %d", result.StateIndex)
	}
	if result.PathIndex != 0 {
		t.Errorf("expected PathIndex=0, got %d", result.PathIndex)
	}
	if !result.Found {
		t.Error("expected Found=true")
	}
	if len(result.Appearances) != 2 {
		t.Fatalf("expected 2 appearances, got %d", len(result.Appearances))
	}

	// Check first appearance
	if result.Appearances[0].StepIndex != 1 {
		t.Errorf("expected first appearance StepIndex=1, got %d", result.Appearances[0].StepIndex)
	}
	if result.Appearances[0].ControllerId != "controller-b" {
		t.Errorf("expected first appearance ControllerId='controller-b', got '%s'", result.Appearances[0].ControllerId)
	}

	// Check second appearance
	if result.Appearances[1].StepIndex != 3 {
		t.Errorf("expected second appearance StepIndex=3, got %d", result.Appearances[1].StepIndex)
	}
	if result.Appearances[1].ControllerId != "controller-d" {
		t.Errorf("expected second appearance ControllerId='controller-d', got '%s'", result.Appearances[1].ControllerId)
	}
}

func TestAnalyzeObjectLifecycle_NotFound(t *testing.T) {
	// Target hash never appears in the path
	targetKey := testKey("ConfigMap", "default", "config")

	dump := &Dump{
		States: []DumpResultState{
			{
				ID: "state-1",
				State: DumpStateNode{
					Contents: DumpStateSnapshot{
						Objects: []DumpObjectVersion{
							{Key: targetKey, Hash: testHash("hash3")},
						},
					},
				},
				Paths: [][]DumpReconcileResult{
					{
						// Step 0: object has hash1
						{
							ControllerID: "controller-a",
							StateAfter: []DumpObjectVersion{
								{Key: targetKey, Hash: testHash("hash1")},
							},
							StateBefore: []DumpObjectVersion{},
						},
						// Step 1: object has hash2
						{
							ControllerID: "controller-b",
							StateAfter: []DumpObjectVersion{
								{Key: targetKey, Hash: testHash("hash2")},
							},
							StateBefore: []DumpObjectVersion{
								{Key: targetKey, Hash: testHash("hash1")},
							},
						},
						// Step 2: object has hash3
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

	// Search for hash that never appears
	result := AnalyzeObjectLifecycle(dump, 0, 0, targetKey, testHash("nonexistent-hash"))

	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.Object != targetKey {
		t.Errorf("expected Object=%v, got %v", targetKey, result.Object)
	}
	if result.TargetHash.Value != "nonexistent-hash" {
		t.Errorf("expected TargetHash.Value='nonexistent-hash', got '%s'", result.TargetHash.Value)
	}
	if result.Found {
		t.Error("expected Found=false")
	}
	if len(result.Appearances) != 0 {
		t.Errorf("expected 0 appearances, got %d", len(result.Appearances))
	}
}

func TestAnalyzeObjectLifecycle_NilDump(t *testing.T) {
	targetKey := testKey("ConfigMap", "default", "config")
	result := AnalyzeObjectLifecycle(nil, 0, 0, targetKey, testHash("hash1"))

	if result == nil {
		t.Fatal("expected non-nil result for nil dump")
	}
	if result.Object != targetKey {
		t.Errorf("expected Object=%v, got %v", targetKey, result.Object)
	}
	if result.Found {
		t.Error("expected Found=false for nil dump")
	}
	if len(result.Appearances) != 0 {
		t.Errorf("expected 0 appearances for nil dump, got %d", len(result.Appearances))
	}
}

func TestAnalyzeObjectLifecycle_InvalidStateIndex(t *testing.T) {
	targetKey := testKey("ConfigMap", "default", "config")

	dump := &Dump{
		States: []DumpResultState{
			{
				ID: "state-1",
				Paths: [][]DumpReconcileResult{
					{},
				},
			},
		},
	}

	// Test with out-of-bounds state index
	result := AnalyzeObjectLifecycle(dump, 5, 0, targetKey, testHash("hash1"))

	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.Found {
		t.Error("expected Found=false for invalid state index")
	}
	if len(result.Appearances) != 0 {
		t.Errorf("expected 0 appearances, got %d", len(result.Appearances))
	}
}

func TestAnalyzeObjectLifecycle_InvalidPathIndex(t *testing.T) {
	targetKey := testKey("ConfigMap", "default", "config")

	dump := &Dump{
		States: []DumpResultState{
			{
				ID: "state-1",
				Paths: [][]DumpReconcileResult{
					{},
				},
			},
		},
	}

	// Test with out-of-bounds path index
	result := AnalyzeObjectLifecycle(dump, 0, 5, targetKey, testHash("hash1"))

	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.Found {
		t.Error("expected Found=false for invalid path index")
	}
	if len(result.Appearances) != 0 {
		t.Errorf("expected 0 appearances, got %d", len(result.Appearances))
	}
}

func TestAnalyzeObjectLifecycle_SingleAppearance(t *testing.T) {
	// Object takes on target hash at exactly one step
	targetKey := testKey("Deployment", "default", "app")

	dump := &Dump{
		States: []DumpResultState{
			{
				ID: "state-1",
				State: DumpStateNode{
					Contents: DumpStateSnapshot{
						Objects: []DumpObjectVersion{
							{Key: targetKey, Hash: testHash("final")},
						},
					},
				},
				Paths: [][]DumpReconcileResult{
					{
						// Step 0: object has hash1
						{
							ControllerID: "deployment-controller",
							StateAfter: []DumpObjectVersion{
								{Key: targetKey, Hash: testHash("initial")},
							},
							StateBefore: []DumpObjectVersion{},
						},
						// Step 1: object has target hash
						{
							ControllerID: "replicaset-controller",
							StateAfter: []DumpObjectVersion{
								{Key: targetKey, Hash: testHash("target")},
							},
							StateBefore: []DumpObjectVersion{
								{Key: targetKey, Hash: testHash("initial")},
							},
						},
						// Step 2: object has different hash
						{
							ControllerID: "deployment-controller",
							StateAfter: []DumpObjectVersion{
								{Key: targetKey, Hash: testHash("final")},
							},
							StateBefore: []DumpObjectVersion{
								{Key: targetKey, Hash: testHash("target")},
							},
						},
					},
				},
			},
		},
	}

	result := AnalyzeObjectLifecycle(dump, 0, 0, targetKey, testHash("target"))

	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if !result.Found {
		t.Error("expected Found=true")
	}
	if len(result.Appearances) != 1 {
		t.Fatalf("expected 1 appearance, got %d", len(result.Appearances))
	}
	if result.Appearances[0].StepIndex != 1 {
		t.Errorf("expected StepIndex=1, got %d", result.Appearances[0].StepIndex)
	}
	if result.Appearances[0].ControllerId != "replicaset-controller" {
		t.Errorf("expected ControllerId='replicaset-controller', got '%s'", result.Appearances[0].ControllerId)
	}
}
