package analysis

import (
	"testing"

	"github.com/tgoodwin/kamera/pkg/snapshot"
)

// Test helpers
func testKey(kind, ns, name string) snapshot.CompositeKey {
	return snapshot.NewCompositeKey(kind, ns, name, "")
}

func testHash(val string) snapshot.VersionHash {
	return snapshot.VersionHash{Value: val, Strategy: "test"}
}

func TestDiffConvergedStates_NilDump(t *testing.T) {
	result := DiffConvergedStates(nil)
	if result == nil {
		t.Fatal("expected non-nil result for nil dump")
	}
	if result.NumStates != 0 {
		t.Errorf("expected NumStates=0, got %d", result.NumStates)
	}
	if len(result.DifferingObjects) != 0 {
		t.Errorf("expected empty DifferingObjects, got %d", len(result.DifferingObjects))
	}
}

func TestDiffConvergedStates_SingleState(t *testing.T) {
	dump := &Dump{
		States: []DumpResultState{
			{
				ID: "state-1",
				State: DumpStateNode{
					Contents: DumpStateSnapshot{
						Objects: []DumpObjectVersion{
							{Key: testKey("ConfigMap", "default", "cm1"), Hash: testHash("hash1")},
							{Key: testKey("Secret", "default", "secret1"), Hash: testHash("hash2")},
						},
					},
				},
			},
		},
	}

	result := DiffConvergedStates(dump)
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.NumStates != 1 {
		t.Errorf("expected NumStates=1, got %d", result.NumStates)
	}
	if len(result.DifferingObjects) != 0 {
		t.Errorf("expected empty DifferingObjects for single state, got %d", len(result.DifferingObjects))
	}
	// Single state means no comparison possible, so IdenticalCount should be 0
	if result.IdenticalCount != 0 {
		t.Errorf("expected IdenticalCount=0 for single state, got %d", result.IdenticalCount)
	}
}

func TestDiffConvergedStates_TwoStatesOneDiff(t *testing.T) {
	// Two states where most objects are identical, but Endpoints differs
	dump := &Dump{
		States: []DumpResultState{
			{
				ID: "state-1",
				State: DumpStateNode{
					Contents: DumpStateSnapshot{
						Objects: []DumpObjectVersion{
							{Key: testKey("ConfigMap", "default", "cm1"), Hash: testHash("hash-cm")},
							{Key: testKey("Service", "default", "svc1"), Hash: testHash("hash-svc")},
							{Key: testKey("Endpoints", "default", "ep1"), Hash: testHash("hash-ep-v1")},
						},
					},
				},
			},
			{
				ID: "state-2",
				State: DumpStateNode{
					Contents: DumpStateSnapshot{
						Objects: []DumpObjectVersion{
							{Key: testKey("ConfigMap", "default", "cm1"), Hash: testHash("hash-cm")},
							{Key: testKey("Service", "default", "svc1"), Hash: testHash("hash-svc")},
							{Key: testKey("Endpoints", "default", "ep1"), Hash: testHash("hash-ep-v2")}, // Different!
						},
					},
				},
			},
		},
	}

	result := DiffConvergedStates(dump)
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.NumStates != 2 {
		t.Errorf("expected NumStates=2, got %d", result.NumStates)
	}
	if result.IdenticalCount != 2 {
		t.Errorf("expected IdenticalCount=2 (ConfigMap and Service), got %d", result.IdenticalCount)
	}
	if len(result.DifferingObjects) != 1 {
		t.Fatalf("expected 1 differing object (Endpoints), got %d", len(result.DifferingObjects))
	}

	// Verify the differing object is Endpoints
	diff := result.DifferingObjects[0]
	if diff.Key.ResourceKey.Kind != "Endpoints" {
		t.Errorf("expected differing object to be Endpoints, got %s", diff.Key.ResourceKey.Kind)
	}
	if diff.Key.ResourceKey.Name != "ep1" {
		t.Errorf("expected differing object name to be 'ep1', got %s", diff.Key.ResourceKey.Name)
	}

	// Verify the hashes per state
	if hash, ok := diff.ByState["state-1"]; !ok || hash.Value != "hash-ep-v1" {
		t.Errorf("expected state-1 hash to be 'hash-ep-v1', got %v", hash)
	}
	if hash, ok := diff.ByState["state-2"]; !ok || hash.Value != "hash-ep-v2" {
		t.Errorf("expected state-2 hash to be 'hash-ep-v2', got %v", hash)
	}
}

func TestDiffConvergedStates_ObjectMissingInOneState(t *testing.T) {
	// State 2 is missing an object that exists in State 1
	dump := &Dump{
		States: []DumpResultState{
			{
				ID: "state-1",
				State: DumpStateNode{
					Contents: DumpStateSnapshot{
						Objects: []DumpObjectVersion{
							{Key: testKey("ConfigMap", "default", "cm1"), Hash: testHash("hash-cm")},
							{Key: testKey("Secret", "default", "secret1"), Hash: testHash("hash-secret")},
						},
					},
				},
			},
			{
				ID: "state-2",
				State: DumpStateNode{
					Contents: DumpStateSnapshot{
						Objects: []DumpObjectVersion{
							{Key: testKey("ConfigMap", "default", "cm1"), Hash: testHash("hash-cm")},
							// Secret is missing in state-2
						},
					},
				},
			},
		},
	}

	result := DiffConvergedStates(dump)
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.NumStates != 2 {
		t.Errorf("expected NumStates=2, got %d", result.NumStates)
	}
	if result.IdenticalCount != 1 {
		t.Errorf("expected IdenticalCount=1 (ConfigMap), got %d", result.IdenticalCount)
	}
	if len(result.DifferingObjects) != 1 {
		t.Fatalf("expected 1 differing object (Secret), got %d", len(result.DifferingObjects))
	}

	// Verify the differing object is Secret
	diff := result.DifferingObjects[0]
	if diff.Key.ResourceKey.Kind != "Secret" {
		t.Errorf("expected differing object to be Secret, got %s", diff.Key.ResourceKey.Kind)
	}

	// State-1 should have the hash, state-2 should have empty hash
	if hash, ok := diff.ByState["state-1"]; !ok || hash.Value != "hash-secret" {
		t.Errorf("expected state-1 hash to be 'hash-secret', got %v", hash)
	}
	if hash, ok := diff.ByState["state-2"]; !ok || hash.Value != "" {
		t.Errorf("expected state-2 hash to be empty (missing), got %v", hash)
	}
}

func TestDiffConvergedStates_AllIdentical(t *testing.T) {
	// Two states with identical objects
	dump := &Dump{
		States: []DumpResultState{
			{
				ID: "state-1",
				State: DumpStateNode{
					Contents: DumpStateSnapshot{
						Objects: []DumpObjectVersion{
							{Key: testKey("ConfigMap", "default", "cm1"), Hash: testHash("hash-cm")},
							{Key: testKey("Service", "default", "svc1"), Hash: testHash("hash-svc")},
						},
					},
				},
			},
			{
				ID: "state-2",
				State: DumpStateNode{
					Contents: DumpStateSnapshot{
						Objects: []DumpObjectVersion{
							{Key: testKey("ConfigMap", "default", "cm1"), Hash: testHash("hash-cm")},
							{Key: testKey("Service", "default", "svc1"), Hash: testHash("hash-svc")},
						},
					},
				},
			},
		},
	}

	result := DiffConvergedStates(dump)
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.NumStates != 2 {
		t.Errorf("expected NumStates=2, got %d", result.NumStates)
	}
	if result.IdenticalCount != 2 {
		t.Errorf("expected IdenticalCount=2, got %d", result.IdenticalCount)
	}
	if len(result.DifferingObjects) != 0 {
		t.Errorf("expected no differing objects, got %d", len(result.DifferingObjects))
	}
}

func TestDiffConvergedStates_ThreeStates(t *testing.T) {
	// Three states where an object differs in one state
	dump := &Dump{
		States: []DumpResultState{
			{
				ID: "state-1",
				State: DumpStateNode{
					Contents: DumpStateSnapshot{
						Objects: []DumpObjectVersion{
							{Key: testKey("Deployment", "default", "app"), Hash: testHash("hash-v1")},
						},
					},
				},
			},
			{
				ID: "state-2",
				State: DumpStateNode{
					Contents: DumpStateSnapshot{
						Objects: []DumpObjectVersion{
							{Key: testKey("Deployment", "default", "app"), Hash: testHash("hash-v1")},
						},
					},
				},
			},
			{
				ID: "state-3",
				State: DumpStateNode{
					Contents: DumpStateSnapshot{
						Objects: []DumpObjectVersion{
							{Key: testKey("Deployment", "default", "app"), Hash: testHash("hash-v2")}, // Different!
						},
					},
				},
			},
		},
	}

	result := DiffConvergedStates(dump)
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.NumStates != 3 {
		t.Errorf("expected NumStates=3, got %d", result.NumStates)
	}
	if len(result.DifferingObjects) != 1 {
		t.Fatalf("expected 1 differing object, got %d", len(result.DifferingObjects))
	}
	if result.IdenticalCount != 0 {
		t.Errorf("expected IdenticalCount=0, got %d", result.IdenticalCount)
	}

	// Verify all three states are in the diff
	diff := result.DifferingObjects[0]
	if len(diff.ByState) != 3 {
		t.Errorf("expected 3 states in diff, got %d", len(diff.ByState))
	}
}

func TestDiffConvergedStates_Trial1(t *testing.T) {
	// Integration test with real dump file
	dumpPath := "../../analysis/nondeterminism-verification/trials-study1-both/trial-1/dump.jsonl"

	dump, err := LoadDump(dumpPath)
	if err != nil {
		t.Skipf("skipping integration test: could not load dump file: %v", err)
	}

	result := DiffConvergedStates(dump)
	if result == nil {
		t.Fatal("expected non-nil result")
	}

	// Log the results for inspection
	t.Logf("NumStates: %d", result.NumStates)
	t.Logf("IdenticalCount: %d", result.IdenticalCount)
	t.Logf("DifferingObjects: %d", len(result.DifferingObjects))

	// Basic sanity checks
	if result.NumStates < 2 {
		t.Errorf("expected at least 2 states in trial-1 dump, got %d", result.NumStates)
	}

	// Log details of differing objects for debugging
	for i, diff := range result.DifferingObjects {
		t.Logf("Diff %d: %s/%s/%s", i+1, diff.Key.ResourceKey.Kind, diff.Key.ResourceKey.Namespace, diff.Key.ResourceKey.Name)
		for stateID, hash := range diff.ByState {
			hashVal := hash.Value
			if hashVal == "" {
				hashVal = "(missing)"
			} else if len(hashVal) > 16 {
				hashVal = hashVal[:16] + "..."
			}
			t.Logf("  %s: %s", stateID, hashVal)
		}
	}
}
