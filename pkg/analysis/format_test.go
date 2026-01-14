package analysis

import (
	"strings"
	"testing"

	"github.com/tgoodwin/kamera/pkg/snapshot"
)

func TestFormatConvergedStateDiff_Nil(t *testing.T) {
	result := FormatConvergedStateDiff(nil)
	if result != "No diff available" {
		t.Errorf("expected 'No diff available', got %q", result)
	}
}

func TestFormatConvergedStateDiff_Empty(t *testing.T) {
	diff := &ConvergedStateDiff{
		NumStates:        2,
		DifferingObjects: nil,
		IdenticalCount:   5,
	}

	result := FormatConvergedStateDiff(diff)

	// Verify header contains key information
	if !strings.Contains(result, "2 converged states") {
		t.Errorf("expected '2 converged states' in output, got %q", result)
	}
	if !strings.Contains(result, "0 differing object(s)") {
		t.Errorf("expected '0 differing object(s)' in output, got %q", result)
	}
	if !strings.Contains(result, "5 identical") {
		t.Errorf("expected '5 identical' in output, got %q", result)
	}
}

func TestFormatConvergedStateDiff_WithDiffs(t *testing.T) {
	diff := &ConvergedStateDiff{
		NumStates: 2,
		DifferingObjects: []ObjectDiff{
			{
				Key: testKey("Endpoints", "ns", "ep1"),
				ByState: map[string]snapshot.VersionHash{
					"state1": testHash("abc1234567890"),
					"state2": testHash("def4567890123"),
				},
			},
		},
		IdenticalCount: 5,
	}

	result := FormatConvergedStateDiff(diff)

	// Verify header
	if !strings.Contains(result, "2 converged states with 1 differing object(s), 5 identical") {
		t.Errorf("header mismatch in output: %q", result)
	}

	// Verify object key is formatted correctly
	if !strings.Contains(result, "Endpoints/ns/ep1") {
		t.Errorf("expected 'Endpoints/ns/ep1' in output, got %q", result)
	}

	// Verify state information is present
	if !strings.Contains(result, "State state1:") {
		t.Errorf("expected 'State state1:' in output, got %q", result)
	}
	if !strings.Contains(result, "State state2:") {
		t.Errorf("expected 'State state2:' in output, got %q", result)
	}

	// Verify hashes are shortened
	if !strings.Contains(result, "abc1234") {
		t.Errorf("expected shortened hash 'abc1234' in output, got %q", result)
	}
	if !strings.Contains(result, "def4567") {
		t.Errorf("expected shortened hash 'def4567' in output, got %q", result)
	}
}

func TestFormatConvergedStateDiff_MissingHash(t *testing.T) {
	diff := &ConvergedStateDiff{
		NumStates: 2,
		DifferingObjects: []ObjectDiff{
			{
				Key: testKey("Secret", "default", "secret1"),
				ByState: map[string]snapshot.VersionHash{
					"state1": testHash("hash123"),
					"state2": {Value: ""}, // missing
				},
			},
		},
		IdenticalCount: 1,
	}

	result := FormatConvergedStateDiff(diff)

	// Verify missing hash is indicated
	if !strings.Contains(result, "(missing)") {
		t.Errorf("expected '(missing)' for empty hash in output, got %q", result)
	}
}

func TestFormatLastWriteAnalysis_Nil(t *testing.T) {
	result := FormatLastWriteAnalysis(nil)
	if result != "No last write analysis available" {
		t.Errorf("expected 'No last write analysis available', got %q", result)
	}
}

func TestFormatLastWriteAnalysis_NoPaths(t *testing.T) {
	analysis := &LastWriteAnalysis{
		Object: testKey("Endpoints", "ns", "ep1"),
		ByPath: nil,
	}

	result := FormatLastWriteAnalysis(analysis)

	if !strings.Contains(result, "Last write analysis for Endpoints/ns/ep1") {
		t.Errorf("expected header with object key, got %q", result)
	}
	if !strings.Contains(result, "No paths found") {
		t.Errorf("expected 'No paths found' in output, got %q", result)
	}
}

func TestFormatLastWriteAnalysis_WithPaths(t *testing.T) {
	analysis := &LastWriteAnalysis{
		Object: testKey("Endpoints", "ns", "ep1"),
		ByPath: []PathLastWrite{
			{
				PathIndex: 0,
				StateID:   "state1",
				FinalHash: testHash("abc1234567890"),
				LastWriteStep: LastWriteStep{
					StepIndex:    25,
					ControllerId: "EndpointsController",
				},
			},
			{
				PathIndex: 1,
				StateID:   "state2",
				FinalHash: testHash("def4567890123"),
				LastWriteStep: LastWriteStep{
					StepIndex:    40,
					ControllerId: "EndpointsController",
				},
			},
		},
	}

	result := FormatLastWriteAnalysis(analysis)

	// Verify header
	if !strings.Contains(result, "Last write analysis for Endpoints/ns/ep1") {
		t.Errorf("expected header, got %q", result)
	}

	// Verify path information
	if !strings.Contains(result, "Path 0 (-> state state1)") {
		t.Errorf("expected 'Path 0 (-> state state1)' in output, got %q", result)
	}
	if !strings.Contains(result, "Path 1 (-> state state2)") {
		t.Errorf("expected 'Path 1 (-> state state2)' in output, got %q", result)
	}

	// Verify last write details
	if !strings.Contains(result, "step 25 by EndpointsController") {
		t.Errorf("expected 'step 25 by EndpointsController' in output, got %q", result)
	}
	if !strings.Contains(result, "step 40 by EndpointsController") {
		t.Errorf("expected 'step 40 by EndpointsController' in output, got %q", result)
	}

	// Verify hashes
	if !strings.Contains(result, "abc1234") {
		t.Errorf("expected shortened hash 'abc1234' in output, got %q", result)
	}
}

func TestFormatObjectLifecycle_Nil(t *testing.T) {
	result := FormatObjectLifecycle(nil)
	if result != "No lifecycle analysis available" {
		t.Errorf("expected 'No lifecycle analysis available', got %q", result)
	}
}

func TestFormatObjectLifecycle_NotFound(t *testing.T) {
	lifecycle := &ObjectLifecycleResult{
		Object:      testKey("Pod", "ns", "pod1"),
		TargetHash:  testHash("xyz7890123456"),
		StateIndex:  0,
		PathIndex:   0,
		Found:       false,
		Appearances: nil,
	}

	result := FormatObjectLifecycle(lifecycle)

	if !strings.Contains(result, "Object: Pod/ns/pod1") {
		t.Errorf("expected 'Object: Pod/ns/pod1', got %q", result)
	}
	if !strings.Contains(result, "Target hash: xyz7890") {
		t.Errorf("expected 'Target hash: xyz7890', got %q", result)
	}
	if !strings.Contains(result, "State 0, Path 0") {
		t.Errorf("expected 'State 0, Path 0', got %q", result)
	}
	if !strings.Contains(result, "No appearances found") {
		t.Errorf("expected 'No appearances found' in output, got %q", result)
	}
}

func TestFormatObjectLifecycle_WithAppearances(t *testing.T) {
	lifecycle := &ObjectLifecycleResult{
		Object:     testKey("Pod", "ns", "pod1"),
		TargetHash: testHash("xyz7890123456"),
		StateIndex: 0,
		PathIndex:  0,
		Found:      true,
		Appearances: []StepInfo{
			{StepIndex: 38, ControllerId: "PodLifecycleController"},
			{StepIndex: 45, ControllerId: "PodLifecycleController"},
		},
	}

	result := FormatObjectLifecycle(lifecycle)

	// Verify header information
	if !strings.Contains(result, "Object: Pod/ns/pod1") {
		t.Errorf("expected 'Object: Pod/ns/pod1', got %q", result)
	}
	if !strings.Contains(result, "Target hash: xyz7890") {
		t.Errorf("expected 'Target hash: xyz7890', got %q", result)
	}
	if !strings.Contains(result, "State 0, Path 0") {
		t.Errorf("expected 'State 0, Path 0', got %q", result)
	}

	// Verify appearance count
	if !strings.Contains(result, "Found 2 appearance(s)") {
		t.Errorf("expected 'Found 2 appearance(s)' in output, got %q", result)
	}

	// Verify appearance details
	if !strings.Contains(result, "step 38: PodLifecycleController") {
		t.Errorf("expected 'step 38: PodLifecycleController' in output, got %q", result)
	}
	if !strings.Contains(result, "step 45: PodLifecycleController") {
		t.Errorf("expected 'step 45: PodLifecycleController' in output, got %q", result)
	}
}

func TestFormatKey(t *testing.T) {
	key := testKey("Deployment", "my-namespace", "my-app")
	result := formatKey(key)

	expected := "Deployment/my-namespace/my-app"
	if result != expected {
		t.Errorf("expected %q, got %q", expected, result)
	}
}

func TestShortenHash(t *testing.T) {
	tests := []struct {
		name     string
		hash     snapshot.VersionHash
		expected string
	}{
		{
			name:     "empty hash",
			hash:     snapshot.VersionHash{Value: ""},
			expected: "(missing)",
		},
		{
			name:     "short hash",
			hash:     testHash("abc"),
			expected: "abc",
		},
		{
			name:     "exactly 7 chars",
			hash:     testHash("abc1234"),
			expected: "abc1234",
		},
		{
			name:     "long hash",
			hash:     testHash("abc1234567890xyz"),
			expected: "abc1234",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := shortenHash(tt.hash)
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}
