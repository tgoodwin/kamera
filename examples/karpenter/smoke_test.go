package main

import "testing"

func TestScenarioBuilds(t *testing.T) {
	if _, err := newEnvironmentObjects(); err != nil {
		t.Fatalf("expected environment objects to build: %v", err)
	}
}
