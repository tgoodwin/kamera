package main

import "testing"

func TestScenarioBuilds(t *testing.T) {
	if _, err := newScenarioObjects(); err != nil {
		t.Fatalf("expected scenario objects to build: %v", err)
	}
}
