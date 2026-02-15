package explore

import "testing"

func TestScenarioResultFields(t *testing.T) {
	scenario := Scenario{Name: "example"}
	result := ScenarioResult{Name: scenario.Name}
	if result.Name != "example" {
		t.Fatalf("expected scenario name to propagate")
	}
}
