package explore

import "testing"

func TestInputsPathDefault(t *testing.T) {
	if InputsPath() != "" {
		t.Fatalf("expected empty inputs path by default")
	}
}
