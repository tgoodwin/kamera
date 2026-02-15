package coverage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

type inputMapFixture struct {
	Mapping map[string][]map[string]any `json:"mapping"`
}

func TestLoadInputMap(t *testing.T) {
	good := inputMapFixture{
		Mapping: map[string][]map[string]any{
			"core/v1/Service": {
				{
					"name": "svc",
					"object": map[string]any{
						"apiVersion": "v1",
						"kind":       "Service",
						"metadata": map[string]any{
							"name":      "demo",
							"namespace": "default",
						},
					},
				},
			},
		},
	}

	badMissing := map[string]any{}
	badMulti := inputMapFixture{
		Mapping: map[string][]map[string]any{
			"core/v1/Service": {
				{"name": "one", "object": map[string]any{"apiVersion": "v1", "kind": "Service"}},
				{"name": "two", "object": map[string]any{"apiVersion": "v1", "kind": "Service"}},
			},
		},
	}

	cases := []struct {
		name      string
		payload   any
		wantError bool
	}{
		{name: "valid", payload: good, wantError: false},
		{name: "missing-mapping", payload: badMissing, wantError: true},
		{name: "multi-template", payload: badMulti, wantError: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeInputMap(t, tc.payload)
			loaded, err := LoadInputMap(path)
			if tc.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			templates := loaded.Mapping["core/v1/Service"]
			require.Len(t, templates, 1)
			require.NotNil(t, templates[0].Object)
			require.Equal(t, "Service", templates[0].Object.GetKind())
		})
	}
}

func writeInputMap(t *testing.T, payload any) string {
	t.Helper()
	data, err := json.Marshal(payload)
	require.NoError(t, err)
	dir := t.TempDir()
	path := filepath.Join(dir, "input-map.json")
	require.NoError(t, os.WriteFile(path, data, 0644))
	return path
}
