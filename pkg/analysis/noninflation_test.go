package analysis

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"
)

func TestFindVolatileFields(t *testing.T) {
	tests := []struct {
		name     string
		timeline []ResourceState
		want     map[string][]string
	}{
		{
			name: "label removed later is volatile",
			timeline: []ResourceState{
				{Name: "R0", JSON: mustJSON(map[string]any{})},
				{
					Name: "R1",
					JSON: mustJSON(map[string]any{
						"foolabel":    true,
						"stablelabel": true,
					}),
				},
				{
					Name: "R2",
					JSON: mustJSON(map[string]any{
						"stablelabel": true,
					}),
				},
			},
			want: map[string][]string{
				"R1": {"/foolabel"},
			},
		},
		{
			name: "monotonic growth is stable",
			timeline: []ResourceState{
				{Name: "R0", JSON: mustJSON(map[string]any{})},
				{Name: "R1", JSON: mustJSON(map[string]any{"foo": true})},
				{Name: "R2", JSON: mustJSON(map[string]any{"foo": true, "bar": 1})},
			},
			want: map[string][]string{},
		},
		{
			name: "nested field disappears",
			timeline: []ResourceState{
				{Name: "R0", JSON: mustJSON(map[string]any{"spec": map[string]any{"replicas": 1}})},
				{Name: "R1", JSON: mustJSON(map[string]any{"spec": map[string]any{"replicas": 1, "annotations": map[string]any{"active": true}}})},
				{Name: "R2", JSON: mustJSON(map[string]any{"spec": map[string]any{"replicas": 1}})},
			},
			want: map[string][]string{
				"R1": {"/spec/annotations/active"},
			},
		},
		{
			name: "nested field changes",
			timeline: []ResourceState{
				{Name: "R0", JSON: mustJSON(map[string]any{"spec": map[string]any{"replicas": 1}})},
				{Name: "R1", JSON: mustJSON(map[string]any{"spec": map[string]any{"replicas": 1, "annotations": map[string]any{"active": true, "hasBeenActive": true}}})},
				{Name: "R2", JSON: mustJSON(map[string]any{"spec": map[string]any{"replicas": 1, "annotations": map[string]any{"active": false, "hasBeenActive": true}}})},
			},
			want: map[string][]string{
				"R1": {"/spec/annotations/active"},
			},
		},
		{
			name: "multiple volatile fields",
			timeline: []ResourceState{
				{Name: "R0", JSON: mustJSON(map[string]any{"spec": map[string]any{"replicas": 1}})},
				{Name: "R1", JSON: mustJSON(map[string]any{"spec": map[string]any{"replicas": 1, "active": true}})},
				{Name: "R2", JSON: mustJSON(map[string]any{"spec": map[string]any{"replicas": 1, "active": false}})},
				{Name: "R3", JSON: mustJSON(map[string]any{"spec": map[string]any{"replicas": 2, "active": false}})},
			},
			want: map[string][]string{
				"R1": {"/spec/active"},
				"R2": {"/spec/replicas"},
			},
		},
		{
			name: "value change marks earlier state volatile",
			timeline: []ResourceState{
				{Name: "R0", JSON: mustJSON(map[string]any{"spec": map[string]any{"replicas": 1}})},
				{Name: "R1", JSON: mustJSON(map[string]any{"spec": map[string]any{"replicas": 2}})},
				{Name: "R2", JSON: mustJSON(map[string]any{"spec": map[string]any{"replicas": 2}})},
			},
			want: map[string][]string{
				"R0": {"/spec/replicas"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := FindVolatileFields(tt.timeline)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			normalize := func(m map[string][]string) map[string][]string {
				cp := map[string][]string{}
				for k, v := range m {
					out := append([]string(nil), v...)
					sort.Strings(out)
					cp[k] = out
				}
				return cp
			}
			if !reflect.DeepEqual(normalize(got), normalize(tt.want)) {
				t.Fatalf("volatile paths mismatch: got %+v, want %+v", got, tt.want)
			}
		})
	}
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
