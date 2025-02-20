package diff

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestCompareJSON_SimpleChanges(t *testing.T) {
	tests := []struct {
		name     string
		x        map[string]interface{}
		y        map[string]interface{}
		expected DiffReport
	}{
		{
			name: "simple value modification",
			x: map[string]interface{}{
				"name": "John",
				"age":  30,
			},
			y: map[string]interface{}{
				"name": "John",
				"age":  31,
			},
			expected: DiffReport{
				ModifiedPaths: []ModifiedPath{
					{Path: "age", OldValue: 30, NewValue: 31},
				},
			},
		},
		{
			name: "add and remove fields",
			x: map[string]interface{}{
				"name":   "John",
				"age":    30,
				"oldKey": "remove me",
			},
			y: map[string]interface{}{
				"name":   "John",
				"age":    30,
				"newKey": "added",
			},
			expected: DiffReport{
				AddedPaths:   []string{"newKey"},
				RemovedPaths: []string{"oldKey"},
			},
		},
		{
			name: "nested structure changes",
			x: map[string]interface{}{
				"person": map[string]interface{}{
					"name": "John",
					"address": map[string]interface{}{
						"city": "New York",
					},
				},
			},
			y: map[string]interface{}{
				"person": map[string]interface{}{
					"name": "John",
					"address": map[string]interface{}{
						"city":    "Boston",
						"country": "USA",
					},
				},
			},
			expected: DiffReport{
				AddedPaths: []string{"person.address.country"},
				ModifiedPaths: []ModifiedPath{
					{Path: "person.address.city", OldValue: "New York", NewValue: "Boston"},
				},
			},
		},
		{
			name: "array changes",
			x: map[string]interface{}{
				"numbers": []interface{}{1, 2, 3},
				"users": []interface{}{
					map[string]interface{}{"id": 1, "name": "John"},
					map[string]interface{}{"id": 2, "name": "Jane"},
				},
			},
			y: map[string]interface{}{
				"numbers": []interface{}{1, 4, 3},
				"users": []interface{}{
					map[string]interface{}{"id": 1, "name": "Johnny"},
					map[string]interface{}{"id": 2, "name": "Jane"},
				},
			},
			expected: DiffReport{
				ModifiedPaths: []ModifiedPath{
					{Path: "numbers[1]", OldValue: 2, NewValue: 4},
					{Path: "users[0].name", OldValue: "John", NewValue: "Johnny"},
				},
			},
		},
		{
			name: "null value handling",
			x: map[string]interface{}{
				"nullField": nil,
				"value":     "exists",
			},
			y: map[string]interface{}{
				"nullField": "not null anymore",
				"value":     nil,
			},
			expected: DiffReport{
				ModifiedPaths: []ModifiedPath{
					{Path: "nullField", OldValue: nil, NewValue: "not null anymore"},
					{Path: "value", OldValue: "exists", NewValue: nil},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report := CompareJSON(tt.x, tt.y)

			// Compare lengths first
			if len(report.AddedPaths) != len(tt.expected.AddedPaths) {
				t.Errorf("AddedPaths length mismatch: got %d, want %d",
					len(report.AddedPaths), len(tt.expected.AddedPaths))
			}
			if len(report.RemovedPaths) != len(tt.expected.RemovedPaths) {
				t.Errorf("RemovedPaths length mismatch: got %d, want %d",
					len(report.RemovedPaths), len(tt.expected.RemovedPaths))
			}
			if len(report.ModifiedPaths) != len(tt.expected.ModifiedPaths) {
				t.Errorf("ModifiedPaths length mismatch: got %d, want %d",
					len(report.ModifiedPaths), len(tt.expected.ModifiedPaths))
			}

			// Compare added paths
			for i, path := range tt.expected.AddedPaths {
				if i >= len(report.AddedPaths) || report.AddedPaths[i] != path {
					t.Errorf("AddedPaths[%d] mismatch: got %s, want %s",
						i, report.AddedPaths[i], path)
				}
			}

			// Compare removed paths
			for i, path := range tt.expected.RemovedPaths {
				if i >= len(report.RemovedPaths) || report.RemovedPaths[i] != path {
					t.Errorf("RemovedPaths[%d] mismatch: got %s, want %s",
						i, report.RemovedPaths[i], path)
				}
			}

			// Compare modified paths
			for i, mp := range tt.expected.ModifiedPaths {
				if i >= len(report.ModifiedPaths) {
					t.Errorf("Missing ModifiedPath[%d]: want %+v", i, mp)
					continue
				}
				got := report.ModifiedPaths[i]
				if got.Path != mp.Path {
					t.Errorf("ModifiedPath[%d].Path mismatch: got %s, want %s",
						i, got.Path, mp.Path)
				}
				if !cmp.Equal(got.OldValue, mp.OldValue) {
					t.Errorf("ModifiedPath[%d].OldValue mismatch: got %v, want %v",
						i, got.OldValue, mp.OldValue)
				}
				if !cmp.Equal(got.NewValue, mp.NewValue) {
					t.Errorf("ModifiedPath[%d].NewValue mismatch: got %v, want %v",
						i, got.NewValue, mp.NewValue)
				}
			}
		})
	}
}

func TestCompareJSON_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		x        map[string]interface{}
		y        map[string]interface{}
		expected DiffReport
	}{
		{
			name:     "empty maps",
			x:        map[string]interface{}{},
			y:        map[string]interface{}{},
			expected: DiffReport{},
		},
		{
			name: "completely different maps",
			x: map[string]interface{}{
				"a": 1,
				"b": 2,
			},
			y: map[string]interface{}{
				"c": 3,
				"d": 4,
			},
			expected: DiffReport{
				AddedPaths:   []string{"c", "d"},
				RemovedPaths: []string{"a", "b"},
			},
		},
		{
			name: "deep nested nulls",
			x: map[string]interface{}{
				"nested": map[string]interface{}{
					"deep": map[string]interface{}{
						"value": nil,
					},
				},
			},
			y: map[string]interface{}{
				"nested": map[string]interface{}{
					"deep": map[string]interface{}{
						"value": "not null",
					},
				},
			},
			expected: DiffReport{
				ModifiedPaths: []ModifiedPath{
					{Path: "nested.deep.value", OldValue: nil, NewValue: "not null"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report := CompareJSON(tt.x, tt.y)
			if !cmp.Equal(report, tt.expected) {
				t.Errorf("CompareJSON() mismatch:\ngot: %+v\nwant: %+v",
					report, tt.expected)
			}
		})
	}
}

func TestDiffReport_ToRFC6902(t *testing.T) {
	tests := []struct {
		name   string
		report DiffReport
		want   []string
	}{
		{
			name: "simple field changes",
			report: DiffReport{
				AddedPaths: []string{"person.address.country"},
				ModifiedPaths: []ModifiedPath{
					{Path: "person.address.city", OldValue: "New York", NewValue: "Boston"},
				},
			},
			want: []string{
				`{"op":"add","path":"/person/address/country","value":null}`,
				`{"op":"replace","path":"/person/address/city","value":"Boston"}`,
			},
		},
		{
			name: "array notation",
			report: DiffReport{
				ModifiedPaths: []ModifiedPath{
					{Path: "users[0].name", OldValue: "John", NewValue: "Johnny"},
					{Path: "numbers[1]", OldValue: 2, NewValue: 4},
				},
			},
			want: []string{
				`{"op":"replace","path":"/numbers[1]","value":4}`,
				`{"op":"replace","path":"/users[0]/name","value":"Johnny"}`,
			},
		},
		{
			name: "add and remove",
			report: DiffReport{
				AddedPaths:   []string{"newKey"},
				RemovedPaths: []string{"oldKey"},
			},
			want: []string{
				`{"op":"add","path":"/newKey","value":null}`,
				`{"op":"remove","path":"/oldKey"}`,
			},
		},
		{
			name:   "empty report",
			report: DiffReport{},
			want:   []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.report.ToRFC6902()

			if len(got) != len(tt.want) {
				t.Errorf("ToRFC6902() returned %d patches, want %d", len(got), len(tt.want))
				return
			}

			// Verify each string is valid JSON and compare after normalizing
			for i := range got {
				var gotJSON, wantJSON interface{}
				if err := json.Unmarshal([]byte(got[i]), &gotJSON); err != nil {
					t.Errorf("Invalid JSON in result[%d]: %s", i, got[i])
				}
				if err := json.Unmarshal([]byte(tt.want[i]), &wantJSON); err != nil {
					t.Errorf("Invalid JSON in expected[%d]: %s", i, tt.want[i])
				}
				if !cmp.Equal(gotJSON, wantJSON) {
					t.Errorf("Patch[%d] = %v, want %v", i, got[i], tt.want[i])
				}
			}
		})
	}
}
