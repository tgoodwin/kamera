package diff

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/google/go-cmp/cmp"
)

// DiffReport represents a structured difference between two JSON objects
type DiffReport struct {
	AddedPaths    []string
	RemovedPaths  []string
	ModifiedPaths []ModifiedPath
}

// ModifiedPath represents a change in value at a specific path
type ModifiedPath struct {
	Path     string
	OldValue interface{}
	NewValue interface{}
}

// JSONDiffReporter implements custom reporting for JSON differences
type JSONDiffReporter struct {
	path   cmp.Path
	report DiffReport
}

func (r *JSONDiffReporter) PushStep(ps cmp.PathStep) {
	r.path = append(r.path, ps)
}

func (r *JSONDiffReporter) PopStep() {
	r.path = r.path[:len(r.path)-1]
}

func (r *JSONDiffReporter) Report(rs cmp.Result) {
	if !rs.Equal() {
		currentPath := r.buildPath()
		vx, vy := r.path.Last().Values()

		switch {
		case !vx.IsValid() && vy.IsValid():
			r.report.AddedPaths = append(r.report.AddedPaths, currentPath)
		case vx.IsValid() && !vy.IsValid():
			r.report.RemovedPaths = append(r.report.RemovedPaths, currentPath)
		default:
			r.report.ModifiedPaths = append(r.report.ModifiedPaths, ModifiedPath{
				Path:     currentPath,
				OldValue: reflectValueToInterface(vx),
				NewValue: reflectValueToInterface(vy),
			})
		}
	}
}

// buildPath constructs a stable JSON path from the current path steps
func (r *JSONDiffReporter) buildPath() string {
	var parts []string
	for _, ps := range r.path {
		switch t := ps.(type) {
		case cmp.MapIndex:
			// For JSON objects, map keys should always be strings
			key := t.Key()
			if s, ok := key.Interface().(string); ok {
				parts = append(parts, s)
			} else {
				// Fallback for non-string keys (shouldn't happen with JSON)
				parts = append(parts, fmt.Sprint(key.Interface()))
			}
		case cmp.SliceIndex:
			parts = append(parts, fmt.Sprintf("[%d]", t.Key()))
		case cmp.StructField:
			// Include struct field names if we encounter them
			parts = append(parts, t.Name())
		default:
			// Skip unknown path steps - might want to log this in production
			continue
		}
	}
	// Build path, handling array indices without dots
	var result string
	for i, part := range parts {
		if strings.HasPrefix(part, "[") {
			result += part
		} else if i > 0 {
			result += "." + part
		} else {
			result += part
		}
	}
	return result
}

// reflectValueToInterface safely converts a reflect.Value to interface{}
func reflectValueToInterface(v reflect.Value) interface{} {
	if !v.IsValid() {
		return nil
	}
	return v.Interface()
}

// GetReport returns a sorted, structured report of the differences
func (r *JSONDiffReporter) GetReport() DiffReport {
	// Sort all slices for stable output
	sort.Strings(r.report.AddedPaths)
	sort.Strings(r.report.RemovedPaths)
	sort.Slice(r.report.ModifiedPaths, func(i, j int) bool {
		return r.report.ModifiedPaths[i].Path < r.report.ModifiedPaths[j].Path
	})
	return r.report
}

// CompareJSON compares two JSON objects and returns a structured report
func CompareJSON(x, y map[string]interface{}) DiffReport {
	reporter := &JSONDiffReporter{}
	cmp.Equal(x, y, cmp.Reporter(reporter))
	return reporter.GetReport()
}
