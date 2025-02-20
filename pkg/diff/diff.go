package diff

import (
	"encoding/json"
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

// ToRFC6902 converts a DiffReport to a slice of RFC 6902 JSON Patch strings.
// Converts dot notation paths (e.g., "person.address.city") to RFC 6902 paths
// (e.g., "/person/address/city").
func (r DiffReport) ToRFC6902() []string {
	// Pre-allocate slice with total capacity
	total := len(r.AddedPaths) + len(r.RemovedPaths) + len(r.ModifiedPaths)
	patches := make([]string, 0, total)

	// Convert dot notation to RFC 6902 path format
	dotToRFC6902 := func(dotPath string) string {
		// Handle array indices specially
		var parts []string
		for _, part := range strings.Split(dotPath, ".") {
			if strings.HasPrefix(part, "[") && strings.HasSuffix(part, "]") {
				// Keep array notation as is
				parts = append(parts, part)
			} else {
				parts = append(parts, part)
			}
		}
		// Join with forward slashes and add leading slash
		return "/" + strings.Join(parts, "/")
	}

	// Helper to create and append JSON patch strings
	addPatch := func(op, path string, value interface{}) {
		rfcPath := dotToRFC6902(path)
		// For remove operations, we don't include the value field
		var patch string
		if op == "remove" {
			patch = fmt.Sprintf(`{"op":"%s","path":"%s"}`, op, rfcPath)
		} else {
			// Marshal the value to ensure proper JSON escaping
			valueJSON, err := json.Marshal(value)
			if err != nil {
				// If we can't marshal the value, use null
				valueJSON = []byte("null")
			}
			patch = fmt.Sprintf(`{"op":"%s","path":"%s","value":%s}`, op, rfcPath, string(valueJSON))
		}
		patches = append(patches, patch)
	}

	// Add operations
	for _, path := range r.AddedPaths {
		// Note: Since DiffReport doesn't store values for added paths,
		// we use null as a placeholder
		addPatch("add", path, nil)
	}

	// Remove operations
	for _, path := range r.RemovedPaths {
		addPatch("remove", path, nil)
	}

	// Replace operations
	for _, mp := range r.ModifiedPaths {
		addPatch("replace", mp.Path, mp.NewValue)
	}

	// Sort for stable output
	sort.Strings(patches)

	return patches
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
