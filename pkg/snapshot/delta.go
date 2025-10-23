package snapshot

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"

	"github.com/google/go-cmp/cmp"
	"github.com/tgoodwin/kamera/pkg/tag"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/diff"
	"sigs.k8s.io/yaml"
)

type uniqueKey struct {
	Kind     string
	ObjectID string
	Version  string
}

func ReadFile(f io.Reader) ([]Record, error) {
	seen := make(map[VersionKey]struct{})
	scanner := bufio.NewScanner(f)
	records := make([]Record, 0)
	for scanner.Scan() {
		r, err := LoadFromString(scanner.Text())
		if err != nil {
			return nil, err
		}
		key := VersionKey{r.Kind, r.ObjectID, r.Version}
		if _, ok := seen[key]; !ok {
			records = append(records, r)
			seen[key] = struct{}{}
		}
	}
	return records, nil
}

func GroupByID(records []Record) map[string][]Record {
	seen := make(map[VersionKey]struct{})
	groups := make(map[string][]Record)
	for _, r := range records {
		if _, ok := seen[VersionKey{r.Kind, r.ObjectID, r.Version}]; ok {
			continue
		}
		seen[VersionKey{r.Kind, r.ObjectID, r.Version}] = struct{}{}

		if _, ok := groups[r.ObjectID]; !ok {
			groups[r.ObjectID] = make([]Record, 0)
		}
		groups[r.ObjectID] = append(groups[r.ObjectID], r)
	}

	return groups
}

func LoadFromString(s string) (Record, error) {
	var r Record
	err := json.Unmarshal([]byte(s), &r)
	return r, err
}

func (r Record) Diff(other Record) (string, error) {
	if r.Kind != other.Kind || r.ObjectID != other.ObjectID {
		return "", fmt.Errorf("cannot diff records with different kinds or object IDs")
	}
	this := unstructured.Unstructured{}
	otherObj := unstructured.Unstructured{}
	if err := json.Unmarshal([]byte(r.Value), &this); err != nil {
		return "", err
	}
	if err := json.Unmarshal([]byte(other.Value), &otherObj); err != nil {
		return "", err
	}
	header := fmt.Sprintf("currVersion: %s\nprevVersion:%s", other.GetID(), r.GetID())
	return fmt.Sprintf("%s\nDeltas:\n%s", header, computeDelta(&this, &otherObj)), nil
}

func DiffObjects(first, other *unstructured.Unstructured) (string, error) {
	return computeDelta(first, other), nil
}

var toIgnore = map[string]struct{}{
	"resourceVersion": {},
	"managedFields":   {},
	// "generation":         {},
	// "observedGeneration": {},

	// sleeve annotations
	tag.ChangeID:          {},
	tag.TraceyCreatorID:   {},
	tag.TraceyRootID:      {},
	tag.TraceyReconcileID: {},
}

func DefaultIgnore(k string, v interface{}) bool {
	if _, ok := toIgnore[k]; ok {
		return true
	}
	return false
}

func computeDelta(old, new *unstructured.Unstructured) string {
	oldYAML, oldErr := objectToYAML(old)
	newYAML, newErr := objectToYAML(new)

	switch {
	case oldErr != nil && newErr != nil:
		return fmt.Sprintf("error computing diff: old=%v new=%v", oldErr, newErr)
	case oldErr != nil:
		return fmt.Sprintf("error computing old object diff: %v", oldErr)
	case newErr != nil:
		return fmt.Sprintf("error computing new object diff: %v", newErr)
	}

	// If both objects sanitize to nothing, return empty diff.
	if len(oldYAML) == 0 && len(newYAML) == 0 {
		return ""
	}

	return diff.StringDiff(string(oldYAML), string(newYAML))
}

func ComputeDelta(old, new *unstructured.Unstructured, opts ...cmp.Option) string {
	// opts currently unused but kept for backward compatibility with callers.
	return computeDelta(old, new)
}

func objectToYAML(obj *unstructured.Unstructured) ([]byte, error) {
	if obj == nil {
		return nil, nil
	}
	sanitized := sanitizeMap(obj.Object)
	if len(sanitized) == 0 {
		return nil, nil
	}
	out, err := yaml.Marshal(sanitized)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func sanitizeMap(input map[string]interface{}) map[string]interface{} {
	if input == nil {
		return map[string]interface{}{}
	}
	result := make(map[string]interface{}, len(input))
	for key, value := range input {
		if DefaultIgnore(key, value) {
			continue
		}
		result[key] = sanitizeValue(value)
	}
	return result
}

func sanitizeValue(value interface{}) interface{} {
	switch typed := value.(type) {
	case map[string]interface{}:
		return sanitizeMap(typed)
	case []interface{}:
		return sanitizeSlice(typed)
	case []map[string]interface{}:
		slice := make([]interface{}, 0, len(typed))
		for _, item := range typed {
			slice = append(slice, sanitizeMap(item))
		}
		return slice
	default:
		return typed
	}
}

func sanitizeSlice(values []interface{}) []interface{} {
	if values == nil {
		return nil
	}
	result := make([]interface{}, 0, len(values))
	for _, item := range values {
		result = append(result, sanitizeValue(item))
	}
	return result
}
