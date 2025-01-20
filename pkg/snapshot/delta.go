package snapshot

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"reflect"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/tgoodwin/sleeve/pkg/tag"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Delta represents a change between two fields in an object
// where a diff between two versions of an object is represented by a collection of Deltas
type Delta struct {
	path string
	prev reflect.Value
	curr reflect.Value
}

func (d Delta) String() string {
	if !d.prev.IsValid() {
		return fmt.Sprintf("%s:\n\t-: %+v\n\t+: %+v\n", d.path, nil, d.curr)
	}
	if !d.curr.IsValid() {
		return fmt.Sprintf("%s:\n\t-: %+v\n\t+: %+v\n", d.path, d.prev, nil)
	}
	return fmt.Sprintf("%s:\n\t-: %+v\n\t+: %+v\n", d.path, d.prev, d.curr)
}

func (d Delta) Eliminates(other Delta) bool {
	if d.path == other.path {
		if d.prev.String() == other.curr.String() {
			return true
		}
		if d.curr.String() == other.prev.String() {
			return true
		}
	}
	return false
}

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
	reporter := DiffReporter{}
	header := fmt.Sprintf("currVersion: %s\nprevVersion:%s", other.GetID(), r.GetID())
	return fmt.Sprintf("%s\nDeltas:\n%s", header, computeDelta(reporter, &this, &otherObj)), nil
}

func DiffObjects(first, other *unstructured.Unstructured) (string, error) {
	reporter := DiffReporter{}
	return computeDelta(reporter, first, other), nil
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

func computeDelta(dr DiffReporter, old, new *unstructured.Unstructured) string {
	cmpOpt := cmpopts.IgnoreMapEntries(DefaultIgnore)
	cmp.Diff(old, new, cmpOpt, cmp.Reporter(&dr))
	rdiff := dr.String()
	return rdiff
}

func ComputeDelta(old, new *unstructured.Unstructured, opts ...cmp.Option) string {
	reporter := DiffReporter{}
	return computeDelta(reporter, old, new)
}
