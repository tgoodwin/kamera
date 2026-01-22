package snapshot

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/tgoodwin/kamera/pkg/diff"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type NormalizedObject struct {
	APIVersion string
	Kind       string
	Namespace  string
	Name       string
	Spec       map[string]interface{}
	Status     map[string]interface{}
}

func NormalizeObject(hash VersionHash) NormalizedObject {
	// deserialize the versionHash
	objMap := make(map[string]interface{})
	value := hash.Value
	err := json.Unmarshal([]byte(value), &objMap)
	if err != nil {
		panic("could not deserialize object: " + err.Error())
	}

	// extract the kind, namespace, and name
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("panic occurred while normalizing object: ", hash.Value)
			panic(r) // re-throw the panic after logging
		}
	}()

	apiVersion, _ := objMap["apiVersion"].(string)
	kind, _ := objMap["kind"].(string)
	metadata, ok := objMap["metadata"].(map[string]interface{})
	if !ok {
		metadata = map[string]interface{}{}
	}
	namespace, _ := metadata["namespace"].(string)

	// handle cases where the name is generated
	name, ok := metadata["name"].(string)
	if !ok {
		name, _ = metadata["generateName"].(string)
	}

	// extract spec and status
	spec, ok := objMap["spec"].(map[string]interface{})
	if !ok {
		spec = map[string]interface{}{}
	}
	status, ok := objMap["status"].(map[string]interface{})
	if !ok {
		status = make(map[string]interface{})
	}
	return NormalizedObject{
		APIVersion: apiVersion,
		Kind:       kind,
		Namespace:  namespace,
		Name:       name,
		Spec:       spec,
		Status:     status,
	}
}

func (n *NormalizedObject) Identity() string {
	return n.APIVersion + "/" + n.Namespace + "/" + n.Kind + "/" + n.Name
}

type NormalizedDiff struct {
	Base       NormalizedObject
	Other      NormalizedObject
	SpecDiff   []string
	StatusDiff []string
}

func (n NormalizedObject) Diff(other NormalizedObject) (*NormalizedDiff, error) {
	if n.Identity() != other.Identity() {
		return nil, errors.New("cannot diff objects with different identities")
	}
	specDiff := diff.CompareJSON(n.Spec, other.Spec).ToRFC6902()
	statusDiff := diff.CompareJSON(n.Status, other.Status).ToRFC6902()

	return &NormalizedDiff{
		Base:       n,
		Other:      other,
		SpecDiff:   specDiff,
		StatusDiff: statusDiff,
	}, nil
}

// CheckSpecChanged compares the spec sections of two unstructured objects to determine
// if the spec has changed. This mimics APIServer behavior where Generation is only
// incremented on spec changes, not status-only updates.
// Returns (true, nil) if spec changed, (false, nil) if unchanged, or (false, error) on error.
func CheckSpecChanged(oldObj, newObj *unstructured.Unstructured) (bool, error) {
	if oldObj == nil || newObj == nil {
		return false, fmt.Errorf("nil object provided")
	}
	if oldObj.Object == nil || newObj.Object == nil {
		return false, fmt.Errorf("object has nil Object map")
	}

	_, oldHasSpec := oldObj.Object["spec"]
	_, newHasSpec := newObj.Object["spec"]

	// If neither has a spec, consider it unchanged (metadata-only update)
	if !oldHasSpec && !newHasSpec {
		return false, nil
	}

	// If one has spec and the other doesn't, spec changed
	if oldHasSpec != newHasSpec {
		return true, nil
	}

	// Both have specs - compare them using normalized objects
	oldJSON := mustMarshalJSON(oldObj)
	newJSON := mustMarshalJSON(newObj)
	oldNormalized := NormalizeObject(VersionHash{Value: oldJSON})
	newNormalized := NormalizeObject(VersionHash{Value: newJSON})

	diff, err := oldNormalized.Diff(newNormalized)
	if err != nil {
		return false, fmt.Errorf("failed to diff normalized objects: %w", err)
	}

	// Spec changed if there are any spec diffs
	return len(diff.SpecDiff) > 0, nil
}

func mustMarshalJSON(obj interface{}) string {
	// Use a simple approach: convert to JSON string for comparison
	// This is used by NormalizeObject which expects a JSON string
	data, err := json.Marshal(obj)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal object: %v", err))
	}
	return string(data)
}
