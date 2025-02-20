package snapshot

import (
	"encoding/json"
	"errors"

	"github.com/tgoodwin/sleeve/pkg/diff"
)

type NormalizedObject struct {
	APIVersion string
	Kind       string
	Namespace  string
	Name       string
	Spec       map[string]interface{}
	Status     map[string]interface{}
}

func NormalizeObject(value VersionHash) NormalizedObject {
	// deserialize the versionHash
	objMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(value), &objMap)
	if err != nil {
		panic("could not deserialize object: " + err.Error())
	}

	// extract the kind, namespace, and name
	defer func() {
		if r := recover(); r != nil {
			println("panic occurred while normalizing object: ", value)
			panic(r) // re-throw the panic after logging
		}
	}()

	apiVersion := objMap["apiVersion"].(string)
	kind := objMap["kind"].(string)
	metadata := objMap["metadata"].(map[string]interface{})
	namespace := metadata["namespace"].(string)

	// handle cases where the name is generated
	name, ok := metadata["name"].(string)
	if !ok {
		name = metadata["generateName"].(string)
	}

	// extract spec and status
	spec := objMap["spec"].(map[string]interface{})
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
