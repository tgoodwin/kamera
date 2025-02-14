package snapshot

import "encoding/json"

type NormalizedObject struct {
	Kind      string
	Namespace string
	Name      string
	Spec      map[string]interface{}
	Status    map[string]interface{}
}

func NormalizeObject(key IdentityKey, value VersionHash) NormalizedObject {
	// deserialize the versionHash
	objMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(value), &objMap)
	if err != nil {
		panic("could not deserialize object: " + err.Error())
	}

	// extract the kind, namespace, and name
	kind := objMap["kind"].(string)
	metadata := objMap["metadata"].(map[string]interface{})
	namespace := metadata["namespace"].(string)
	name := metadata["name"].(string)
	// extract spec and status
	spec := objMap["spec"].(map[string]interface{})
	status, ok := objMap["status"].(map[string]interface{})
	if !ok {
		status = make(map[string]interface{})
	}
	return NormalizedObject{
		Kind:      kind,
		Namespace: namespace,
		Name:      name,
		Spec:      spec,
		Status:    status,
	}
}
