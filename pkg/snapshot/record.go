package snapshot

import (
	"encoding/json"
	"fmt"
	"log"
	"regexp"

	"github.com/tgoodwin/sleeve/pkg/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Record represents a snapshot of a Kubernetes object as it appears in a Sleeve log
type Record struct {
	ObjectID      string `json:"object_id"`
	ReconcileID   string `json:"reconcile_id"`
	OperationID   string `json:"operation_id"` // the operation (event) that produced this record
	OperationType string `json:"op_type"`      // the operation type that produced this record
	Kind          string `json:"kind"`
	Version       string `json:"version"` // resource version
	// TODO change this to an interface that just requires the object is Marhsalable
	Value string `json:"value"` // full object value (snapshot.VersionHash)
}

func (r Record) ToUnstructured() *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	if err := json.Unmarshal([]byte(r.Value), u); err != nil {
		log.Fatalf("Error unmarshaling JSON to unstructured: record operationID: %v, err: %v", r.OperationID, err)
	}
	return u
}

func (r Record) GetID() string {
	kind := r.Kind
	re := regexp.MustCompile(`Kind=([^,]+)`)
	matches := re.FindStringSubmatch(kind)
	if len(matches) > 1 {
		kind = matches[1]
	}
	return fmt.Sprintf("%s:%s@%s", kind, util.Shorter(r.ObjectID), r.Version)
}

var toMask = map[string]struct{}{
	"UID":               {},
	"ResourceVersion":   {},
	"Generation":        {},
	"CreationTimestamp": {},
	// TODO just distinguish between nil and not-nil for purposes of comparison
	"DeletionTimestamp": {},
}

func serialize(obj interface{}) map[string]interface{} {
	data, err := json.Marshal(obj)
	if err != nil {
		log.Fatalf("Error marshaling struct to JSON: %v", err)
	}

	var resultMap map[string]interface{}
	if err := json.Unmarshal(data, &resultMap); err != nil {
		log.Fatalf("Error unmarshaling JSON to map: %v", err)
	}
	return resultMap
}

func maskFields(in map[string]string) map[string]interface{} {
	masked := make(map[string]interface{})
	for k := range in {
		if _, ok := toMask[k]; ok {
			continue
		}
		masked[k] = in[k]
	}
	return masked
}

func AsRecord(obj client.Object, frameID string) (*Record, error) {
	// first, convert to unstructured
	unstructuredObj, err := util.ConvertToUnstructured(obj)
	if err != nil {
		return nil, err
	}

	// Ensure the Kind field is set locally for objects created with typed clients,
	// as it may not be populated until processed by the API server.
	if unstructuredObj.GetKind() == "" {
		// Kind not set in object contents yet, so pull it from the type information
		// and set it explicitly.
		kind := util.GetKind(obj)
		if kind == "" {
			return nil, fmt.Errorf("could not determine Kind of object: %v", obj)
		}
		unstructuredObj.SetKind(kind)
	}

	if unstructuredObj.GetAPIVersion() == "" {
		return nil, fmt.Errorf("APIVersion not set on object: %v", obj)
	}

	asJSON, _ := json.Marshal(unstructuredObj)
	r := &Record{
		// TODO use sleeve-object-id instead of the API-assigned ID
		ObjectID:    string(obj.GetUID()),
		ReconcileID: frameID,
		Kind:        util.GetKind(obj),
		Version:     obj.GetResourceVersion(),
		Value:       string(asJSON),
	}
	return r, nil
}
