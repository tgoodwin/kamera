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
	ObjectID    string `json:"object_id"`
	ReconcileID string `json:"reconcile_id"`
	Kind        string `json:"kind"`
	Version     string `json:"version"`
	Value       string `json:"value"`
}

func (r Record) ToUnstructured() *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	if err := json.Unmarshal([]byte(r.Value), u); err != nil {
		log.Fatalf("Error unmarshaling JSON to unstructured: %v", err)
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

// check out https://github.com/cisco-open/k8s-objectmatcher
func AsRecord(obj client.Object, frameID string) Record {
	serialized := serialize(obj)
	asJSON, _ := json.Marshal(serialized)
	r := Record{
		ObjectID:    string(obj.GetUID()),
		ReconcileID: frameID,
		Kind:        util.GetKind(obj),
		Version:     obj.GetResourceVersion(),
		Value:       string(asJSON),
	}
	return r
}
