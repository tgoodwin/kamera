package util

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Shorter is used to shorten a UID for display purposes only.
func Shorter(s string) string {
	if idx := strings.Index(s, "-"); idx != -1 {
		return s[:idx]
	}
	return s
}

// GetKind returns the kind of the object. It uses reflection to determine the kind if the client.Object instance
// does not have a GroupVersionKind set yet. This happens during object creation before the object is sent to the
// Kubernetes API server.
func GetKind(obj client.Object) string {
	kind := obj.GetObjectKind().GroupVersionKind().Kind
	if kind == "" {
		t := reflect.TypeOf(obj)
		for t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		kind = t.Name()
	}
	return kind
}

func UUID() string {
	return uuid.New().String()
}

func ConvertToUnstructured(obj client.Object) (*unstructured.Unstructured, error) {
	// Get the GroupVersionKind (GVK) using reflection or Scheme
	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Kind == "" {
		// Fallback: use reflection to infer GVK if not set
		typ := reflect.TypeOf(obj).Elem()
		gvk = schema.GroupVersionKind{
			Group:   "", // Update as needed if group can be inferred
			Version: "v1",
			Kind:    typ.Name(),
		}
	}

	// Serialize the object to JSON
	data, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize object: %w", err)
	}

	// Deserialize into map for manipulation
	var objMap map[string]interface{}
	if err := json.Unmarshal(data, &objMap); err != nil {
		return nil, fmt.Errorf("failed to deserialize object into map: %w", err)
	}

	// Inject apiVersion and kind
	objMap["apiVersion"] = gvk.GroupVersion().String()
	objMap["kind"] = gvk.Kind

	// Convert map to *unstructured.Unstructured
	unstructuredObj := &unstructured.Unstructured{Object: objMap}
	return unstructuredObj, nil
}
