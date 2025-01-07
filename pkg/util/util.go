package util

import (
	"encoding/json"
	"reflect"
	"strings"

	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
	data, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	u := &unstructured.Unstructured{}
	if err := json.Unmarshal(data, u); err != nil {
		return nil, err
	}
	gvk := obj.GetObjectKind().GroupVersionKind()
	u.SetGroupVersionKind(gvk)

	return u, nil
}
