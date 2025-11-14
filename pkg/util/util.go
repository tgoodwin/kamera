package util

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"reflect"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
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

func ShortenHash(input string) string {
	h := fnv.New64a() // Use 64-bit hash instead of 32-bit
	h.Write([]byte(input))
	hashNum := h.Sum64()

	// Convert to base36 for readability, but use more characters
	// A 64-bit hash in base36 gives ~13 chars, so we'll use 8
	hash := strconv.FormatUint(hashNum, 36)
	if len(hash) > 8 {
		hash = hash[:8]
	}
	return hash
}

// GetGroupVersionKind attempts to retrieve a fully populated GroupVersionKind for the object.
// It inspects the object's typed metadata, falling back to reflection when necessary.
func GetGroupVersionKind(obj runtime.Object) schema.GroupVersionKind {
	if obj == nil {
		return schema.GroupVersionKind{}
	}
	gvk := obj.GetObjectKind().GroupVersionKind()

	typeAccessor, err := meta.TypeAccessor(obj)
	if err == nil {
		if gvk.Kind == "" {
			gvk.Kind = typeAccessor.GetKind()
		}
		if (gvk.Group == "" || gvk.Version == "") && typeAccessor.GetAPIVersion() != "" {
			if gv, err := schema.ParseGroupVersion(typeAccessor.GetAPIVersion()); err == nil {
				if gvk.Group == "" {
					gvk.Group = gv.Group
				}
				if gvk.Version == "" {
					gvk.Version = gv.Version
				}
			}
		}
	}

	if gvk.Kind == "" {
		t := reflect.TypeOf(obj)
		for t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		gvk.Kind = t.Name()
	}

	return gvk
}

func GroupVersionKindFromTypeMeta(tm metav1.TypeMeta) schema.GroupVersionKind {
	gvk := schema.FromAPIVersionAndKind(tm.APIVersion, tm.Kind)
	return gvk
}

func CanonicalGroupKind(group, kind string) string {
	if group == "" {
		group = "core"
	}
	return group + "/" + kind
}

func CanonicalGroupKindFromAPIVersion(apiVersion, kind string) string {
	gvk := schema.FromAPIVersionAndKind(apiVersion, kind)
	return CanonicalGroupKind(gvk.Group, gvk.Kind)
}

func CanonicalGroupKindFromGVK(gvk schema.GroupVersionKind) string {
	return CanonicalGroupKind(gvk.Group, gvk.Kind)
}

func CanonicalGroupKindFromTypeMeta(tm metav1.TypeMeta) string {
	return CanonicalGroupKindFromGVK(GroupVersionKindFromTypeMeta(tm))
}

func ParseGroupKind(spec string) schema.GroupKind {
	if spec == "" {
		return schema.GroupKind{}
	}
	if strings.Contains(spec, "/") {
		parts := strings.SplitN(spec, "/", 2)
		group := parts[0]
		if group == "core" {
			group = ""
		}
		return schema.GroupKind{Group: group, Kind: parts[1]}
	}
	if strings.Contains(spec, ".") {
		_, gk := schema.ParseKindArg(spec)
		return gk
	}
	return schema.GroupKind{Kind: spec}
}

// GetKind returns the Kind component of the object's GroupVersionKind.
func GetKind(obj runtime.Object) string {
	return GetGroupVersionKind(obj).Kind
}

func UUID() string {
	return uuid.New().String()
}

// InferListKind returns the Kind of the objects contained in the list
func InferListKind(list client.ObjectList) string {
	if unstructuredList, ok := list.(*unstructured.UnstructuredList); ok {
		kind := unstructuredList.GetKind()
		// if suffixed with "List", remove it
		if len(kind) > 4 && kind[len(kind)-4:] == "List" {
			return kind[:len(kind)-4]
		}
		return kind
	}

	itemsValue := reflect.ValueOf(list).Elem().FieldByName("Items")
	if !itemsValue.IsValid() {
		panic("List object does not have Items field")
	}
	itemType := itemsValue.Type().Elem()
	return itemType.Name()
}

func ConvertToUnstructured(obj runtime.Object) (*unstructured.Unstructured, error) {
	// Get the GroupVersionKind (GVK) using reflection or Scheme
	gvk := GetGroupVersionKind(obj)
	if gvk.Version == "" {
		gvk.Version = "v1"
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

func GetNext[T any](slice []T, mode string) (T, []T, error) {
	var zeroValue T
	if len(slice) == 0 {
		return zeroValue, slice, fmt.Errorf("slice is empty")
	}

	switch mode {
	case "stack":
		// LIFO: Pop the last element
		element := slice[len(slice)-1]
		return element, slice[:len(slice)-1], nil
	case "queue":
		// FIFO: Pop the first element
		element := slice[0]
		return element, slice[1:], nil
	default:
		return zeroValue, slice, fmt.Errorf("invalid mode: %s", mode)
	}
}

func PrettyPrintJSON(jsonStr string) (string, error) {
	var prettyJSON bytes.Buffer
	err := json.Indent(&prettyJSON, []byte(jsonStr), "", "  ")
	if err != nil {
		return "", err
	}
	return prettyJSON.String(), nil
}

// MostCommenElementCount returns the count of the most common element in a slice.
func MostCommonElementCount[T comparable](items []T) int {
	if len(items) == 0 {
		return 0
	}

	// Create a map to count occurrences
	counts := make(map[T]int)

	// Count occurrences of each element
	for _, item := range items {
		counts[item]++
	}

	// Find the maximum count
	maxCount := 0
	for _, count := range counts {
		if count > maxCount {
			maxCount = count
		}
	}

	return maxCount
}
