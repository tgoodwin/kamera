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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
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

func ConvertToUnstructured(obj client.Object) (*unstructured.Unstructured, error) {
	unstructuredMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to convert object to unstructured: %w", err)
	}
	return &unstructured.Unstructured{Object: unstructuredMap}, nil
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
