package util

import (
	"encoding/json"
	"strconv"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// GetObjectGeneration returns an object's generation.
//
// It prioritizes the generated accessor and falls back to raw metadata fields to
// handle test fixtures where generation may be represented as a float or string.
func GetObjectGeneration(obj *unstructured.Unstructured) int64 {
	if obj == nil {
		return 0
	}
	if gen := obj.GetGeneration(); gen != 0 {
		return gen
	}

	raw, found, err := unstructured.NestedFieldNoCopy(obj.Object, "metadata", "generation")
	if err != nil || !found || raw == nil {
		return 0
	}

	switch v := raw.(type) {
	case int64:
		return v
	case int32:
		return int64(v)
	case int:
		return int64(v)
	case float64:
		return int64(v)
	case float32:
		return int64(v)
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return i
		}
		if f, err := v.Float64(); err == nil {
			return int64(f)
		}
	case string:
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i
		}
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return int64(f)
		}
	}

	return 0
}
