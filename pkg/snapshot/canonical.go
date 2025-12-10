package snapshot

import (
	"bytes"
	"encoding/json"
	"sort"
)

// canonicalize recursively sorts map keys to produce stable JSON for hashing.
// TODO re-evaluate if this is necessary... unstructured.Unstructured's MarshalJSON should be
// deterministic.
func canonicalize(v interface{}) interface{} {
	switch t := v.(type) {
	case map[string]interface{}:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make(map[string]interface{}, len(t))
		for _, k := range keys {
			out[k] = canonicalize(t[k])
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(t))
		for i, elem := range t {
			out[i] = canonicalize(elem)
		}
		return out
	default:
		return t
	}
}

// CanonicalJSON marshals an object to stable JSON with sorted map keys.
func CanonicalJSON(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	canon := canonicalize(v)
	if err := enc.Encode(canon); err != nil {
		return nil, err
	}
	// Trim trailing newline added by Encoder.Encode.
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}
