package analysis

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/tgoodwin/kamera/pkg/snapshot"
)

// LoadDump reads and unmarshals a kamera dump file from the specified path.
// It returns the raw dump structure without converting to tracecheck types.
// For conversion to tracecheck types, use pkg/interactive.LoadInspectorDump.
func LoadDump(path string) (*Dump, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read dump file: %w", err)
	}

	var dump Dump
	if err := json.Unmarshal(data, &dump); err != nil {
		return nil, fmt.Errorf("unmarshal dump: %w", err)
	}

	// Enrich keys from object data (for legacy dumps missing Kind/Group)
	enrichKeysFromObjects(&dump)

	return &dump, nil
}

// enrichKeysFromObjects fills in missing Kind and Group fields in CompositeKeys
// by looking up the corresponding object data from the dump.
func enrichKeysFromObjects(dump *Dump) {
	if dump == nil {
		return
	}

	// Build a hash -> object lookup
	objByHash := make(map[string]map[string]interface{})
	for _, obj := range dump.Objects {
		objByHash[obj.Hash.Value] = obj.Object
	}

	// Helper to enrich a single key
	enrichKey := func(ov *DumpObjectVersion) {
		if ov.Key.ResourceKey.Kind != "" {
			return // already has kind
		}
		obj, ok := objByHash[ov.Hash.Value]
		if !ok {
			return
		}
		gk := groupKindFromObject(obj)
		if gk.Kind != "" {
			ov.Key.ResourceKey.Kind = gk.Kind
			ov.Key.IdentityKey.Kind = gk.Kind
		}
		if gk.Group != "" {
			ov.Key.ResourceKey.Group = gk.Group
			ov.Key.IdentityKey.Group = gk.Group
		}
	}

	// Enrich all object versions in the dump
	for i := range dump.States {
		state := &dump.States[i]
		for j := range state.State.Contents.Objects {
			enrichKey(&state.State.Contents.Objects[j])
		}
		for pi := range state.Paths {
			for si := range state.Paths[pi] {
				step := &state.Paths[pi][si]
				for k := range step.StateBefore {
					enrichKey(&step.StateBefore[k])
				}
				for k := range step.StateAfter {
					enrichKey(&step.StateAfter[k])
				}
				for k := range step.Changes.ObjectVersions {
					enrichKey(&step.Changes.ObjectVersions[k])
				}
			}
		}
	}
}

// groupKind holds parsed group and kind from object data.
type groupKind struct {
	Group string
	Kind  string
}

// groupKindFromObject extracts group and kind from an object's apiVersion and kind fields.
func groupKindFromObject(obj map[string]interface{}) groupKind {
	var gk groupKind
	if obj == nil {
		return gk
	}
	if kind, ok := obj["kind"].(string); ok && kind != "" {
		gk.Kind = kind
	}
	if apiVersion, ok := obj["apiVersion"].(string); ok && apiVersion != "" {
		// Parse apiVersion (e.g., "apps/v1" -> Group="apps", "v1" -> Group="")
		if idx := strings.Index(apiVersion, "/"); idx >= 0 {
			gk.Group = apiVersion[:idx]
		}
		// Core API has no group prefix (e.g., "v1")
	}
	return gk
}

// EnrichKey fills in missing Kind and Group fields for a CompositeKey
// by looking up the corresponding object in the dump.
func EnrichKey(dump *Dump, key *snapshot.CompositeKey, hash snapshot.VersionHash) {
	if dump == nil || key.ResourceKey.Kind != "" {
		return
	}
	for _, obj := range dump.Objects {
		if obj.Hash.Value == hash.Value {
			gk := groupKindFromObject(obj.Object)
			if gk.Kind != "" {
				key.ResourceKey.Kind = gk.Kind
				key.IdentityKey.Kind = gk.Kind
			}
			if gk.Group != "" {
				key.ResourceKey.Group = gk.Group
				key.IdentityKey.Group = gk.Group
			}
			return
		}
	}
}
