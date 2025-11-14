package interactive

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	"github.com/tgoodwin/kamera/pkg/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// SaveInspectorDump serializes the supplied inspector states to the provided path.
func SaveInspectorDump(states []tracecheck.ResultState, path string) error {
	dump, err := buildInspectorDump(states)
	if err != nil {
		return err
	}

	data, err := json.MarshalIndent(dump, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal inspector dump: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("write inspector dump: %w", err)
	}
	return nil
}

// LoadInspectorDump loads inspector state from the specified path.
func LoadInspectorDump(path string) ([]tracecheck.ResultState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read inspector dump: %w", err)
	}

	var dump inspectorDump
	if err := json.Unmarshal(data, &dump); err != nil {
		return nil, fmt.Errorf("unmarshal inspector dump: %w", err)
	}

	return dump.toResultStates()
}

type inspectorDump struct {
	Objects []dumpObject      `json:"objects"`
	States  []dumpResultState `json:"states"`
}

type dumpObject struct {
	Hash   snapshot.VersionHash   `json:"hash"`
	Object map[string]interface{} `json:"object"`
}

type dumpResultState struct {
	ID              string                       `json:"id"`
	Reason          string                       `json:"reason"`
	Error           string                       `json:"error,omitempty"`
	DivergencePoint string                       `json:"divergencePoint"`
	State           dumpStateNode                `json:"state"`
	Paths           [][]dumpReconcileResult      `json:"paths"`
	FailedReconcile *tracecheck.PendingReconcile `json:"failedReconcile,omitempty"`
}

type dumpStateNode struct {
	Contents dumpStateSnapshot `json:"contents"`
}

type dumpStateSnapshot struct {
	Objects       []dumpObjectVersion      `json:"objects"`
	KindSequences tracecheck.KindSequences `json:"kindSequences"`
}

type dumpObjectVersion struct {
	Key  snapshot.CompositeKey `json:"key"`
	Hash snapshot.VersionHash  `json:"hash"`
}

type dumpReconcileResult struct {
	ControllerID  string                   `json:"controllerId"`
	FrameID       string                   `json:"frameId"`
	FrameType     tracecheck.FrameType     `json:"frameType"`
	Changes       dumpChanges              `json:"changes"`
	Error         string                   `json:"error,omitempty"`
	Deltas        []dumpDelta              `json:"deltas,omitempty"`
	StateBefore   []dumpObjectVersion      `json:"stateBefore,omitempty"`
	StateAfter    []dumpObjectVersion      `json:"stateAfter,omitempty"`
	KindSeqBefore tracecheck.KindSequences `json:"kindSeqBefore,omitempty"`
	KindSeqAfter  tracecheck.KindSequences `json:"kindSeqAfter,omitempty"`
}

type dumpChanges struct {
	ObjectVersions []dumpObjectVersion `json:"objectVersions"`
	Effects        []tracecheck.Effect `json:"effects"`
}

type dumpDelta struct {
	Key snapshot.CompositeKey `json:"key"`
	Val string                `json:"value"`
}

func buildInspectorDump(states []tracecheck.ResultState) (*inspectorDump, error) {
	if len(states) == 0 {
		return &inspectorDump{}, nil
	}

	objectIndex := make(map[string]dumpObject)
	var defaultResolver tracecheck.VersionManager
	for _, state := range states {
		if defaultResolver == nil && state.Resolver != nil {
			defaultResolver = state.Resolver
		}
	}

	addHash := func(hash snapshot.VersionHash, resolver tracecheck.VersionManager) error {
		if hash.Value == "" {
			return nil
		}
		key := hashKey(hash)
		if _, exists := objectIndex[key]; exists {
			return nil
		}
		if resolver == nil {
			resolver = defaultResolver
		}
		if resolver == nil {
			return fmt.Errorf("no resolver available for hash %s (%s)", util.ShortenHash(hash.Value), hash.Strategy)
		}
		obj := resolver.Resolve(hash)
		if obj == nil {
			return fmt.Errorf("unable to resolve object for hash %s (%s)", util.ShortenHash(hash.Value), hash.Strategy)
		}
		objectIndex[key] = dumpObject{
			Hash:   hash,
			Object: obj.DeepCopy().Object,
		}
		return nil
	}

	resultStates := make([]dumpResultState, 0, len(states))

	for _, state := range states {
		resolver := state.Resolver
		if resolver == nil {
			resolver = defaultResolver
		}
		if err := collectHashesFromObjectVersions(state.State.Objects(), resolver, addHash); err != nil {
			return nil, err
		}

		dumpState := dumpResultState{
			ID:              state.ID,
			Reason:          state.Reason,
			Error:           state.Error,
			DivergencePoint: state.State.DivergencePoint,
			State: dumpStateNode{
				Contents: dumpStateSnapshot{
					Objects:       toDumpObjectVersions(state.State.Objects(), objectIndex),
					KindSequences: state.State.Contents.KindSequences,
				},
			},
		}
		if state.FailedReconcile != nil {
			copy := *state.FailedReconcile
			dumpState.FailedReconcile = &copy
		}

		paths := make([][]dumpReconcileResult, len(state.Paths))
		for i, path := range state.Paths {
			if len(path) == 0 {
				continue
			}
			pathDump := make([]dumpReconcileResult, 0, len(path))
			for _, step := range path {
				if step == nil {
					continue
				}
				if err := collectReconcileHashes(step, resolver, addHash); err != nil {
					return nil, err
				}
				pathDump = append(pathDump, toDumpReconcileResult(step, objectIndex))
			}
			paths[i] = pathDump
		}
		dumpState.Paths = paths
		resultStates = append(resultStates, dumpState)
	}

	objects := make([]dumpObject, 0, len(objectIndex))
	for _, obj := range objectIndex {
		objects = append(objects, obj)
	}
	sort.Slice(objects, func(i, j int) bool {
		if objects[i].Hash.Strategy == objects[j].Hash.Strategy {
			return objects[i].Hash.Value < objects[j].Hash.Value
		}
		return objects[i].Hash.Strategy < objects[j].Hash.Strategy
	})

	sort.Slice(resultStates, func(i, j int) bool {
		return resultStates[i].ID < resultStates[j].ID
	})

	return &inspectorDump{
		Objects: objects,
		States:  resultStates,
	}, nil
}

func (d inspectorDump) toResultStates() ([]tracecheck.ResultState, error) {
	store := snapshot.NewStore()
	for _, obj := range d.Objects {
		u := &unstructured.Unstructured{Object: obj.Object}
		if err := store.StoreObject(u); err != nil {
			return nil, fmt.Errorf("store object for hash %s: %w", obj.Hash.Value, err)
		}
		if _, ok := store.ResolveWithStrategy(obj.Hash, obj.Hash.Strategy); !ok {
			return nil, fmt.Errorf("stored object hash mismatch for %s (%s)", util.ShortenHash(obj.Hash.Value), obj.Hash.Strategy)
		}
	}
	versionManager := tracecheck.NewVersionStore(store)

	keyResolver := newDumpKeyResolver(d.Objects)

	states := make([]tracecheck.ResultState, len(d.States))
	for i, dumped := range d.States {
		stateNode := tracecheck.StateNode{
			ID: dumped.ID,
			Contents: tracecheck.NewStateSnapshot(
				fromDumpObjectVersions(dumped.State.Contents.Objects, keyResolver),
				dumped.State.Contents.KindSequences,
				nil,
			),
			DivergencePoint: dumped.DivergencePoint,
		}

		paths := make([]tracecheck.ExecutionHistory, len(dumped.Paths))
		for j, path := range dumped.Paths {
			if len(path) == 0 {
				continue
			}
			results := make(tracecheck.ExecutionHistory, 0, len(path))
			for _, dumpedRes := range path {
				step := fromDumpReconcileResult(dumpedRes, keyResolver)
				results = append(results, step)
			}
			paths[j] = results
		}

		var failedCopy *tracecheck.PendingReconcile
		if dumped.FailedReconcile != nil {
			copy := *dumped.FailedReconcile
			failedCopy = &copy
		}

		states[i] = tracecheck.ResultState{
			ID:              dumped.ID,
			Reason:          dumped.Reason,
			Error:           dumped.Error,
			State:           stateNode,
			Paths:           paths,
			FailedReconcile: failedCopy,
			Resolver:        versionManager,
		}
	}

	return states, nil
}

func toDumpReconcileResult(step *tracecheck.ReconcileResult, objIndex map[string]dumpObject) dumpReconcileResult {
	if step == nil {
		return dumpReconcileResult{}
	}
	effects := make([]tracecheck.Effect, len(step.Changes.Effects))
	for i, eff := range step.Changes.Effects {
		eff.Key = ensureKeyKindWithObject(eff.Key, eff.Version, objIndex)
		effects[i] = eff
	}
	return dumpReconcileResult{
		ControllerID: step.ControllerID,
		FrameID:      step.FrameID,
		FrameType:    step.FrameType,
		Changes: dumpChanges{
			ObjectVersions: toDumpObjectVersions(step.Changes.ObjectVersions, objIndex),
			Effects:        effects,
		},
		Error:         step.Error,
		Deltas:        toDumpDeltas(step.Deltas),
		StateBefore:   toDumpObjectVersions(step.StateBefore, objIndex),
		StateAfter:    toDumpObjectVersions(step.StateAfter, objIndex),
		KindSeqBefore: step.KindSeqBefore,
		KindSeqAfter:  step.KindSeqAfter,
	}
}

func fromDumpReconcileResult(dump dumpReconcileResult, resolver *dumpKeyResolver) *tracecheck.ReconcileResult {
	return &tracecheck.ReconcileResult{
		ControllerID: dump.ControllerID,
		FrameID:      dump.FrameID,
		FrameType:    dump.FrameType,
		Changes: tracecheck.Changes{
			ObjectVersions: fromDumpObjectVersions(dump.Changes.ObjectVersions, resolver),
			Effects:        fromDumpEffects(dump.Changes.Effects, resolver),
		},
		Error:         dump.Error,
		Deltas:        fromDumpDeltas(dump.Deltas, resolver),
		StateBefore:   fromDumpObjectVersions(dump.StateBefore, resolver),
		StateAfter:    fromDumpObjectVersions(dump.StateAfter, resolver),
		KindSeqBefore: dump.KindSeqBefore,
		KindSeqAfter:  dump.KindSeqAfter,
	}
}

func toDumpObjectVersions(ov tracecheck.ObjectVersions, objIndex map[string]dumpObject) []dumpObjectVersion {
	if len(ov) == 0 {
		return nil
	}
	keys := make([]snapshot.CompositeKey, 0, len(ov))
	for key := range ov {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].String() < keys[j].String()
	})
	out := make([]dumpObjectVersion, 0, len(keys))
	for _, key := range keys {
		hash := ov[key]
		fixed := ensureKeyKindWithObject(key, hash, objIndex)
		out = append(out, dumpObjectVersion{
			Key:  fixed,
			Hash: hash,
		})
	}
	return out
}

func fromDumpObjectVersions(entries []dumpObjectVersion, resolver *dumpKeyResolver) tracecheck.ObjectVersions {
	if len(entries) == 0 {
		return nil
	}
	out := make(tracecheck.ObjectVersions, len(entries))
	for _, entry := range entries {
		key := entry.Key
		if resolver != nil {
			key = resolver.fixKey(key, entry.Hash)
		}
		out[normalizeCompositeKey(key)] = entry.Hash
	}
	return out
}

func toDumpDeltas(deltas map[snapshot.CompositeKey]tracecheck.Delta) []dumpDelta {
	if len(deltas) == 0 {
		return nil
	}
	keys := make([]snapshot.CompositeKey, 0, len(deltas))
	for key := range deltas {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].String() < keys[j].String()
	})
	out := make([]dumpDelta, 0, len(keys))
	for _, key := range keys {
		out = append(out, dumpDelta{
			Key: key,
			Val: string(deltas[key]),
		})
	}
	return out
}

func fromDumpDeltas(entries []dumpDelta, resolver *dumpKeyResolver) map[snapshot.CompositeKey]tracecheck.Delta {
	if len(entries) == 0 {
		return nil
	}
	out := make(map[snapshot.CompositeKey]tracecheck.Delta, len(entries))
	for _, entry := range entries {
		key := entry.Key
		if resolver != nil {
			key = resolver.fixKey(key, snapshot.VersionHash{})
		}
		out[normalizeCompositeKey(key)] = tracecheck.Delta(entry.Val)
	}
	return out
}

func fromDumpEffects(entries []tracecheck.Effect, resolver *dumpKeyResolver) []tracecheck.Effect {
	if len(entries) == 0 {
		return nil
	}
	out := make([]tracecheck.Effect, len(entries))
	for i, eff := range entries {
		if resolver != nil {
			eff.Key = resolver.fixKey(eff.Key, eff.Version)
		}
		eff.Key = normalizeCompositeKey(eff.Key)
		out[i] = eff
	}
	return out
}

func hashKey(hash snapshot.VersionHash) string {
	return fmt.Sprintf("%s|%s", hash.Strategy, hash.Value)
}

func ensureKeyKindWithObject(key snapshot.CompositeKey, hash snapshot.VersionHash, objIndex map[string]dumpObject) snapshot.CompositeKey {
	if key.ResourceKey.Kind != "" && key.IdentityKey.Kind != "" {
		return key
	}
	if key.ResourceKey.Kind != "" && key.IdentityKey.Kind == "" {
		key.IdentityKey.Kind = key.ResourceKey.Kind
		return key
	}
	if key.IdentityKey.Kind != "" && key.ResourceKey.Kind == "" {
		key.ResourceKey.Kind = key.IdentityKey.Kind
		return key
	}
	if objIndex != nil {
		if obj, ok := objIndex[hashKey(hash)]; ok {
			if gk := groupKindFromObjectData(obj.Object); gk.Kind != "" {
				if key.ResourceKey.Kind == "" {
					key.ResourceKey.Kind = gk.Kind
				}
				if key.IdentityKey.Kind == "" {
					key.IdentityKey.Kind = gk.Kind
				}
				if key.ResourceKey.Group == "" {
					key.ResourceKey.Group = gk.Group
				}
				if key.IdentityKey.Group == "" {
					key.IdentityKey.Group = gk.Group
				}
				return key
			}
		}
	}
	return key
}

func collectHashesFromObjectVersions(ov tracecheck.ObjectVersions, resolver tracecheck.VersionManager, add func(snapshot.VersionHash, tracecheck.VersionManager) error) error {
	for _, hash := range ov {
		if err := add(hash, resolver); err != nil {
			return err
		}
	}
	return nil
}

func collectReconcileHashes(step *tracecheck.ReconcileResult, resolver tracecheck.VersionManager, add func(snapshot.VersionHash, tracecheck.VersionManager) error) error {
	if err := collectHashesFromObjectVersions(step.Changes.ObjectVersions, resolver, add); err != nil {
		return err
	}
	for _, eff := range step.Changes.Effects {
		if err := add(eff.Version, resolver); err != nil {
			return err
		}
	}
	if err := collectHashesFromObjectVersions(step.StateBefore, resolver, add); err != nil {
		return err
	}
	if err := collectHashesFromObjectVersions(step.StateAfter, resolver, add); err != nil {
		return err
	}
	return nil
}

func normalizeCompositeKey(key snapshot.CompositeKey) snapshot.CompositeKey {
	kind := key.ResourceKey.Kind
	if kind == "" {
		kind = key.IdentityKey.Kind
	}
	if kind == "" {
		return key
	}
	group := key.ResourceKey.Group
	if group == "" {
		group = key.IdentityKey.Group
	}
	return snapshot.NewCompositeKeyWithGroup(
		group,
		kind,
		key.ResourceKey.Namespace,
		key.ResourceKey.Name,
		key.IdentityKey.ObjectID,
	)
}

type dumpKeyResolver struct {
	hashKinds     map[string]schema.GroupKind
	objectIDKinds map[string]schema.GroupKind
	resourceKinds map[string]schema.GroupKind
}

func newDumpKeyResolver(objects []dumpObject) *dumpKeyResolver {
	if len(objects) == 0 {
		return nil
	}

	resolver := &dumpKeyResolver{
		hashKinds:     make(map[string]schema.GroupKind, len(objects)),
		objectIDKinds: make(map[string]schema.GroupKind),
		resourceKinds: make(map[string]schema.GroupKind),
	}

	for _, obj := range objects {
		gk := groupKindFromObjectData(obj.Object)
		if gk.Kind == "" {
			continue
		}
		if obj.Hash.Value != "" {
			resolver.hashKinds[hashKey(obj.Hash)] = gk
		}

		metadata := asMap(obj.Object["metadata"])
		if metadata == nil {
			continue
		}

		if name := stringFromMap(metadata, "name"); name != "" {
			namespace := stringFromMap(metadata, "namespace")
			resolver.addResourceKind(namespace, name, gk)
		}

		if objectID := resolveObjectID(metadata); objectID != "" {
			resolver.addObjectIDKind(objectID, gk)
		}
	}

	return resolver
}

func (r *dumpKeyResolver) fixKey(key snapshot.CompositeKey, hash snapshot.VersionHash) snapshot.CompositeKey {
	if r == nil {
		return key
	}
	if key.ResourceKey.Kind != "" && key.IdentityKey.Kind != "" {
		return key
	}

	kind := key.ResourceKey.Kind
	if kind == "" {
		kind = key.IdentityKey.Kind
	}
	group := key.ResourceKey.Group
	if group == "" {
		group = key.IdentityKey.Group
	}

	if kind == "" && hash.Value != "" {
		if resolved, ok := r.hashKinds[hashKey(hash)]; ok && resolved.Kind != "" {
			if kind == "" {
				kind = resolved.Kind
			}
			if group == "" {
				group = resolved.Group
			}
		}
	}

	if kind == "" && key.IdentityKey.ObjectID != "" {
		if resolved, ok := r.objectIDKinds[key.IdentityKey.ObjectID]; ok && resolved.Kind != "" {
			if kind == "" {
				kind = resolved.Kind
			}
			if group == "" {
				group = resolved.Group
			}
		}
	}

	if kind == "" && key.ResourceKey.Name != "" {
		if resolved, ok := r.resourceKinds[namespacedNameKey(key.ResourceKey.Namespace, key.ResourceKey.Name)]; ok && resolved.Kind != "" {
			if kind == "" {
				kind = resolved.Kind
			}
			if group == "" {
				group = resolved.Group
			}
		}
	}

	if kind == "" {
		return key
	}

	if key.ResourceKey.Kind == "" {
		key.ResourceKey.Kind = kind
	}
	if key.IdentityKey.Kind == "" {
		key.IdentityKey.Kind = kind
	}
	if key.ResourceKey.Group == "" {
		key.ResourceKey.Group = group
	}
	if key.IdentityKey.Group == "" {
		key.IdentityKey.Group = group
	}

	return key
}

func (r *dumpKeyResolver) addObjectIDKind(id string, gk schema.GroupKind) {
	if r == nil || id == "" || gk.Kind == "" {
		return
	}
	if existing, ok := r.objectIDKinds[id]; ok && existing.Kind != "" && existing != gk {
		r.objectIDKinds[id] = schema.GroupKind{}
		return
	}
	r.objectIDKinds[id] = gk
}

func (r *dumpKeyResolver) addResourceKind(namespace, name string, gk schema.GroupKind) {
	if r == nil || name == "" || gk.Kind == "" {
		return
	}
	key := namespacedNameKey(namespace, name)
	if existing, ok := r.resourceKinds[key]; ok && existing.Kind != "" && existing != gk {
		r.resourceKinds[key] = schema.GroupKind{}
		return
	}
	r.resourceKinds[key] = gk
}

func namespacedNameKey(namespace, name string) string {
	return namespace + "/" + name
}

func resolveObjectID(metadata map[string]interface{}) string {
	if id := stringFromNestedMap(metadata, "labels", tag.TraceyObjectID); id != "" {
		return id
	}
	if id := stringFromNestedMap(metadata, "annotations", tag.TraceyObjectID); id != "" {
		return id
	}
	return stringFromMap(metadata, "uid")
}

func asMap(value interface{}) map[string]interface{} {
	if value == nil {
		return nil
	}
	if m, ok := value.(map[string]interface{}); ok {
		return m
	}
	return nil
}

func stringFromMap(m map[string]interface{}, key string) string {
	if m == nil {
		return ""
	}
	if val, ok := m[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}

func stringFromNestedMap(m map[string]interface{}, key, nestedKey string) string {
	nested := asMap(m[key])
	if nested == nil {
		return ""
	}
	if val, ok := nested[nestedKey]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}

func groupKindFromObjectData(obj map[string]interface{}) schema.GroupKind {
	var gk schema.GroupKind
	if obj == nil {
		return gk
	}
	if kind, ok := obj["kind"].(string); ok && kind != "" {
		gk.Kind = kind
	}
	if apiVersion, ok := obj["apiVersion"].(string); ok && apiVersion != "" {
		if gv, err := schema.ParseGroupVersion(apiVersion); err == nil {
			gk.Group = gv.Group
		}
	}
	metadata := asMap(obj["metadata"])
	if metadata == nil {
		return gk
	}
	if gk.Kind == "" {
		if kind := stringFromNestedMap(metadata, "annotations", "kind"); kind != "" {
			gk.Kind = kind
		} else if kind := stringFromNestedMap(metadata, "labels", "kind"); kind != "" {
			gk.Kind = kind
		}
	}
	if gk.Group == "" {
		if apiVersion := stringFromMap(metadata, "apiVersion"); apiVersion != "" {
			if gv, err := schema.ParseGroupVersion(apiVersion); err == nil {
				gk.Group = gv.Group
			}
		}
	}
	return gk
}
