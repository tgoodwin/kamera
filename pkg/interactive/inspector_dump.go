package interactive

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"

	"github.com/tgoodwin/kamera/pkg/analysis"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	"github.com/tgoodwin/kamera/pkg/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// InspectorDumpContext carries optional scenario metadata stored with a dump file.
type InspectorDumpContext struct {
	ScenarioName     string
	ScenarioRunIndex *int
	Workflow         string
	InputRef         string
	Attributes       map[string]string
}

// SaveInspectorDump serializes the supplied inspector states to the provided path using the resolver to materialize objects.
func SaveInspectorDump(states []tracecheck.ResultState, resolver tracecheck.VersionManager, path string) error {
	return SaveInspectorDumpWithContext(states, resolver, path, nil)
}

// SaveInspectorDumpWithContext serializes inspector states and includes optional scenario metadata.
func SaveInspectorDumpWithContext(states []tracecheck.ResultState, resolver tracecheck.VersionManager, path string, ctx *InspectorDumpContext) error {
	return SaveInspectorDumpWithContextAndStatsAndCampaignMetrics(states, resolver, path, ctx, nil, nil)
}

// SaveInspectorDumpWithContextAndStats serializes inspector states, optional scenario metadata,
// and optional exploration stats.
func SaveInspectorDumpWithContextAndStats(
	states []tracecheck.ResultState,
	resolver tracecheck.VersionManager,
	path string,
	ctx *InspectorDumpContext,
	stats *tracecheck.ExploreStats,
) error {
	return SaveInspectorDumpWithContextAndStatsAndCampaignMetrics(states, resolver, path, ctx, stats, nil)
}

// SaveInspectorDumpWithContextAndStatsAndCampaignMetrics serializes inspector states,
// optional scenario metadata, optional exploration stats, and optional campaign metrics.
func SaveInspectorDumpWithContextAndStatsAndCampaignMetrics(
	states []tracecheck.ResultState,
	resolver tracecheck.VersionManager,
	path string,
	ctx *InspectorDumpContext,
	stats *tracecheck.ExploreStats,
	campaignMetrics *analysis.CampaignMetrics,
) error {
	dump, err := buildInspectorDump(states, resolver, ctx, stats, campaignMetrics)
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

// LoadInspectorDump loads inspector state from the specified path and reconstructs a resolver for inspection.
func LoadInspectorDump(path string) ([]tracecheck.ResultState, tracecheck.VersionManager, error) {
	dump, err := analysis.LoadDump(path)
	if err != nil {
		return nil, nil, fmt.Errorf("load inspector dump: %w", err)
	}

	return dumpToResultStates(dump)
}

func buildInspectorDump(
	states []tracecheck.ResultState,
	resolver tracecheck.VersionManager,
	context *InspectorDumpContext,
	stats *tracecheck.ExploreStats,
	campaignMetrics *analysis.CampaignMetrics,
) (*analysis.Dump, error) {
	if len(states) == 0 {
		return &analysis.Dump{
			Context:         buildAnalysisDumpContext(context),
			Stats:           stats,
			CampaignMetrics: campaignMetrics,
		}, nil
	}

	if resolver == nil {
		return nil, fmt.Errorf("version resolver is required to build an inspector dump")
	}

	objectIndex := make(map[string]analysis.DumpObject)

	addHash := func(hash snapshot.VersionHash) error {
		if hash.Value == "" {
			return nil
		}
		key := hashKey(hash)
		if _, exists := objectIndex[key]; exists {
			return nil
		}
		obj := resolver.Resolve(hash)
		if obj == nil {
			return fmt.Errorf("unable to resolve object for hash %s (%s)", util.ShortenHash(hash.Value), hash.Strategy)
		}
		objectIndex[key] = analysis.DumpObject{
			Hash:   hash,
			Object: obj.DeepCopy().Object,
		}
		return nil
	}

	resultStates := make([]analysis.DumpResultState, 0, len(states))

	for _, state := range states {
		if err := collectHashesFromObjectVersions(state.State.Objects(), addHash); err != nil {
			return nil, err
		}

		errMsg := ""
		if state.Error != nil {
			errMsg = state.Error.Error()
		}
		dumpState := analysis.DumpResultState{
			ID:              state.ID,
			Error:           errMsg,
			DivergencePoint: state.State.DivergencePoint,
			State: analysis.DumpStateNode{
				Contents: analysis.DumpStateSnapshot{
					Objects:           toDumpObjectVersions(state.State.Objects(), objectIndex),
					KindSequences:     state.State.Contents.KindSequences,
					PendingReconciles: toDumpPendingReconciles(state.State.PendingReconciles),
				},
			},
		}

		paths := make([][]analysis.DumpReconcileResult, len(state.Paths))
		for i, path := range state.Paths {
			if len(path) == 0 {
				continue
			}
			pathDump := make([]analysis.DumpReconcileResult, 0, len(path))
			for _, step := range path {
				if step == nil {
					continue
				}
				if err := collectReconcileHashes(step, addHash); err != nil {
					return nil, err
				}
				pathDump = append(pathDump, toDumpReconcileResult(step, objectIndex))
			}
			paths[i] = pathDump
		}
		dumpState.Paths = paths
		resultStates = append(resultStates, dumpState)
	}

	objects := make([]analysis.DumpObject, 0, len(objectIndex))
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

	return &analysis.Dump{
		Context:         buildAnalysisDumpContext(context),
		Stats:           stats,
		CampaignMetrics: campaignMetrics,
		Objects:         objects,
		States:          resultStates,
	}, nil
}

func buildAnalysisDumpContext(ctx *InspectorDumpContext) *analysis.DumpContext {
	if ctx == nil {
		return nil
	}
	if ctx.ScenarioName == "" && ctx.ScenarioRunIndex == nil && ctx.Workflow == "" && ctx.InputRef == "" && len(ctx.Attributes) == 0 {
		return nil
	}

	attributes := make(map[string]string, len(ctx.Attributes))
	for key, value := range ctx.Attributes {
		attributes[key] = value
	}
	if len(attributes) == 0 {
		attributes = nil
	}

	var runIndex *int
	if ctx.ScenarioRunIndex != nil {
		indexCopy := *ctx.ScenarioRunIndex
		runIndex = &indexCopy
	}

	return &analysis.DumpContext{
		Scenario: &analysis.DumpScenarioContext{
			Name:       ctx.ScenarioName,
			RunIndex:   runIndex,
			Workflow:   ctx.Workflow,
			InputRef:   ctx.InputRef,
			Attributes: attributes,
		},
	}
}

func dumpToResultStates(d *analysis.Dump) ([]tracecheck.ResultState, tracecheck.VersionManager, error) {
	store := snapshot.NewStore()
	for _, obj := range d.Objects {
		u := &unstructured.Unstructured{Object: obj.Object}
		if err := store.StoreObject(u); err != nil {
			return nil, nil, fmt.Errorf("store object for hash %s: %w", obj.Hash.Value, err)
		}
		if _, ok := store.ResolveWithStrategy(obj.Hash, obj.Hash.Strategy); !ok {
			return nil, nil, fmt.Errorf("stored object hash mismatch for %s (%s)", util.ShortenHash(obj.Hash.Value), obj.Hash.Strategy)
		}
	}
	versionManager := tracecheck.NewVersionStore(store, nil)

	keyResolver := newDumpKeyResolver(d.Objects)

	states := make([]tracecheck.ResultState, len(d.States))
	for i, dumped := range d.States {
		var errVal error
		if dumped.Error != "" {
			errVal = errors.New(dumped.Error)
		}
		stateNode := tracecheck.StateNode{
			ID: dumped.ID,
			Contents: tracecheck.NewStateSnapshot(
				fromDumpObjectVersions(dumped.State.Contents.Objects, keyResolver),
				dumped.State.Contents.KindSequences,
				nil,
			),
			PendingReconciles: fromDumpPendingReconciles(dumped.State.Contents.PendingReconciles),
			DivergencePoint:   dumped.DivergencePoint,
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

		states[i] = tracecheck.ResultState{
			ID:    dumped.ID,
			Error: errVal,
			State: stateNode,
			Paths: paths,
		}
	}

	return states, versionManager, nil
}

func toDumpReconcileResult(step *tracecheck.ReconcileResult, objIndex map[string]analysis.DumpObject) analysis.DumpReconcileResult {
	if step == nil {
		return analysis.DumpReconcileResult{}
	}
	effects := make([]tracecheck.Effect, len(step.Changes.Effects))
	for i, eff := range step.Changes.Effects {
		eff.Key = ensureKeyKindWithObject(eff.Key, eff.Version, objIndex)
		effects[i] = eff
	}
	observations := make([]tracecheck.Effect, len(step.Changes.Observations))
	for i, obs := range step.Changes.Observations {
		obs.Key = ensureKeyKindWithObject(obs.Key, obs.Version, objIndex)
		observations[i] = obs
	}

	// Compute ContentsHash for StateAfter to enable DAG cross-referencing
	var contentsHashAfter string
	if len(step.StateAfter) > 0 {
		stateNode := tracecheck.StateNode{
			Contents: tracecheck.NewStateSnapshot(step.StateAfter, step.KindSeqAfter, nil),
		}
		contentsHashAfter = string(stateNode.ContentsHash())
	}

	return analysis.DumpReconcileResult{
		ControllerID:      string(step.ControllerID),
		ContentsHashAfter: contentsHashAfter,
		FrameID:           step.FrameID,
		FrameType:         step.FrameType,
		StepMetadata:      step.StepMetadata,
		Changes: analysis.DumpChanges{
			ObjectVersions: toDumpObjectVersions(step.Changes.ObjectVersions, objIndex),
			Effects:        effects,
			Observations:   observations,
		},
		Error:         step.Error,
		Deltas:        toDumpDeltas(step.Deltas, step.Changes.ObjectVersions),
		StateBefore:   toDumpObjectVersions(step.StateBefore, objIndex),
		StateAfter:    toDumpObjectVersions(step.StateAfter, objIndex),
		KindSeqBefore: step.KindSeqBefore,
		KindSeqAfter:  step.KindSeqAfter,
		Pending:       toDumpPendingReconciles(step.PendingReconciles),
		StalenessInfo: step.StalenessInfo,
	}
}

func fromDumpReconcileResult(dump analysis.DumpReconcileResult, resolver *dumpKeyResolver) *tracecheck.ReconcileResult {
	return &tracecheck.ReconcileResult{
		ControllerID: tracecheck.ReconcilerID(dump.ControllerID),
		FrameID:      dump.FrameID,
		FrameType:    dump.FrameType,
		StepMetadata: dump.StepMetadata,
		Changes: tracecheck.Changes{
			ObjectVersions: fromDumpObjectVersions(dump.Changes.ObjectVersions, resolver),
			Effects:        fromDumpEffects(dump.Changes.Effects, resolver),
			Observations:   fromDumpEffects(dump.Changes.Observations, resolver),
		},
		Error:             dump.Error,
		Deltas:            fromDumpDeltas(dump.Deltas, resolver),
		StateBefore:       fromDumpObjectVersions(dump.StateBefore, resolver),
		StateAfter:        fromDumpObjectVersions(dump.StateAfter, resolver),
		KindSeqBefore:     dump.KindSeqBefore,
		KindSeqAfter:      dump.KindSeqAfter,
		PendingReconciles: fromDumpPendingReconciles(dump.Pending),
		StalenessInfo:     dump.StalenessInfo,
	}
}

func toDumpObjectVersions(ov tracecheck.ObjectVersions, objIndex map[string]analysis.DumpObject) []analysis.DumpObjectVersion {
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
	out := make([]analysis.DumpObjectVersion, 0, len(keys))
	for _, key := range keys {
		hash := ov[key]
		fixed := ensureKeyKindWithObject(key, hash, objIndex)
		out = append(out, analysis.DumpObjectVersion{
			Key:  fixed,
			Hash: hash,
		})
	}
	return out
}

func fromDumpObjectVersions(entries []analysis.DumpObjectVersion, resolver *dumpKeyResolver) tracecheck.ObjectVersions {
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

func toDumpDeltas(deltas map[snapshot.CompositeKey]tracecheck.Delta, objectVersions tracecheck.ObjectVersions) []analysis.DumpDelta {
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
	out := make([]analysis.DumpDelta, 0, len(keys))
	for _, key := range keys {
		hash := objectVersions[key]
		out = append(out, analysis.DumpDelta{
			Key:  key,
			Hash: hash,
			Val:  string(deltas[key]),
		})
	}
	return out
}

func fromDumpDeltas(entries []analysis.DumpDelta, resolver *dumpKeyResolver) map[snapshot.CompositeKey]tracecheck.Delta {
	if len(entries) == 0 {
		return nil
	}
	out := make(map[snapshot.CompositeKey]tracecheck.Delta, len(entries))
	for _, entry := range entries {
		key := entry.Key
		if resolver != nil {
			key = resolver.fixKey(key, entry.Hash)
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

func toDumpPendingReconciles(pending []tracecheck.PendingReconcile) []analysis.DumpPendingReconcile {
	if len(pending) == 0 {
		return nil
	}
	out := make([]analysis.DumpPendingReconcile, len(pending))
	for i, pr := range pending {
		out[i] = analysis.DumpPendingReconcile{
			ReconcilerID: string(pr.ReconcilerID),
			Namespace:    pr.Request.Namespace,
			Name:         pr.Request.Name,
			Source:       pr.Source,
		}
	}
	return out
}

func fromDumpPendingReconciles(entries []analysis.DumpPendingReconcile) []tracecheck.PendingReconcile {
	if len(entries) == 0 {
		return nil
	}
	out := make([]tracecheck.PendingReconcile, len(entries))
	for i, pr := range entries {
		out[i] = tracecheck.PendingReconcile{
			ReconcilerID: tracecheck.ReconcilerID(pr.ReconcilerID),
			Request: reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: pr.Namespace,
					Name:      pr.Name,
				},
			},
			Source: pr.Source,
		}
	}
	return out
}

func hashKey(hash snapshot.VersionHash) string {
	return fmt.Sprintf("%s|%s", hash.Strategy, hash.Value)
}

func ensureKeyKindWithObject(key snapshot.CompositeKey, hash snapshot.VersionHash, objIndex map[string]analysis.DumpObject) snapshot.CompositeKey {
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

func collectHashesFromObjectVersions(ov tracecheck.ObjectVersions, add func(snapshot.VersionHash) error) error {
	for _, hash := range ov {
		if err := add(hash); err != nil {
			return err
		}
	}
	return nil
}

func collectReconcileHashes(step *tracecheck.ReconcileResult, add func(snapshot.VersionHash) error) error {
	if err := collectHashesFromObjectVersions(step.Changes.ObjectVersions, add); err != nil {
		return err
	}
	for _, eff := range step.Changes.Effects {
		if err := add(eff.Version); err != nil {
			return err
		}
	}
	for _, obs := range step.Changes.Observations {
		if err := add(obs.Version); err != nil {
			return err
		}
	}
	if err := collectHashesFromObjectVersions(step.StateBefore, add); err != nil {
		return err
	}
	if err := collectHashesFromObjectVersions(step.StateAfter, add); err != nil {
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

func newDumpKeyResolver(objects []analysis.DumpObject) *dumpKeyResolver {
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
