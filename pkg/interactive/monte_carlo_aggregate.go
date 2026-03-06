package interactive

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/tgoodwin/kamera/pkg/analysis"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func aggregateMonteCarloDumpFiles(paths []string) (*analysis.Dump, error) {
	if len(paths) == 0 {
		return nil, fmt.Errorf("no monte-carlo dump paths provided")
	}

	dumps := make([]*analysis.Dump, 0, len(paths))
	for _, path := range paths {
		dump, err := analysis.LoadDump(path)
		if err != nil {
			return nil, fmt.Errorf("load dump %s: %w", path, err)
		}
		dumps = append(dumps, dump)
	}
	return buildMonteCarloAggregateDump(dumps)
}

func buildMonteCarloAggregateDump(dumps []*analysis.Dump) (*analysis.Dump, error) {
	if len(dumps) == 0 {
		return nil, fmt.Errorf("no dumps to aggregate")
	}

	objectIndex := make(map[string]analysis.DumpObject)
	stateIndex := make(map[string]int)
	mergedStates := make([]analysis.DumpResultState, 0)

	for _, dump := range dumps {
		for _, obj := range dump.Objects {
			key := string(obj.Hash.Strategy) + ":" + obj.Hash.Value
			if _, exists := objectIndex[key]; !exists {
				objectIndex[key] = obj
			}
		}
		for _, state := range dump.States {
			key := aggregateStateKey(state)
			idx, exists := stateIndex[key]
			if !exists {
				stateCopy := state
				stateCopy.Paths = cloneDumpPaths(state.Paths)
				ensureDumpStateKindSequences(&stateCopy)
				stateIndex[key] = len(mergedStates)
				mergedStates = append(mergedStates, stateCopy)
				continue
			}
			mergedStates[idx].Paths = append(mergedStates[idx].Paths, cloneDumpPaths(state.Paths)...)
		}
	}

	for i := range mergedStates {
		mergedStates[i].Paths = dedupeDumpPathsByUniqueKey(mergedStates[i].Paths)
	}
	sort.Slice(mergedStates, func(i, j int) bool {
		left := aggregateStateKey(mergedStates[i])
		right := aggregateStateKey(mergedStates[j])
		if left == right {
			return mergedStates[i].ID < mergedStates[j].ID
		}
		return left < right
	})
	for i := range mergedStates {
		mergedStates[i].ID = fmt.Sprintf("state-%d", i)
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

	context := aggregateMonteCarloContext(dumps)
	return &analysis.Dump{
		Context: context,
		Objects: objects,
		States:  mergedStates,
	}, nil
}

func ensureDumpStateKindSequences(state *analysis.DumpResultState) {
	if state == nil {
		return
	}
	if len(state.State.Contents.Objects) == 0 || len(state.State.Contents.KindSequences) > 0 {
		return
	}
	kindSequences := make(tracecheck.KindSequences, len(state.State.Contents.Objects))
	for _, ov := range state.State.Contents.Objects {
		kindSequences[ov.Key.CanonicalGroupKind()] = 0
	}
	state.State.Contents.KindSequences = kindSequences
}

func aggregateMonteCarloContext(dumps []*analysis.Dump) *analysis.DumpContext {
	if len(dumps) == 0 {
		return nil
	}
	base := dumps[0]
	if base == nil || base.Context == nil || base.Context.Scenario == nil {
		return nil
	}

	scenario := base.Context.Scenario
	attrs := cloneStringMap(scenario.Attributes)
	if attrs == nil {
		attrs = map[string]string{}
	}
	attrs["search_mode"] = "monte_carlo"
	attrs["mc_role"] = "aggregate"
	delete(attrs, "mc_trial_index")
	delete(attrs, "mc_seed")

	maxTrialCount := 0
	for _, dump := range dumps {
		if dump == nil || dump.Context == nil || dump.Context.Scenario == nil {
			continue
		}
		count, err := strconv.Atoi(strings.TrimSpace(dump.Context.Scenario.Attributes["mc_trial_count"]))
		if err == nil && count > maxTrialCount {
			maxTrialCount = count
		}
	}
	if maxTrialCount > 0 {
		attrs["mc_trial_count"] = strconv.Itoa(maxTrialCount)
	}
	attrs["mc_trials_aggregated"] = strconv.Itoa(len(dumps))

	return &analysis.DumpContext{
		Scenario: &analysis.DumpScenarioContext{
			Name:       scenario.Name,
			Workflow:   scenario.Workflow,
			InputRef:   scenario.InputRef,
			Attributes: attrs,
		},
	}
}

func aggregateStateKey(state analysis.DumpResultState) string {
	objects := cloneDumpObjectVersions(state.State.Contents.Objects)
	sort.Slice(objects, func(i, j int) bool {
		left, right := objects[i], objects[j]
		if left.Key.ResourceKey.Group != right.Key.ResourceKey.Group {
			return left.Key.ResourceKey.Group < right.Key.ResourceKey.Group
		}
		if left.Key.ResourceKey.Kind != right.Key.ResourceKey.Kind {
			return left.Key.ResourceKey.Kind < right.Key.ResourceKey.Kind
		}
		if left.Key.ResourceKey.Namespace != right.Key.ResourceKey.Namespace {
			return left.Key.ResourceKey.Namespace < right.Key.ResourceKey.Namespace
		}
		if left.Key.ResourceKey.Name != right.Key.ResourceKey.Name {
			return left.Key.ResourceKey.Name < right.Key.ResourceKey.Name
		}
		if left.Key.ObjectID != right.Key.ObjectID {
			return left.Key.ObjectID < right.Key.ObjectID
		}
		if left.Hash.Strategy != right.Hash.Strategy {
			return left.Hash.Strategy < right.Hash.Strategy
		}
		return left.Hash.Value < right.Hash.Value
	})

	pending := cloneDumpPending(state.State.Contents.PendingReconciles)
	sort.Slice(pending, func(i, j int) bool {
		left, right := pending[i], pending[j]
		if left.ReconcilerID != right.ReconcilerID {
			return left.ReconcilerID < right.ReconcilerID
		}
		if left.Namespace != right.Namespace {
			return left.Namespace < right.Namespace
		}
		if left.Name != right.Name {
			return left.Name < right.Name
		}
		return left.Source < right.Source
	})

	payload := struct {
		Objects       []analysis.DumpObjectVersion    `json:"objects"`
		KindSequences tracecheck.KindSequences        `json:"kindSequences,omitempty"`
		Pending       []analysis.DumpPendingReconcile `json:"pending"`
		Error         string                          `json:"error,omitempty"`
	}{
		Objects:       objects,
		KindSequences: state.State.Contents.KindSequences,
		Pending:       pending,
		Error:         strings.TrimSpace(state.Error),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Sprintf("state:%d:%d:%s", len(objects), len(pending), strings.TrimSpace(state.Error))
	}
	return string(data)
}

func dedupeDumpPathsByUniqueKey(paths [][]analysis.DumpReconcileResult) [][]analysis.DumpReconcileResult {
	if len(paths) <= 1 {
		return paths
	}
	seen := make(map[string]struct{}, len(paths))
	out := make([][]analysis.DumpReconcileResult, 0, len(paths))
	for _, path := range paths {
		key := dumpPathUniqueKey(path)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, path)
	}
	return out
}

func dumpPathUniqueKey(path []analysis.DumpReconcileResult) string {
	history := make(tracecheck.ExecutionHistory, 0, len(path))
	for _, step := range path {
		history = append(history, &tracecheck.ReconcileResult{
			ControllerID: tracecheck.ReconcilerID(step.ControllerID),
			StepMetadata: step.StepMetadata,
			Changes: tracecheck.Changes{
				ObjectVersions: dumpObjectVersionsToObjectVersions(step.Changes.ObjectVersions),
				Effects:        step.Changes.Effects,
			},
			Error:             step.Error,
			PendingReconciles: dumpPendingToPending(step.Pending),
		})
	}
	return history.UniqueKey()
}

func dumpObjectVersionsToObjectVersions(values []analysis.DumpObjectVersion) tracecheck.ObjectVersions {
	out := make(tracecheck.ObjectVersions, len(values))
	for _, ov := range values {
		out[ov.Key] = ov.Hash
	}
	return out
}

func dumpPendingToPending(values []analysis.DumpPendingReconcile) []tracecheck.PendingReconcile {
	out := make([]tracecheck.PendingReconcile, 0, len(values))
	for _, pr := range values {
		out = append(out, tracecheck.PendingReconcile{
			ReconcilerID: tracecheck.ReconcilerID(pr.ReconcilerID),
			Request: reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: pr.Namespace,
					Name:      pr.Name,
				},
			},
			Source: pr.Source,
		})
	}
	return out
}

func cloneDumpPaths(paths [][]analysis.DumpReconcileResult) [][]analysis.DumpReconcileResult {
	out := make([][]analysis.DumpReconcileResult, len(paths))
	for i := range paths {
		out[i] = append([]analysis.DumpReconcileResult(nil), paths[i]...)
	}
	return out
}

func cloneDumpObjectVersions(values []analysis.DumpObjectVersion) []analysis.DumpObjectVersion {
	return append([]analysis.DumpObjectVersion(nil), values...)
}

func cloneDumpPending(values []analysis.DumpPendingReconcile) []analysis.DumpPendingReconcile {
	return append([]analysis.DumpPendingReconcile(nil), values...)
}
