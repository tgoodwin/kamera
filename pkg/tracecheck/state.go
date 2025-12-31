package tracecheck

import (
	"fmt"
	"maps"
	"os"
	"slices"
	"sort"
	"strings"

	"github.com/samber/lo"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/util"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type HashInfo struct {
	DefaultHash    snapshot.VersionHash
	AnonymizedHash snapshot.VersionHash
}

// ObjectVersions is a map of object IDs to their version hashes
type ObjectVersions map[snapshot.CompositeKey]snapshot.VersionHash

func (ov ObjectVersions) Equals(other ObjectVersions) bool {
	if len(ov) != len(other) {
		return false
	}
	for key, value := range ov {
		if otherValue, exists := other[key]; !exists || otherValue != value {
			return false
		}
	}
	return true
}

func (ov ObjectVersions) HasNamespacedNameForKind(key snapshot.ResourceKey) (snapshot.CompositeKey, bool) {
	for compositeKey := range ov {
		if compositeKey.ResourceKey == key {
			return compositeKey, true
		}
	}
	return snapshot.CompositeKey{}, false
}

func (ov ObjectVersions) Objects() ObjectVersions {
	return ov
}

func (ov ObjectVersions) DumpContents() {
	for key, value := range ov {
		fmt.Printf("\t%s:%s\n", key, util.ShortenHash(value.Value))
	}
}

func (ov ObjectVersions) Summarize() {
	// sort by key first
	keys := lo.Keys(ov)
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].ObjectID < keys[j].ObjectID
	})
	for _, key := range keys {
		fmt.Printf("\t%s:%s\n", key, util.ShortenHash(ov[key].Value))
	}
}

type Delta string

type FrameType string

const (
	FrameTypeReplay  FrameType = "replay"
	FrameTypeExplore FrameType = "explore"
)

type Changes struct {
	ObjectVersions ObjectVersions
	Effects        []Effect
}

type ReconcileResult struct {
	ControllerID ReconcilerID
	FrameID      string
	FrameType    FrameType
	Changes      Changes // this is just the writeset, not the resulting full state of the world
	Deltas       map[snapshot.CompositeKey]Delta
	Error        string

	StateBefore   ObjectVersions
	StateAfter    ObjectVersions
	KindSeqBefore KindSequences
	KindSeqAfter  KindSequences

	PendingReconciles []PendingReconcile // Pending reconciles produced by this step

	ctrlRes reconcile.Result
}

func (r *ReconcileResult) wasNoOp() bool {
	return r != nil && len(r.Changes.ObjectVersions) == 0 && r.Error == ""
}

type ExecutionHistory []*ReconcileResult

func (eh ExecutionHistory) UniqueKey() string {
	// Determine if the original path converged before filtering.
	// Convergence occurs when:
	// 1. The last step has 0 pending reconciles, OR
	// 2. All remaining pending reconciles are ignorable for convergence (async enqueues/requeues)
	originalConverged := false
	if len(eh) > 0 {
		lastStep := eh[len(eh)-1]
		originalConverged = len(lastStep.PendingReconciles) == 0 ||
			allPendingIgnorableForConvergence(lastStep.PendingReconciles)
	}

	// Filter out no-ops (steps with no changes and no errors)
	filterNoOps := lo.Filter(eh, func(r *ReconcileResult, _ int) bool {
		return len(r.Changes.ObjectVersions) > 0 || r.Error != ""
	})

	strComponents := lo.Map(filterNoOps, func(r *ReconcileResult, idx int) string {
		suffix := ""
		if r.Error != "" {
			suffix = "!"
		}
		// Include convergence marker on the last step if the original path converged.
		// This ensures paths ending in convergence are not considered equivalent
		// to paths that were cut off (e.g., due to max depth).
		if idx == len(filterNoOps)-1 && originalConverged {
			suffix += ":converged"
		}
		return fmt.Sprintf("%s@%d%s", r.ControllerID, len(r.Changes.Effects), suffix)
	})
	return strings.Join(strComponents, ",")
}

func (eh ExecutionHistory) SummarizeToFile(file *os.File) error {
	for _, r := range eh {
		_, err := fmt.Fprintf(file, "\t%s:%s (%s) - #changes=%d\n", r.ControllerID, util.Shorter(r.FrameID), r.FrameType, len(r.Changes.ObjectVersions))
		if err != nil {
			return err
		}
		if r.Error != "" {
			if _, err := fmt.Fprintf(file, "\tError: %s\n", r.Error); err != nil {
				return err
			}
		}
		for _, effect := range r.Changes.Effects {
			if _, err := fmt.Fprintf(file, "\t%s: %s\n", effect.OpType, effect.Key); err != nil {
				return err
			}
			if _, hasDelta := r.Deltas[effect.Key]; hasDelta {
				_, err := fmt.Fprintf(file, "\t%s: %s\n", effect.Key, r.Deltas[effect.Key])
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (eh ExecutionHistory) Summarize() {
	err := eh.SummarizeToFile(os.Stdout)
	if err != nil {
		fmt.Printf("Error summarizing to stdout: %v\n", err)
	}
}

func (eh ExecutionHistory) FilterNoOps() ExecutionHistory {
	var filtered ExecutionHistory
	for _, r := range eh {
		// Keep reconciles that:
		// 1. Have effects (state changes)
		// 2. Have errors
		// No-op steps (no changes, no errors) are filtered out.
		// Convergence is determined by whether the path completed, not by a special no-op step.
		if len(r.Changes.ObjectVersions) > 0 || r.Error != "" {
			filtered = append(filtered, r)
		}
	}
	return filtered
}

func DebugPaths(paths []ExecutionHistory) {
	for i, path := range paths {
		fmt.Printf("Path %d:\n", i+1)
		pathParts := lo.Map(path, func(r *ReconcileResult, _ int) string {
			return fmt.Sprintf("%s:%d", r.ControllerID, len(r.Changes.Effects))
		})
		fmt.Println("\t" + strings.Join(pathParts, " -> "))
	}
}

// normalizeNoOpSuffix sorts the trailing no-op reconciles (no changes, no errors)
// so that exploration order does not create spurious path permutations when the
// remaining work is idempotent.
func normalizeNoOpSuffix(path ExecutionHistory) ExecutionHistory {
	cut := len(path)
	for cut > 0 {
		r := path[cut-1]
		if len(r.Changes.ObjectVersions) == 0 && r.Error == "" {
			cut--
			continue
		}
		break
	}

	// No no-op suffix or only a single no-op reconcile to reorder.
	if cut >= len(path)-1 {
		return path
	}

	normalized := make(ExecutionHistory, 0, len(path))
	normalized = append(normalized, path[:cut]...)

	suffix := slices.Clone(path[cut:])
	sort.SliceStable(suffix, func(i, j int) bool {
		if suffix[i].ControllerID != suffix[j].ControllerID {
			return suffix[i].ControllerID < suffix[j].ControllerID
		}
		return len(suffix[i].PendingReconciles) < len(suffix[j].PendingReconciles)
	})

	normalized = append(normalized, suffix...)
	return normalized
}

// normalizeAndDedupePaths keeps full execution histories (including no-ops) but
// normalizes trailing no-op reconciles to a deterministic order and removes
// duplicate paths that would otherwise differ only by those no-op permutations.
func normalizeAndDedupePaths(paths []ExecutionHistory) []ExecutionHistory {
	seen := make(map[string]struct{})
	deduped := make([]ExecutionHistory, 0, len(paths))

	for _, path := range paths {
		normalized := normalizeNoOpSuffix(path)
		sigParts := make([]string, len(normalized))
		for i, r := range normalized {
			sigParts[i] = fmt.Sprintf("%s@%d", r.ControllerID, len(r.Deltas))
		}
		sig := strings.Join(sigParts, ",")
		if _, ok := seen[sig]; ok {
			continue
		}
		seen[sig] = struct{}{}
		deduped = append(deduped, normalized)
	}

	return deduped
}

func getUniquePaths(paths []ExecutionHistory) []ExecutionHistory {
	return lo.UniqBy(paths, func(path ExecutionHistory) string {
		return path.UniqueKey()
	})
}

func GetUniquePaths(paths []ExecutionHistory) []ExecutionHistory {
	normalized := lo.Map(paths, func(path ExecutionHistory, _ int) ExecutionHistory {
		return normalizeNoOpSuffix(path)
	})

	// Deduplicate based on the filtered key (ignoring no-ops), but return the
	// full normalized paths so the inspector can still display no-op steps.
	unique := make([]ExecutionHistory, 0, len(normalized))
	seen := make(map[string]struct{}, len(normalized))

	for _, path := range normalized {
		if len(path) == 0 {
			continue
		}
		// If the path is entirely no-ops, skip it as before.
		if len(path.FilterNoOps()) == 0 {
			continue
		}
		key := path.UniqueKey()
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, path)
	}

	return unique
}

type ObservableState interface {
	Objects() ObjectVersions
}

type StateNode struct {
	ID       string
	Contents StateSnapshot
	// PendingReconciles is a list of controller IDs that are pending reconciliation.
	// In our "game tree", they represent branches that we can explore.
	PendingReconciles []PendingReconcile

	parent *StateNode
	action *ReconcileResult // the action that led to this state

	// ExecutionHistory tracks the sequence of reconciles that led to this state
	ExecutionHistory ExecutionHistory

	// used to track children of a divergence point of interest
	divergenceKey StateHash

	depth int

	DivergencePoint string // reconcileID of the first divergence

	// tracks what KindSequences a controller may be "stuck" on
	// e.g. if a controller's watches are connected to a partitioned APIServer
	stuckReconcilerPositions map[ReconcilerID]KindSequences
}

func (sn StateNode) ObserveAs(reconcilerID ReconcilerID) ObjectVersions {
	if sn.stuckReconcilerPositions == nil {
		return sn.Contents.All()
	}
	// return the objects that this reconciler can see
	if _, ok := sn.stuckReconcilerPositions[reconcilerID]; ok {
		kindSequences := maps.Clone(sn.Contents.KindSequences)
		for k, stuckSeq := range sn.stuckReconcilerPositions[reconcilerID] {
			kindSequences[k] = stuckSeq
		}
		return sn.Contents.ObserveAt(kindSequences)
	}
	return sn.Contents.All()
}

func (sn StateNode) DumpPending() {
	for _, pr := range sn.PendingReconciles {
		fmt.Printf("\tpending:%s\n", pr)
	}
}

func (sn StateNode) IsConverged() bool {
	return len(sn.PendingReconciles) == 0
}

func (sn StateNode) Objects() ObjectVersions {
	return sn.Contents.All()
}

func (sn StateNode) Summarize() {
	// TODO
	fmt.Printf("---------StateNode Summary: depth %d---------\n", sn.depth)
	if sn.parent == nil {
		fmt.Println("Top-Level StateNode")
	}

	// print the controller that created this state
	if sn.action != nil {
		fmt.Println("ControllerID: ", sn.action.ControllerID)
		fmt.Println("Num Changes: ", len(sn.action.Changes.ObjectVersions))
		fmt.Println("Pending Reconciles: ", sn.PendingReconciles)
	}
}

func (sn StateNode) SummarizeFromRoot() {
	if sn.parent != nil {
		sn.parent.SummarizeFromRoot()
	} else {
		fmt.Println("Root StateNode")
	}
	sn.Summarize()
}

func (sn StateNode) Clone() StateNode {
	return StateNode{
		ID:                sn.ID,
		Contents:          sn.Contents, // assuming Contents is immutable or has copy-on-write semantics
		PendingReconciles: slices.Clone(sn.PendingReconciles),
		parent:            sn.parent,
		action:            sn.action,
		ExecutionHistory:  slices.Clone(sn.ExecutionHistory),
		depth:             sn.depth,
		DivergencePoint:   sn.DivergencePoint, // TODO deprecate
		divergenceKey:     sn.divergenceKey,

		stuckReconcilerPositions: maps.Clone(sn.stuckReconcilerPositions),
	}
}

// WithDepth returns a copy of the state node with the provided depth set.
func (sn StateNode) WithDepth(d int) StateNode {
	sn.depth = d
	return sn
}

func (sn StateNode) serialize(reconcileOrderSensitive bool) string {
	// collect and sort object keys for deterministic ordering. Multiple
	// resources can share the same sleeve ObjectID, so compare on the full
	// composite key to avoid unstable ordering across runs.
	objectKeys := make([]snapshot.CompositeKey, 0, len(sn.Objects()))
	for objKey := range sn.Objects() {
		objectKeys = append(objectKeys, objKey)
	}
	sort.Slice(objectKeys, func(i, j int) bool {
		ai, aj := objectKeys[i], objectKeys[j]
		if ai.ResourceKey.Group != aj.ResourceKey.Group {
			return ai.ResourceKey.Group < aj.ResourceKey.Group
		}
		if ai.ResourceKey.Kind != aj.ResourceKey.Kind {
			return ai.ResourceKey.Kind < aj.ResourceKey.Kind
		}
		if ai.ResourceKey.Namespace != aj.ResourceKey.Namespace {
			return ai.ResourceKey.Namespace < aj.ResourceKey.Namespace
		}
		if ai.ResourceKey.Name != aj.ResourceKey.Name {
			return ai.ResourceKey.Name < aj.ResourceKey.Name
		}
		return ai.ObjectID < aj.ObjectID
	})

	// collect pending reconciles (and sort if not order-sensitive)
	pending := sn.PendingReconciles
	if !reconcileOrderSensitive && len(pending) > 1 {
		pending = slices.Clone(pending)
		sort.Slice(pending, func(i, j int) bool {
			pi, pj := pending[i], pending[j]
			if pi.ReconcilerID != pj.ReconcilerID {
				return pi.ReconcilerID < pj.ReconcilerID
			}
			if pi.Request.Namespace != pj.Request.Namespace {
				return pi.Request.Namespace < pj.Request.Namespace
			}
			return pi.Request.Name < pj.Request.Name
		})
	}

	// Rough capacity hint: each object contributes ~len(ObjectID)+len(hash)+2 (for '=' and ',')
	// plus pending reconciles and separator.
	builder := strings.Builder{}
	if l := len(objectKeys); l > 0 {
		builder.Grow(l * 32)
	}

	for idx, objKey := range objectKeys {
		if idx > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(serializeCompositeKey(objKey))
		builder.WriteByte('=')
		builder.WriteString(sn.Contents.contents[objKey].Value)
	}

	builder.WriteByte('|')

	for idx, pr := range pending {
		if idx > 0 {
			builder.WriteByte(',')
		}
		// inline PendingReconcile.String to avoid fmt.Sprintf allocations
		builder.WriteString(string(pr.ReconcilerID))
		builder.WriteByte(':')
		builder.WriteString(pr.Request.Namespace)
		builder.WriteByte('/')
		builder.WriteString(pr.Request.Name)
	}

	return builder.String()
}

func (sn StateNode) Serialize() string {
	return sn.serialize(false)
}

func serializeCompositeKey(ck snapshot.CompositeKey) string {
	group := ck.ResourceKey.Group
	if group == "" {
		group = "core"
	}
	return fmt.Sprintf("%s/%s/%s/%s:%s", group, ck.ResourceKey.Kind, ck.ResourceKey.Namespace, ck.ResourceKey.Name, ck.ObjectID)
}

// StateHash represents the contents of the state node and the pending reconciles, unaffected by the order of pending reconciles.
type StateHash string

// ContentsHash represents the contents of the state node only, excluding metadata such as pending reconciles.
type ContentsHash string

// Hash returns a hash of the state node, unaffected by the order of pending reconciles.
func (sn StateNode) Hash() StateHash {
	s := sn.Serialize()
	return StateHash(util.ShortenHash(s))
}

// ContentsHash returns a hash of just the object contents, excluding pending reconciles.
// This is useful for caching reconcile results since reconciler behavior only depends on objects.
func (sn StateNode) ContentsHash() ContentsHash {
	objectKeys := make([]snapshot.CompositeKey, 0, len(sn.Objects()))
	for objKey := range sn.Objects() {
		objectKeys = append(objectKeys, objKey)
	}
	sort.Slice(objectKeys, func(i, j int) bool {
		ai, aj := objectKeys[i], objectKeys[j]
		if ai.ResourceKey.Group != aj.ResourceKey.Group {
			return ai.ResourceKey.Group < aj.ResourceKey.Group
		}
		if ai.ResourceKey.Kind != aj.ResourceKey.Kind {
			return ai.ResourceKey.Kind < aj.ResourceKey.Kind
		}
		if ai.ResourceKey.Namespace != aj.ResourceKey.Namespace {
			return ai.ResourceKey.Namespace < aj.ResourceKey.Namespace
		}
		if ai.ResourceKey.Name != aj.ResourceKey.Name {
			return ai.ResourceKey.Name < aj.ResourceKey.Name
		}
		return ai.ObjectID < aj.ObjectID
	})

	var buf strings.Builder
	for _, ck := range objectKeys {
		buf.WriteString(serializeCompositeKey(ck))
		buf.WriteString("=")
		buf.WriteString(sn.Objects()[ck].Value)
		buf.WriteString(";")
	}
	return ContentsHash(util.ShortenHash(buf.String()))
}

// ConvergenceHash returns a hash normalized for convergence by dropping pending reconciles
// that are ignorable for convergence (async enqueues / requeues).
func (sn StateNode) ConvergenceHash() StateHash {
	filtered := lo.Filter(sn.PendingReconciles, func(pr PendingReconcile, _ int) bool {
		return pr.Source != SourceAsyncEnqueue && pr.Source != SourceRequeue
	})
	clone := sn
	clone.PendingReconciles = filtered
	return StateHash(util.ShortenHash(clone.Serialize()))
}

// LogicalStateKey uniquely identifies a logical state for subtree completion tracking.
// Two states with the same LogicalStateKey will have identical future exploration subtrees,
// regardless of how they were reached (execution history).
//
// TODO: Consider adding StuckPositions to the key when staleness expansion is unified
// with the marker-based completion tracking model.
type LogicalStateKey struct {
	ObjectsHash ContentsHash
	PendingSet  string
}

// LogicalKey returns the LogicalStateKey for this state node.
func (sn StateNode) LogicalKey() LogicalStateKey {
	return LogicalStateKey{
		ObjectsHash: sn.ContentsHash(),
		PendingSet:  sn.sortedPendingSignature(),
	}
}

// sortedPendingSignature returns an order-insensitive signature of pending reconciles.
func (sn StateNode) sortedPendingSignature() string {
	if len(sn.PendingReconciles) == 0 {
		return ""
	}
	keys := make([]string, len(sn.PendingReconciles))
	for i, pr := range sn.PendingReconciles {
		keys[i] = fmt.Sprintf("%s:%s/%s", pr.ReconcilerID, pr.Request.Namespace, pr.Request.Name)
	}
	sort.Strings(keys)
	return strings.Join(keys, "|")
}

func (sn *StateSnapshot) trimForInspection() {
	if sn == nil {
		return
	}
	sn.stateEvents = nil
}

func (sn *StateNode) TrimForInspection() {
	if sn == nil {
		return
	}
	sn.parent = nil
	sn.action = nil
	sn.ExecutionHistory = nil
	sn.stuckReconcilerPositions = nil
	sn.Contents.trimForInspection()
}

// OrderHash represents the contents of the state node and the order of pending reconciles.
type OrderHash string

// OrderSensitiveHash returns a hash of the state node and the order of pending reconciles.
func (sn StateNode) OrderSensitiveHash() OrderHash {
	s := sn.serialize(true)
	return OrderHash(util.ShortenHash(s))
}

func (sn StateNode) LineageHash() string {
	if sn.parent == nil {
		return string(sn.OrderSensitiveHash())
	}
	return fmt.Sprintf("%s->%s", sn.parent.LineageHash(), sn.OrderSensitiveHash())
}

func (sn StateNode) DetailedLineage() string {
	var id string
	var numChanges int = 0
	if sn.action != nil {
		id = string(sn.action.ControllerID)
		numChanges = len(sn.action.Changes.ObjectVersions)
	} else {
		id = "root"
	}
	if sn.parent == nil {
		return fmt.Sprintf("%s:%s", id, sn.OrderSensitiveHash())
	}
	return fmt.Sprintf("%s->%s:%s@%d", sn.parent.DetailedLineage(), id, sn.OrderSensitiveHash(), numChanges)
}

func (sn StateNode) ReconcileLineage() string {
	var id string
	var frameID string
	var numChanges int = 0

	if sn.action != nil {
		id = string(sn.action.ControllerID)
		frameID = util.Shorter(sn.action.FrameID) // TODO this is not robust
		numChanges = len(sn.action.Changes.ObjectVersions)
	} else {
		id = "root"
		frameID = ""
	}

	if sn.parent == nil {
		return id
	}

	return fmt.Sprintf("%s=>%s:%s[%d]", sn.parent.ReconcileLineage(), id, frameID, numChanges)
}

// expandStateByReconcileOrder handles permuting the order of the reconcilers triggered by the creation of
// a new StateNode. It produces a new StateNodes for each triggered reconciler where that reconciler is placed
// as the first element in its PendingReconciles list. This allows the explorer to explore any potential
// order sensitivity among the reconcilers triggered by the same state change.
func (e *Explorer) expandStateByReconcileOrder(state StateNode, triggered []PendingReconcile) []StateNode {
	// If there are no pending reconciles or just one, just return the original state
	if len(state.PendingReconciles) <= 1 {
		return []StateNode{state}
	}

	if e.Config == nil || e.Config.PermuteOrder == nil {
		return []StateNode{state}
	}

	originalPending := state.PendingReconciles
	var result []StateNode

	toPermute := util.NewSet[ReconcilerID]()
	for _, pr := range triggered {
		if permute, ok := e.Config.PermuteOrder[pr.ReconcilerID]; ok && permute {
			toPermute.Add(pr.ReconcilerID)
		}
	}

	// For each pending reconcile in toPermute, create a new StateNode with that reconcile first,
	for i := range originalPending {
		reconcilerID := originalPending[i].ReconcilerID
		if _, ok := toPermute[reconcilerID]; !ok {
			continue
		}

		alternativeOrder := make([]PendingReconcile, 0, len(originalPending))
		alternativeOrder = append(alternativeOrder, originalPending[i])
		alternativeOrder = append(alternativeOrder, originalPending[:i]...)
		alternativeOrder = append(alternativeOrder, originalPending[i+1:]...)

		cloned := state.Clone()
		cloned.PendingReconciles = alternativeOrder
		cloned.ID = string(cloned.OrderSensitiveHash()) // Generate a new deterministic ID based on the new ordering
		result = append(result, cloned)
	}

	return result
}
