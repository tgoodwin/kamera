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
	ControllerID string
	FrameID      string
	FrameType    FrameType
	Changes      Changes // this is just the writeset, not the resulting full state of the world
	Deltas       map[snapshot.CompositeKey]Delta
	Error        string

	StateBefore   ObjectVersions
	StateAfter    ObjectVersions
	KindSeqBefore KindSequences
	KindSeqAfter  KindSequences

	ctrlRes reconcile.Result
}

type ExecutionHistory []*ReconcileResult

func (eh ExecutionHistory) UniqueKey() string {
	// first filter out no-ops
	filterNoOps := lo.Filter(eh, func(r *ReconcileResult, _ int) bool {
		return len(r.Changes.ObjectVersions) > 0 || r.Error != ""
	})
	strComponents := lo.Map(filterNoOps, func(r *ReconcileResult, _ int) string {
		suffix := ""
		if r.Error != "" {
			suffix = "!"
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

func getUniquePaths(paths []ExecutionHistory) []ExecutionHistory {
	return lo.UniqBy(paths, func(path ExecutionHistory) string {
		return path.UniqueKey()
	})
}

func GetUniquePaths(paths []ExecutionHistory) []ExecutionHistory {
	getUniquePaths(paths)
	pathsWithoutNoOps := lo.Map(paths, func(path ExecutionHistory, _ int) ExecutionHistory {
		return path.FilterNoOps()
	})
	// filter out empty paths
	pathsWithoutNoOps = lo.Filter(pathsWithoutNoOps, func(path ExecutionHistory, _ int) bool {
		return len(path) > 0
	})
	unique := lo.UniqBy(pathsWithoutNoOps, func(path ExecutionHistory) string {
		return strings.Join(
			lo.Map(path, func(r *ReconcileResult, _ int) string {
				return r.ControllerID
			}), ",",
		)
	})

	return unique
}

type ObservableState interface {
	Objects() ObjectVersions
}

type NodeMode string

const (
	NodeModeNatural      NodeMode = "natural"
	NodeModeHypothetical NodeMode = "hypothetical"
)

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

	mode NodeMode // used to track if we are in a natural or hypothetical state

	// used to track children of a divergence point of interest
	divergenceKey StateHash

	depth int

	DivergencePoint string // reconcileID of the first divergence

	// tracks what KindSequences a controller may be "stuck" on
	// e.g. if a controller's watches are connected to a partitioned APIServer
	stuckReconcilerPositions map[string]KindSequences
}

func (sn StateNode) ObserveAs(reconcilerID string) ObjectVersions {
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

		mode:          sn.mode,
		divergenceKey: sn.divergenceKey,

		stuckReconcilerPositions: maps.Clone(sn.stuckReconcilerPositions),
	}
}

func (sn StateNode) serialize(reconcileOrderSensitive bool) string {
	objectPairs := make([]string, 0, len(sn.Objects()))
	for objKey, version := range sn.Objects() {
		objectPairs = append(objectPairs, fmt.Sprintf("%s=%s", objKey.ObjectID, version.Value))
	}
	sort.Strings(objectPairs)

	prStrs := make([]string, 0, len(sn.PendingReconciles))
	for _, pr := range sn.PendingReconciles {
		prStrs = append(prStrs, pr.String())
	}

	// Sort the pending reconciles if we're not order sensitive
	if !reconcileOrderSensitive && len(prStrs) > 1 {
		sort.Strings(prStrs)
	}

	objectStr := strings.Join(objectPairs, ",")
	prStr := strings.Join(prStrs, ",")
	return fmt.Sprintf("%s|%s", objectStr, prStr)
}

func (sn StateNode) Serialize() string {
	return sn.serialize(false)
}

// StateHash represents the contents of the state node and the pending reconciles, unaffected by the order of pending reconciles.
type StateHash string

// Hash returns a hash of the state node, unaffected by the order of pending reconciles.
func (sn StateNode) Hash() StateHash {
	s := sn.Serialize()
	return StateHash(util.ShortenHash(s))
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
	sn.PendingReconciles = nil
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
		id = sn.action.ControllerID
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
		id = sn.action.ControllerID
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

// expandStateByReconcileOrder takes a StateNode and returns a slice of new StateNodes,
// where each new StateNode is a clone of the input but with a different pending reconcile
// as the first element in its PendingReconciles list.
func expandStateByReconcileOrder(state StateNode) []StateNode {
	// If there are no pending reconciles or just one, just return the original state
	if len(state.PendingReconciles) <= 1 {
		return []StateNode{state}
	}

	originalPending := state.PendingReconciles
	result := make([]StateNode, len(originalPending))

	// For each pending reconcile, create a new StateNode with that reconcile first
	for i := 0; i < len(originalPending); i++ {
		// Create a new ordering with this reconcile first
		alternativeOrder := make([]PendingReconcile, len(originalPending))
		alternativeOrder[0] = originalPending[i] // Put the ith reconcile first

		// Add the rest in their original order, skipping the one we put first
		j := 1
		for k := 0; k < len(originalPending); k++ {
			if k != i {
				alternativeOrder[j] = originalPending[k]
				j++
			}
		}

		cloned := state.Clone()
		cloned.PendingReconciles = alternativeOrder
		cloned.ID = string(cloned.OrderSensitiveHash()) // Generate a new deterministic ID based on the new ordering
		result[i] = cloned
	}

	return result
}
