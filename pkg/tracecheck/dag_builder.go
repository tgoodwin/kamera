package tracecheck

import (
	"fmt"
	"maps"
)

// StateDAG captures the order-insensitive state graph produced during exploration.
// Nodes are keyed by resource contents only (ContentsHash), so identical object sets
// reached with different pending reconcile lists collapse into a single node.
type StateDAG struct {
	Nodes map[StateHash]*DAGNode
}

// DAGNode represents a unique contents hash along with incoming/outgoing edges.
type DAGNode struct {
	Hash StateHash

	// Sample is a best-effort snapshot of the objects/kind sequences for this hash.
	Sample NodeSample

	Outgoing map[StateHash][]*DAGEdge
	Incoming map[StateHash][]*DAGEdge

	ConvergedIDs  []string
	MaxDepthIDs   []string // states aborted due to max depth
	AbortedIDs    []string // states aborted due to other errors
}

// NodeSample carries a representative view of a node's objects.
type NodeSample struct {
	Objects       ObjectVersions
	KindSequences KindSequences
}

// DAGEdge records a reconcile transition between two contents hashes.
type DAGEdge struct {
	From         StateHash
	To           StateHash
	Controller   ReconcilerID
	Effects      int
	Error        string
	PendingAfter int
	Occurrences  int
}

// BuildStateDAG constructs a contents-based DAG from an exploration Result.
// It is intended for offline analysis/inspection and does not alter exploration.
func BuildStateDAG(result Result) *StateDAG {
	dag := newStateDAG()

	appendPaths := func(rs ResultState, mark func(node *DAGNode, id string)) {
		for _, path := range rs.Paths {
			dag.addPath(path)
		}

		stateObjects := rs.State.Objects()
		stateHash := contentsHashForObjects(stateObjects)
		sample := NodeSample{
			Objects:       maps.Clone(stateObjects),
			KindSequences: maps.Clone(rs.State.Contents.KindSequences),
		}
		node := dag.ensureNode(stateHash, sample)
		mark(node, rs.ID)
	}

	for _, rs := range result.ConvergedStates {
		appendPaths(rs, func(node *DAGNode, id string) {
			node.ConvergedIDs = append(node.ConvergedIDs, id)
		})
	}
	for _, rs := range result.AbortedStates {
		appendPaths(rs, func(node *DAGNode, id string) {
			// Categorize by error type
			if rs.Error != nil && rs.Error.Error() == "max depth reached" {
				node.MaxDepthIDs = append(node.MaxDepthIDs, id)
			} else {
				node.AbortedIDs = append(node.AbortedIDs, id)
			}
		})
	}

	return dag
}

func newStateDAG() *StateDAG {
	return &StateDAG{
		Nodes: make(map[StateHash]*DAGNode),
	}
}

func (dag *StateDAG) addPath(path ExecutionHistory) {
	for _, step := range path {
		if step == nil {
			continue
		}

		beforeHash := contentsHashForObjects(step.StateBefore)
		afterHash := contentsHashForObjects(step.StateAfter)

		beforeSample := NodeSample{
			Objects:       maps.Clone(step.StateBefore),
			KindSequences: maps.Clone(step.KindSeqBefore),
		}
		afterSample := NodeSample{
			Objects:       maps.Clone(step.StateAfter),
			KindSequences: maps.Clone(step.KindSeqAfter),
		}

		dag.ensureNode(beforeHash, beforeSample)
		dag.ensureNode(afterHash, afterSample)
		dag.addEdge(beforeHash, afterHash, step)
	}
}

func (dag *StateDAG) ensureNode(hash StateHash, sample NodeSample) *DAGNode {
	if dag.Nodes == nil {
		dag.Nodes = make(map[StateHash]*DAGNode)
	}
	if node, ok := dag.Nodes[hash]; ok {
		if node.Sample.Objects == nil && sample.Objects != nil {
			node.Sample = sample.clone()
		}
		return node
	}

	node := &DAGNode{
		Hash:     hash,
		Sample:   sample.clone(),
		Outgoing: make(map[StateHash][]*DAGEdge),
		Incoming: make(map[StateHash][]*DAGEdge),
	}
	dag.Nodes[hash] = node
	return node
}

func (dag *StateDAG) addEdge(from, to StateHash, step *ReconcileResult) {
	fromNode := dag.ensureNode(from, NodeSample{})
	toNode := dag.ensureNode(to, NodeSample{})

	effects := len(step.Changes.Effects)
	errStr := step.Error
	pendingAfter := len(step.PendingReconciles)
	controller := step.ControllerID

	edges := fromNode.Outgoing[to]
	for _, edge := range edges {
		if edge.Controller == controller && edge.Effects == effects && edge.Error == errStr && edge.PendingAfter == pendingAfter {
			edge.Occurrences++
			return
		}
	}

	edge := &DAGEdge{
		From:         from,
		To:           to,
		Controller:   controller,
		Effects:      effects,
		Error:        errStr,
		PendingAfter: pendingAfter,
		Occurrences:  1,
	}
	fromNode.Outgoing[to] = append(edges, edge)
	toNode.Incoming[from] = append(toNode.Incoming[from], edge)
}

func (s NodeSample) clone() NodeSample {
	return NodeSample{
		Objects:       maps.Clone(s.Objects),
		KindSequences: maps.Clone(s.KindSequences),
	}
}

func contentsHashForObjects(objs ObjectVersions) StateHash {
	n := StateNode{
		Contents: StateSnapshot{contents: objs},
	}
	return n.StateHash()
}

func (e *DAGEdge) String() string {
	return fmt.Sprintf("%s -> %s via %s (effects=%d, pendingAfter=%d, err=%s, count=%d)",
		e.From, e.To, e.Controller, e.Effects, e.PendingAfter, e.Error, e.Occurrences)
}
