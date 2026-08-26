package analyze

import (
	"fmt"
	"sort"
	"strings"
)

// Hotspot detection scans the static controller/resource graph and emits
// hotspot instances: small sets of controllers/resources that are structurally
// risky for ordering or consistency bugs. Detection is rule-based over edge
// semantics (reads/writes/watches/owns) rather than raw graph shape. Each
// instance records the specific controllers/resources involved and minimal
// attributes needed for downstream scenario generation (e.g., missing trigger
// resources or convergence type).

type HotspotType string

const (
	HotspotMultiWriter    HotspotType = "multi_writer"
	HotspotMissingTrigger HotspotType = "missing_trigger"
	HotspotDiamondPattern HotspotType = "diamond_pattern"
	HotspotReducer        HotspotType = "reducer_controller"
	HotspotFeedbackCycle  HotspotType = "feedback_cycle"
)

type HotspotInstance struct {
	Type        HotspotType
	Controllers []NodeID
	Resources   []NodeID
	EdgeIDs     []EdgeID
	Attributes  map[string]string
}

type graphIndex struct {
	readsByController      map[NodeID]map[NodeID]struct{}
	writesByController     map[NodeID]map[NodeID]struct{}
	watchesByController    map[NodeID]map[NodeID]struct{}
	reconcilesByController map[NodeID]map[NodeID]struct{}
	triggersByResource     map[NodeID]map[NodeID]struct{}
	writersByResource      map[NodeID]map[NodeID]struct{}
	writesByResourceTarget map[NodeID]map[ReadWriteTarget]map[NodeID]struct{}
	ownsByParent           map[NodeID]map[NodeID]struct{}
	parentsByChild         map[NodeID]map[NodeID]struct{}
}

func DetectHotspots(graph *Graph) ([]HotspotInstance, error) {
	if graph == nil {
		return nil, fmt.Errorf("graph is nil")
	}
	idx := buildGraphIndex(graph)

	var hotspots []HotspotInstance
	hotspots = append(hotspots, detectMultiWriter(idx)...)
	hotspots = append(hotspots, detectMissingTrigger(idx)...)
	hotspots = append(hotspots, detectDiamondPattern(idx)...)
	hotspots = append(hotspots, detectReducerPattern(idx)...)
	hotspots = append(hotspots, detectFeedbackCycles(graph, idx)...)

	return hotspots, nil
}

func buildGraphIndex(graph *Graph) graphIndex {
	idx := graphIndex{
		readsByController:      make(map[NodeID]map[NodeID]struct{}),
		writesByController:     make(map[NodeID]map[NodeID]struct{}),
		watchesByController:    make(map[NodeID]map[NodeID]struct{}),
		reconcilesByController: make(map[NodeID]map[NodeID]struct{}),
		triggersByResource:     make(map[NodeID]map[NodeID]struct{}),
		writersByResource:      make(map[NodeID]map[NodeID]struct{}),
		writesByResourceTarget: make(map[NodeID]map[ReadWriteTarget]map[NodeID]struct{}),
		ownsByParent:           make(map[NodeID]map[NodeID]struct{}),
		parentsByChild:         make(map[NodeID]map[NodeID]struct{}),
	}

	for _, edge := range graph.Edges {
		switch edge.Kind {
		case EdgeReads:
			addSet(idx.readsByController, edge.From, edge.To)
		case EdgeWrites:
			addSet(idx.writesByController, edge.From, edge.To)
			addSet(idx.writersByResource, edge.To, edge.From)
			if _, ok := idx.writesByResourceTarget[edge.To]; !ok {
				idx.writesByResourceTarget[edge.To] = make(map[ReadWriteTarget]map[NodeID]struct{})
			}
			if _, ok := idx.writesByResourceTarget[edge.To][edge.Attr.Write.Target]; !ok {
				idx.writesByResourceTarget[edge.To][edge.Attr.Write.Target] = make(map[NodeID]struct{})
			}
			idx.writesByResourceTarget[edge.To][edge.Attr.Write.Target][edge.From] = struct{}{}
		case EdgeWatches:
			addSet(idx.watchesByController, edge.From, edge.To)
			addSet(idx.triggersByResource, edge.To, edge.From)
		case EdgeReconciles:
			addSet(idx.reconcilesByController, edge.From, edge.To)
			addSet(idx.triggersByResource, edge.To, edge.From)
		case EdgeOwns:
			addSet(idx.ownsByParent, edge.From, edge.To)
			addSet(idx.parentsByChild, edge.To, edge.From)
		}
	}

	return idx
}

func detectMultiWriter(idx graphIndex) []HotspotInstance {
	var out []HotspotInstance
	for resource, writers := range idx.writersByResource {
		if len(writers) < 2 {
			continue
		}

		targets := idx.writesByResourceTarget[resource]
		hasTargetGroup := false
		for target, controllers := range targets {
			if len(controllers) < 2 {
				continue
			}
			hasTargetGroup = true
			out = append(out, HotspotInstance{
				Type:        HotspotMultiWriter,
				Controllers: sortedNodeIDs(controllers),
				Resources:   []NodeID{resource},
				Attributes:  map[string]string{"target": targetString(target)},
			})
		}

		if !hasTargetGroup {
			out = append(out, HotspotInstance{
				Type:        HotspotMultiWriter,
				Controllers: sortedNodeIDs(writers),
				Resources:   []NodeID{resource},
				Attributes:  map[string]string{"target": "any"},
			})
		}
	}

	return out
}

func detectMissingTrigger(idx graphIndex) []HotspotInstance {
	var out []HotspotInstance
	for controller, reads := range idx.readsByController {
		triggers := unionSets(idx.watchesByController[controller], idx.reconcilesByController[controller])
		for resource := range reads {
			_, hasTrigger := triggers[resource]
			writers := copySet(idx.writersByResource[resource])
			delete(writers, controller)

			if !hasTrigger || len(writers) > 0 {
				attrs := make(map[string]string)
				if !hasTrigger {
					attrs["missing_trigger_resource"] = string(resource)
				}
				if len(writers) > 0 {
					attrs["writers"] = strings.Join(nodeIDStrings(sortedNodeIDs(writers)), ",")
				}
				out = append(out, HotspotInstance{
					Type:        HotspotMissingTrigger,
					Controllers: append([]NodeID{controller}, sortedNodeIDs(writers)...),
					Resources:   []NodeID{resource},
					Attributes:  attrs,
				})
			}
		}
	}

	return out
}

func detectDiamondPattern(idx graphIndex) []HotspotInstance {
	var out []HotspotInstance
	seen := make(map[string]struct{})

	parentsWrittenByController := make(map[NodeID]map[NodeID]struct{})
	for controller, writes := range idx.writesByController {
		for child := range writes {
			for parent := range idx.parentsByChild[child] {
				addSet(parentsWrittenByController, controller, parent)
			}
		}
	}

	for triggerResource, controllers := range idx.triggersByResource {
		controllerList := sortedNodeIDs(controllers)
		for i := 0; i < len(controllerList); i++ {
			for j := i + 1; j < len(controllerList); j++ {
				c1 := controllerList[i]
				c2 := controllerList[j]

				writes1 := idx.writesByController[c1]
				writes2 := idx.writesByController[c2]

				for _, rend := range intersectKeys(writes1, writes2) {
					key := fmt.Sprintf("direct|%s|%s|%s|%s", triggerResource, c1, c2, rend)
					if _, ok := seen[key]; ok {
						continue
					}
					seen[key] = struct{}{}
					out = append(out, HotspotInstance{
						Type:        HotspotDiamondPattern,
						Controllers: []NodeID{c1, c2},
						Resources:   []NodeID{triggerResource, rend},
						Attributes:  map[string]string{"converges_via": "direct"},
					})
				}

				parents1 := parentsWrittenByController[c1]
				parents2 := parentsWrittenByController[c2]
				for _, parent := range intersectKeys(parents1, parents2) {
					key := fmt.Sprintf("owns|%s|%s|%s|%s", triggerResource, c1, c2, parent)
					if _, ok := seen[key]; ok {
						continue
					}
					seen[key] = struct{}{}
					out = append(out, HotspotInstance{
						Type:        HotspotDiamondPattern,
						Controllers: []NodeID{c1, c2},
						Resources:   []NodeID{triggerResource, parent},
						Attributes:  map[string]string{"converges_via": "owns"},
					})
				}
			}
		}
	}

	return out
}

func detectReducerPattern(idx graphIndex) []HotspotInstance {
	var out []HotspotInstance
	for controller, reads := range idx.readsByController {
		if len(reads) < 2 {
			continue
		}
		writes := idx.writesByController[controller]
		if len(writes) == 0 {
			continue
		}
		inputs := sortedNodeIDs(reads)
		outputs := sortedNodeIDs(writes)
		resources := append([]NodeID{}, inputs...)
		resources = append(resources, outputs...)
		resources = uniqueNodeIDs(resources)

		attrs := map[string]string{
			"inputs":  strings.Join(nodeIDStrings(inputs), ","),
			"outputs": strings.Join(nodeIDStrings(outputs), ","),
		}
		out = append(out, HotspotInstance{
			Type:        HotspotReducer,
			Controllers: []NodeID{controller},
			Resources:   resources,
			Attributes:  attrs,
		})
	}
	return out
}

func detectFeedbackCycles(graph *Graph, idx graphIndex) []HotspotInstance {
	adj := make(map[NodeID][]NodeID)
	selfLoops := make(map[NodeID]bool)

	for _, edge := range graph.Edges {
		switch edge.Kind {
		case EdgeWrites:
			adj[edge.From] = append(adj[edge.From], edge.To)
		case EdgeWatches, EdgeReconciles:
			adj[edge.To] = append(adj[edge.To], edge.From)
		}
	}

	for from, tos := range adj {
		for _, to := range tos {
			if from == to {
				selfLoops[from] = true
			}
		}
	}

	components := tarjanSCC(adj)
	var out []HotspotInstance
	for _, component := range components {
		if len(component) == 1 {
			if !selfLoops[component[0]] {
				continue
			}
		}
		var controllers []NodeID
		var resources []NodeID
		for _, node := range component {
			if graph.Nodes[node].Kind == NodeController {
				controllers = append(controllers, node)
			} else if graph.Nodes[node].Kind == NodeResource {
				resources = append(resources, node)
			}
		}
		if len(controllers) == 0 || len(resources) == 0 {
			continue
		}
		out = append(out, HotspotInstance{
			Type:        HotspotFeedbackCycle,
			Controllers: sortedNodeIDSlice(controllers),
			Resources:   sortedNodeIDSlice(resources),
			Attributes:  map[string]string{"cycle_size": fmt.Sprintf("%d", len(component))},
		})
	}
	return out
}

func addSet(set map[NodeID]map[NodeID]struct{}, key, value NodeID) {
	if _, ok := set[key]; !ok {
		set[key] = make(map[NodeID]struct{})
	}
	set[key][value] = struct{}{}
}

func copySet(set map[NodeID]struct{}) map[NodeID]struct{} {
	out := make(map[NodeID]struct{}, len(set))
	for key := range set {
		out[key] = struct{}{}
	}
	return out
}

func unionSets(a map[NodeID]struct{}, b map[NodeID]struct{}) map[NodeID]struct{} {
	out := make(map[NodeID]struct{})
	for key := range a {
		out[key] = struct{}{}
	}
	for key := range b {
		out[key] = struct{}{}
	}
	return out
}

func intersectKeys(a map[NodeID]struct{}, b map[NodeID]struct{}) []NodeID {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}
	var out []NodeID
	for key := range a {
		if _, ok := b[key]; ok {
			out = append(out, key)
		}
	}
	return sortedNodeIDSlice(out)
}

func sortedNodeIDs(set map[NodeID]struct{}) []NodeID {
	var out []NodeID
	for key := range set {
		out = append(out, key)
	}
	return sortedNodeIDSlice(out)
}

func sortedNodeIDSlice(values []NodeID) []NodeID {
	sort.Slice(values, func(i, j int) bool {
		return values[i] < values[j]
	})
	return values
}

func nodeIDStrings(values []NodeID) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, string(value))
	}
	return out
}

func uniqueNodeIDs(values []NodeID) []NodeID {
	seen := make(map[NodeID]struct{})
	var out []NodeID
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func boolString(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

type tarjanState struct {
	index      int
	indexByID  map[NodeID]int
	lowlink    map[NodeID]int
	onStack    map[NodeID]bool
	stack      []NodeID
	components [][]NodeID
}

// Tarjan's algorithm for finding strongly connected components in a directed graph.
func tarjanSCC(adj map[NodeID][]NodeID) [][]NodeID {
	state := &tarjanState{
		index:     0,
		indexByID: make(map[NodeID]int),
		lowlink:   make(map[NodeID]int),
		onStack:   make(map[NodeID]bool),
	}

	for node := range adj {
		if _, ok := state.indexByID[node]; !ok {
			state.strongConnect(node, adj)
		}
	}
	return state.components
}

func (s *tarjanState) strongConnect(node NodeID, adj map[NodeID][]NodeID) {
	s.indexByID[node] = s.index
	s.lowlink[node] = s.index
	s.index++

	s.stack = append(s.stack, node)
	s.onStack[node] = true

	for _, neighbor := range adj[node] {
		if _, ok := s.indexByID[neighbor]; !ok {
			s.strongConnect(neighbor, adj)
			if s.lowlink[neighbor] < s.lowlink[node] {
				s.lowlink[node] = s.lowlink[neighbor]
			}
		} else if s.onStack[neighbor] {
			if s.indexByID[neighbor] < s.lowlink[node] {
				s.lowlink[node] = s.indexByID[neighbor]
			}
		}
	}

	if s.lowlink[node] == s.indexByID[node] {
		var component []NodeID
		for {
			if len(s.stack) == 0 {
				break
			}
			w := s.stack[len(s.stack)-1]
			s.stack = s.stack[:len(s.stack)-1]
			s.onStack[w] = false
			component = append(component, w)
			if w == node {
				break
			}
		}
		if len(component) > 0 {
			s.components = append(s.components, component)
		}
	}
}
