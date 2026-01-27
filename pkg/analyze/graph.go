package analyze

import (
	"encoding/json"
	"fmt"
	"strings"
)

type NodeID string

type EdgeID string

type NodeKind int

const (
	NodeController NodeKind = iota
	NodeResource
)

type EdgeKind int

const (
	EdgeReconciles EdgeKind = iota
	EdgeWatches
	EdgeOwns
	EdgeReads
	EdgeWrites
)

type WatchKind int

const (
	WatchUnknown WatchKind = iota
	WatchPrimary
	WatchOwned
	WatchIndexed
)

type ReadWriteTarget int

const (
	TargetUnknown ReadWriteTarget = iota
	TargetSpec
	TargetStatus
)

type Controller struct {
	Name string
}

type Resource struct {
	Group   string
	Version string
	Kind    string
}

type Node struct {
	ID         NodeID
	Kind       NodeKind
	Controller Controller
	Resource   Resource
}

type WatchAttrs struct {
	Kind WatchKind
}

type ReadAttrs struct {
	Target ReadWriteTarget
}

type WriteAttrs struct {
	Target ReadWriteTarget
}

type EdgeAttrs struct {
	Watch WatchAttrs
	Read  ReadAttrs
	Write WriteAttrs
}

type Edge struct {
	ID   EdgeID
	From NodeID
	To   NodeID
	Kind EdgeKind
	Attr EdgeAttrs
}

type Graph struct {
	Nodes map[NodeID]Node
	Edges []Edge
}

type RawGraph struct {
	Nodes []RawNode `json:"nodes"`
	Edges []RawEdge `json:"edges"`
}

type RawNode struct {
	Kind string `json:"kind"`
	Name string `json:"name,omitempty"`
	GVK  string `json:"gvk,omitempty"`
}

type RawEdge struct {
	Kind      string `json:"kind"`
	From      string `json:"from"`
	To        string `json:"to"`
	WatchKind string `json:"watchKind,omitempty"`
	Target    string `json:"target,omitempty"`
}

func ParseRawGraphJSON(data []byte) (RawGraph, error) {
	var raw RawGraph
	if err := json.Unmarshal(data, &raw); err != nil {
		return RawGraph{}, err
	}
	return raw, nil
}

func BuildGraphFromRaw(raw RawGraph) (*Graph, error) {
	graph := &Graph{Nodes: make(map[NodeID]Node)}

	for _, node := range raw.Nodes {
		switch strings.ToLower(strings.TrimSpace(node.Kind)) {
		case "controller":
			if node.Name == "" {
				return nil, fmt.Errorf("controller node missing name")
			}
			if _, err := ensureController(graph, node.Name); err != nil {
				return nil, err
			}
		case "resource":
			if node.GVK == "" {
				return nil, fmt.Errorf("resource node missing gvk")
			}
			if _, err := ensureResource(graph, node.GVK); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unknown node kind %q", node.Kind)
		}
	}

	for _, edge := range raw.Edges {
		edgeKind, err := parseEdgeKind(edge.Kind)
		if err != nil {
			return nil, err
		}

		fromID, fromKind, err := ensureNodeFromRef(graph, edge.From)
		if err != nil {
			return nil, err
		}
		toID, toKind, err := ensureNodeFromRef(graph, edge.To)
		if err != nil {
			return nil, err
		}

		if err := validateEdgeKinds(edgeKind, fromKind, toKind); err != nil {
			return nil, err
		}

		attrs, err := buildEdgeAttrs(edgeKind, edge)
		if err != nil {
			return nil, err
		}

		edgeID := EdgeID(buildEdgeID(fromID, edgeKind, toID, attrs))
		graph.Edges = append(graph.Edges, Edge{
			ID:   edgeID,
			From: fromID,
			To:   toID,
			Kind: edgeKind,
			Attr: attrs,
		})
	}

	return graph, nil
}

func ensureController(graph *Graph, name string) (NodeID, error) {
	if strings.TrimSpace(name) == "" {
		return "", fmt.Errorf("controller name is empty")
	}
	id := NodeID(fmt.Sprintf("c:%s", name))
	if existing, ok := graph.Nodes[id]; ok {
		if existing.Kind != NodeController {
			return "", fmt.Errorf("node %s is not a controller", id)
		}
		return id, nil
	}
	graph.Nodes[id] = Node{ID: id, Kind: NodeController, Controller: Controller{Name: name}}
	return id, nil
}

func ensureResource(graph *Graph, gvk string) (NodeID, error) {
	resource, err := parseGVKString(gvk)
	if err != nil {
		return "", err
	}
	id := NodeID(fmt.Sprintf("r:%s/%s/%s", canonicalGroup(resource.Group), resource.Version, resource.Kind))
	if existing, ok := graph.Nodes[id]; ok {
		if existing.Kind != NodeResource {
			return "", fmt.Errorf("node %s is not a resource", id)
		}
		return id, nil
	}
	graph.Nodes[id] = Node{ID: id, Kind: NodeResource, Resource: resource}
	return id, nil
}

func ensureNodeFromRef(graph *Graph, ref string) (NodeID, NodeKind, error) {
	if isGVKString(ref) {
		id, err := ensureResource(graph, ref)
		return id, NodeResource, err
	}
	id, err := ensureController(graph, ref)
	return id, NodeController, err
}

func isGVKString(ref string) bool {
	return strings.Count(ref, "/") == 2
}

func parseGVKString(spec string) (Resource, error) {
	parts := strings.Split(spec, "/")
	if len(parts) != 3 {
		return Resource{}, fmt.Errorf("invalid gvk %q", spec)
	}
	group := strings.TrimSpace(parts[0])
	version := strings.TrimSpace(parts[1])
	kind := strings.TrimSpace(parts[2])
	if group == "core" {
		group = ""
	}
	if version == "" || kind == "" {
		return Resource{}, fmt.Errorf("invalid gvk %q", spec)
	}
	return Resource{Group: group, Version: version, Kind: kind}, nil
}

func canonicalGroup(group string) string {
	if group == "" {
		return "core"
	}
	return group
}

func parseEdgeKind(kind string) (EdgeKind, error) {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "reconciles":
		return EdgeReconciles, nil
	case "watches":
		return EdgeWatches, nil
	case "owns":
		return EdgeOwns, nil
	case "reads":
		return EdgeReads, nil
	case "writes":
		return EdgeWrites, nil
	default:
		return 0, fmt.Errorf("unknown edge kind %q", kind)
	}
}

func parseWatchKind(kind string) (WatchKind, error) {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "":
		return WatchUnknown, nil
	case "primary":
		return WatchPrimary, nil
	case "owned":
		return WatchOwned, nil
	case "indexed":
		return WatchIndexed, nil
	default:
		return 0, fmt.Errorf("unknown watchKind %q", kind)
	}
}

func parseTarget(target string) (ReadWriteTarget, error) {
	switch strings.ToLower(strings.TrimSpace(target)) {
	case "":
		return TargetUnknown, nil
	case "spec":
		return TargetSpec, nil
	case "status":
		return TargetStatus, nil
	default:
		return 0, fmt.Errorf("unknown target %q", target)
	}
}

func buildEdgeAttrs(kind EdgeKind, raw RawEdge) (EdgeAttrs, error) {
	attrs := EdgeAttrs{}
	switch kind {
	case EdgeWatches:
		watchKind, err := parseWatchKind(raw.WatchKind)
		if err != nil {
			return EdgeAttrs{}, err
		}
		attrs.Watch.Kind = watchKind
	case EdgeReads:
		target, err := parseTarget(raw.Target)
		if err != nil {
			return EdgeAttrs{}, err
		}
		attrs.Read.Target = target
	case EdgeWrites:
		target, err := parseTarget(raw.Target)
		if err != nil {
			return EdgeAttrs{}, err
		}
		attrs.Write.Target = target
	}
	return attrs, nil
}

func validateEdgeKinds(kind EdgeKind, fromKind NodeKind, toKind NodeKind) error {
	switch kind {
	case EdgeReconciles, EdgeWatches, EdgeReads, EdgeWrites:
		if fromKind != NodeController || toKind != NodeResource {
			return fmt.Errorf("edge %s requires controller -> resource", edgeKindString(kind))
		}
	case EdgeOwns:
		if fromKind != NodeResource || toKind != NodeResource {
			return fmt.Errorf("edge owns requires resource -> resource")
		}
	}
	return nil
}

func edgeKindString(kind EdgeKind) string {
	switch kind {
	case EdgeReconciles:
		return "reconciles"
	case EdgeWatches:
		return "watches"
	case EdgeOwns:
		return "owns"
	case EdgeReads:
		return "reads"
	case EdgeWrites:
		return "writes"
	default:
		return "unknown"
	}
}

func watchKindString(kind WatchKind) string {
	switch kind {
	case WatchPrimary:
		return "primary"
	case WatchOwned:
		return "owned"
	case WatchIndexed:
		return "indexed"
	default:
		return "unknown"
	}
}

func targetString(target ReadWriteTarget) string {
	switch target {
	case TargetSpec:
		return "spec"
	case TargetStatus:
		return "status"
	default:
		return "unknown"
	}
}

func buildEdgeID(from NodeID, kind EdgeKind, to NodeID, attrs EdgeAttrs) string {
	attrSuffix := edgeAttrSuffix(kind, attrs)
	if attrSuffix == "" {
		return fmt.Sprintf("e:%s|%s|%s", from, edgeKindString(kind), to)
	}
	return fmt.Sprintf("e:%s|%s|%s|%s", from, edgeKindString(kind), to, attrSuffix)
}

func edgeAttrSuffix(kind EdgeKind, attrs EdgeAttrs) string {
	switch kind {
	case EdgeWatches:
		return watchKindString(attrs.Watch.Kind)
	case EdgeReads:
		return targetString(attrs.Read.Target)
	case EdgeWrites:
		return targetString(attrs.Write.Target)
	default:
		return ""
	}
}
