package coverage

import (
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/tgoodwin/kamera/pkg/analyze"
	"github.com/tgoodwin/kamera/pkg/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const (
	defaultNamespace = "default"
	feedbackMaxDepth = 20
	defaultLookback  = 1
)

// TranslateHotspots converts hotspot instances into concrete Inputs.
func TranslateHotspots(graph *analyze.Graph, hotspots []analyze.HotspotInstance, inputMap InputMap) ([]Input, error) {
	if graph == nil {
		return nil, fmt.Errorf("graph is nil")
	}

	out := make([]Input, 0, len(hotspots))
	for i, hotspot := range hotspots {
		resourceIDs := collectResourceIDs(hotspot)
		resourceList := sortedNodeIDs(resourceIDs)

		objects := make([]*unstructured.Unstructured, 0, len(resourceList))
		gvkToObject := make(map[string]*unstructured.Unstructured)

		for _, nodeID := range resourceList {
			node, ok := graph.Nodes[nodeID]
			if !ok {
				return nil, fmt.Errorf("resource node %s not found in graph", nodeID)
			}
			if node.Kind != analyze.NodeResource {
				return nil, fmt.Errorf("node %s is not a resource", nodeID)
			}

			gvk := gvkString(node.Resource)
			templates, ok := inputMap.Mapping[gvk]
			if !ok {
				return nil, fmt.Errorf("input map missing template for %s", gvk)
			}
			if len(templates) != 1 {
				return nil, fmt.Errorf("input map for %s must contain exactly one template", gvk)
			}
			if templates[0].Object == nil {
				return nil, fmt.Errorf("input map for %s has nil template object", gvk)
			}

			// make some unique name/namespace for the resource instance
			name := normalizedName(hotspot.Type, i, node.Resource.Kind)
			ns := namespaceForTemplate(templates[0].Object)
			obj := NormalizeTemplate(templates[0].Object, name, ns)
			gvkToObject[gvk] = obj
			objects = append(objects, obj)
		}

		pending, err := buildPending(graph, hotspot.Controllers, gvkToObject)
		if err != nil {
			return nil, err
		}

		tuning := buildTuning(hotspot, graph)
		input := Input{
			Name:    fmt.Sprintf("hotspot-%s-%d", hotspot.Type, i),
			Objects: objects,
			Pending: pending,
			Tuning:  tuning,
		}
		out = append(out, input)
	}

	return out, nil
}

func collectResourceIDs(hotspot analyze.HotspotInstance) map[analyze.NodeID]struct{} {
	resources := make(map[analyze.NodeID]struct{})
	for _, nodeID := range hotspot.Resources {
		resources[nodeID] = struct{}{}
	}

	for _, attr := range []string{"inputs", "outputs"} {
		for _, nodeID := range parseNodeIDList(hotspot.Attributes[attr]) {
			resources[nodeID] = struct{}{}
		}
	}

	return resources
}

func parseNodeIDList(value string) []analyze.NodeID {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]analyze.NodeID, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		out = append(out, analyze.NodeID(trimmed))
	}
	return out
}

func sortedNodeIDs(input map[analyze.NodeID]struct{}) []analyze.NodeID {
	out := make([]analyze.NodeID, 0, len(input))
	for id := range input {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i] < out[j]
	})
	return out
}

func gvkString(resource analyze.Resource) string {
	group := resource.Group
	if group == "" {
		group = "core"
	}
	return fmt.Sprintf("%s/%s/%s", group, resource.Version, resource.Kind)
}

func normalizedName(kind analyze.HotspotType, idx int, resourceKind string) string {
	return fmt.Sprintf("hs-%s-%d-%s", kind, idx, strings.ToLower(resourceKind))
}

func namespaceForTemplate(obj *unstructured.Unstructured) string {
	if obj.GetNamespace() == "" {
		return ""
	}
	return defaultNamespace
}

func buildPending(graph *analyze.Graph, controllers []analyze.NodeID, gvkToObject map[string]*unstructured.Unstructured) ([]Pending, error) {
	pending := make([]Pending, 0, len(controllers))
	for _, controllerID := range controllers {
		node, ok := graph.Nodes[controllerID]
		if !ok {
			return nil, fmt.Errorf("controller node %s not found in graph", controllerID)
		}
		if node.Kind != analyze.NodeController {
			return nil, fmt.Errorf("node %s is not a controller", controllerID)
		}

		targets := reconcilesTargets(graph, controllerID)
		if len(targets) == 0 {
			return nil, fmt.Errorf("controller %s has no reconciles target", node.Controller.Name)
		}
		if len(targets) > 1 {
			log.Printf("warning: controller %s reconciles multiple kinds; using %s", node.Controller.Name, targets[0])
		}
		gvk := targets[0]
		obj, ok := gvkToObject[gvk]
		if !ok || obj == nil {
			return nil, fmt.Errorf("no object for reconciles target %s", gvk)
		}
		pending = append(pending, Pending{
			ControllerID: node.Controller.Name,
			Key: NamespacedName{
				Namespace: obj.GetNamespace(),
				Name:      obj.GetName(),
			},
		})
	}
	return pending, nil
}

func reconcilesTargets(graph *analyze.Graph, controllerID analyze.NodeID) []string {
	set := make(map[string]struct{})
	for _, edge := range graph.Edges {
		if edge.Kind != analyze.EdgeReconciles || edge.From != controllerID {
			continue
		}
		node, ok := graph.Nodes[edge.To]
		if !ok || node.Kind != analyze.NodeResource {
			continue
		}
		set[gvkString(node.Resource)] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for gvk := range set {
		out = append(out, gvk)
	}
	sort.Strings(out)
	return out
}

func buildTuning(hotspot analyze.HotspotInstance, graph *analyze.Graph) InputTuning {
	tuning := InputTuning{
		StaleReads:    make(map[string][]string),
		StaleLookback: make(map[string]int),
	}

	controllerNames := controllerNames(graph, hotspot.Controllers)
	switch hotspot.Type {
	case analyze.HotspotMultiWriter, analyze.HotspotDiamondPattern, analyze.HotspotFeedbackCycle:
		tuning.PermuteControllers = controllerNames
	}

	switch hotspot.Type {
	case analyze.HotspotMissingTrigger:
		if len(controllerNames) > 0 {
			reader := controllerNames[0]
			kinds := groupKindsForResources(graph, hotspot.Resources)
			if len(kinds) > 0 {
				tuning.StaleReads[reader] = kinds
				setLookbackDefaults(tuning.StaleLookback, kinds)
			}
		}
	case analyze.HotspotReducer:
		if len(controllerNames) > 0 {
			inputs := groupKindsFromAttr(graph, hotspot.Attributes["inputs"])
			if len(inputs) > 0 {
				tuning.StaleReads[controllerNames[0]] = inputs
				setLookbackDefaults(tuning.StaleLookback, inputs)
			}
		}
	case analyze.HotspotFeedbackCycle:
		tuning.MaxDepth = feedbackMaxDepth
	}

	return tuning
}

func controllerNames(graph *analyze.Graph, controllers []analyze.NodeID) []string {
	out := make([]string, 0, len(controllers))
	for _, id := range controllers {
		if node, ok := graph.Nodes[id]; ok && node.Kind == analyze.NodeController {
			out = append(out, node.Controller.Name)
		}
	}
	return out
}

func groupKindsForResources(graph *analyze.Graph, resources []analyze.NodeID) []string {
	out := make([]string, 0, len(resources))
	seen := make(map[string]struct{})
	for _, id := range resources {
		node, ok := graph.Nodes[id]
		if !ok || node.Kind != analyze.NodeResource {
			continue
		}
		gk := util.CanonicalGroupKind(node.Resource.Group, node.Resource.Kind)
		if _, exists := seen[gk]; exists {
			continue
		}
		seen[gk] = struct{}{}
		out = append(out, gk)
	}
	sort.Strings(out)
	return out
}

func groupKindsFromAttr(graph *analyze.Graph, value string) []string {
	ids := parseNodeIDList(value)
	return groupKindsForResources(graph, ids)
}

func setLookbackDefaults(target map[string]int, kinds []string) {
	for _, kind := range kinds {
		if _, exists := target[kind]; exists {
			continue
		}
		target[kind] = defaultLookback
	}
}
