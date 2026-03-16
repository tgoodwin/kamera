package coverage

import (
	"fmt"
	"sort"
	"strings"

	"github.com/tgoodwin/kamera/pkg/analyze"
	"github.com/tgoodwin/kamera/pkg/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const (
	defaultNamespace = "default"
)

// TranslateHotspots converts hotspot instances into concrete Inputs.
func TranslateHotspots(graph *analyze.Graph, hotspots []analyze.HotspotInstance, inputMap InputMap) ([]Input, error) {
	if graph == nil {
		return nil, fmt.Errorf("graph is nil")
	}
	return nil, fmt.Errorf("TranslateHotspots deprecated")

	// out := make([]Input, 0, len(hotspots))
	// for i, hotspot := range hotspots {
	// 	resourceIDs := collectResourceIDs(hotspot)
	// 	resourceList := sortedNodeIDs(resourceIDs)

	// 	userInputs := make([]UserInput, 0, len(resourceList))

	// 	for _, nodeID := range resourceList {
	// 		node, ok := graph.Nodes[nodeID]
	// 		if !ok {
	// 			return nil, fmt.Errorf("resource node %s not found in graph", nodeID)
	// 		}
	// 		if node.Kind != analyze.NodeResource {
	// 			return nil, fmt.Errorf("node %s is not a resource", nodeID)
	// 		}

	// 		gvk := gvkString(node.Resource)
	// 		templates, ok := inputMap.Mapping[gvk]
	// 		if !ok {
	// 			return nil, fmt.Errorf("input map missing template for %s", gvk)
	// 		}
	// 		if len(templates) != 1 {
	// 			return nil, fmt.Errorf("input map for %s must contain exactly one template", gvk)
	// 		}
	// 		if templates[0].Object == nil {
	// 			return nil, fmt.Errorf("input map for %s has nil template object", gvk)
	// 		}

	// 		// make some unique name/namespace for the resource instance
	// 		name := normalizedName(hotspot.Type, i, node.Resource.Kind)
	// 		ns := namespaceForTemplate(templates[0].Object)
	// 		obj := NormalizeTemplate(templates[0].Object, name, ns)
	// 		userInputs = append(userInputs, UserInput{
	// 			ID:     fmt.Sprintf("user-input-%d", len(userInputs)),
	// 			Type:   event.CREATE,
	// 			Object: obj,
	// 		})
	// 	}

	// 	tuning := buildTuning(hotspot, graph)
	// 	input := Input{
	// 		Name:       fmt.Sprintf("hotspot-%s-%d", hotspot.Type, i),
	// 		UserInputs: userInputs,
	// 		Tuning:     tuning,
	// 	}
	// 	out = append(out, input)
	// }

	// return out, nil
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
