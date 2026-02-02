package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/tgoodwin/kamera/pkg/analyze"
)

type hotspotSummary struct {
	Type        string            `json:"type"`
	Kind        string            `json:"kind"`
	Nodes       []string          `json:"nodes"`
	Resources   []string          `json:"resources,omitempty"`
	Controllers []string          `json:"controllers,omitempty"`
	Attributes  map[string]string `json:"attributes,omitempty"`
}

func runHotspots(args []string) int {
	if len(args) == 1 && isHelpArg(args[0]) {
		fmt.Fprintln(os.Stderr, hotspotsUsage())
		return 0
	}
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, hotspotsUsage())
		return 1
	}

	graphPath := args[0]
	data, err := os.ReadFile(graphPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read graph: %v\n", err)
		return 1
	}

	raw, err := analyze.ParseRawGraphJSON(data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse graph: %v\n", err)
		return 1
	}

	graph, err := analyze.BuildGraphFromRaw(raw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "build graph: %v\n", err)
		return 1
	}

	hotspots, err := analyze.DetectHotspots(graph)
	if err != nil {
		fmt.Fprintf(os.Stderr, "detect hotspots: %v\n", err)
		return 1
	}

	summaries := summarizeHotspots(hotspots)
	payload, err := json.MarshalIndent(summaries, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "encode hotspots: %v\n", err)
		return 1
	}

	fmt.Println(string(payload))
	return 0
}

func hotspotsUsage() string {
	return "usage: inspect hotspots <graph.json>"
}

func summarizeHotspots(hotspots []analyze.HotspotInstance) []hotspotSummary {
	out := make([]hotspotSummary, 0, len(hotspots))
	for _, hotspot := range hotspots {
		rawControllers := nodeIDStrings(hotspot.Controllers)
		rawResources := nodeIDStrings(hotspot.Resources)
		nodes := append(append([]string{}, rawControllers...), rawResources...)
		sort.Strings(nodes)

		controllers := stripPrefix("c:", rawControllers)
		resources := stripPrefix("r:", rawResources)
		sort.Strings(controllers)
		sort.Strings(resources)

		summary := hotspotSummary{
			Type:        string(hotspot.Type),
			Kind:        humanHotspotKind(hotspot.Type),
			Nodes:       nodes,
			Resources:   resources,
			Controllers: controllers,
			Attributes:  hotspot.Attributes,
		}
		out = append(out, summary)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Type != out[j].Type {
			return out[i].Type < out[j].Type
		}
		return strings.Join(out[i].Nodes, ",") < strings.Join(out[j].Nodes, ",")
	})

	return out
}

func stripPrefix(prefix string, values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, strings.TrimPrefix(value, prefix))
	}
	return out
}

func humanHotspotKind(kind analyze.HotspotType) string {
	switch kind {
	case analyze.HotspotMultiWriter:
		return "multi-writer contention"
	case analyze.HotspotMissingTrigger:
		return "missing trigger / stale read"
	case analyze.HotspotDiamondPattern:
		return "fan-out converging writes"
	case analyze.HotspotReducer:
		return "aggregation/join"
	case analyze.HotspotFeedbackCycle:
		return "feedback cycle"
	default:
		return string(kind)
	}
}

func nodeIDStrings(values []analyze.NodeID) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, string(value))
	}
	return out
}
