package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/tgoodwin/kamera/pkg/analyze"
	"github.com/tgoodwin/kamera/pkg/coverage"
)

func runGenerate(args []string) (int, error) {
	if len(args) == 0 {
		return 1, errors.New(generateUsage())
	}
	if isHelpArg(args[0]) {
		fmt.Fprintln(os.Stderr, generateUsage())
		return 0, nil
	}

	var (
		graphPath    string
		inputMapPath string
		outPath      string
		hotspotType  string
		limit        int
	)

	fs := flag.NewFlagSet("generate", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&graphPath, "graph", "", "path to dependency graph JSON")
	fs.StringVar(&inputMapPath, "input-map", "", "path to input map JSON")
	fs.StringVar(&outPath, "out", "", "path to output JSON")
	fs.StringVar(&hotspotType, "hotspot-type", "", "filter by hotspot type")
	fs.IntVar(&limit, "limit", 0, "limit number of hotspots")

	if err := fs.Parse(args); err != nil {
		return 1, err
	}
	if graphPath == "" || inputMapPath == "" || outPath == "" {
		return 1, errors.New(generateUsage())
	}
	if limit < 0 {
		return 1, fmt.Errorf("limit must be >= 0")
	}

	graph, err := loadGraph(graphPath)
	if err != nil {
		return 1, err
	}

	hotspots, err := analyze.DetectHotspots(graph)
	if err != nil {
		return 1, fmt.Errorf("detect hotspots: %w", err)
	}

	filtered := filterHotspots(hotspots, hotspotType, limit)
	if len(filtered) == 0 {
		return 1, fmt.Errorf("no hotspots found")
	}

	inputMap, err := coverage.LoadInputMap(inputMapPath)
	if err != nil {
		return 1, err
	}

	inputs, err := coverage.TranslateHotspots(graph, filtered, inputMap)
	if err != nil {
		return 1, err
	}

	data, err := json.MarshalIndent(inputs, "", "  ")
	if err != nil {
		return 1, fmt.Errorf("encode inputs: %w", err)
	}

	if err := os.WriteFile(outPath, data, 0644); err != nil {
		return 1, fmt.Errorf("write output: %w", err)
	}

	return 0, nil
}

func loadGraph(path string) (*analyze.Graph, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read graph: %w", err)
	}
	raw, err := analyze.ParseRawGraphJSON(data)
	if err != nil {
		return nil, fmt.Errorf("parse graph: %w", err)
	}
	graph, err := analyze.BuildGraphFromRaw(raw)
	if err != nil {
		return nil, fmt.Errorf("build graph: %w", err)
	}
	return graph, nil
}

func filterHotspots(hotspots []analyze.HotspotInstance, hotspotType string, limit int) []analyze.HotspotInstance {
	out := make([]analyze.HotspotInstance, 0, len(hotspots))
	for _, hotspot := range hotspots {
		if hotspotType != "" && string(hotspot.Type) != strings.TrimSpace(hotspotType) {
			continue
		}
		out = append(out, hotspot)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

func generateUsage() string {
	return "usage: generate --graph <graph.json> --input-map <input-map.json> --out <inputs.json> [--hotspot-type <type>] [--limit N]"
}

func isHelpArg(arg string) bool {
	return arg == "-h" || arg == "--help" || arg == "help"
}
