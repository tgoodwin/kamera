package kamera

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strings"

	"github.com/tgoodwin/kamera/pkg/analyze"
	"github.com/tgoodwin/kamera/pkg/interactive"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
)

type hotspotSummary struct {
	Type        string            `json:"type"`
	Nodes       []string          `json:"nodes"`
	Resources   []string          `json:"resources,omitempty"`
	Controllers []string          `json:"controllers,omitempty"`
	Attributes  map[string]string `json:"attributes,omitempty"`
}

func RunInspect(args []string) (int, error) {
	if len(args) == 0 {
		return 1, errors.New(inspectUsage())
	}
	if isHelpArg(args[0]) {
		fmt.Fprintln(os.Stderr, inspectUsage())
		return 0, nil
	}

	switch args[0] {
	case "dependency-graph":
		return runDependencyGraph(args[1:]), nil
	case "exploration":
		return runExploration(args[1:]), nil
	case "hotspots":
		return runHotspots(args[1:]), nil
	default:
		return 1, errors.New(inspectUsage())
	}
}

func runExploration(args []string) int {
	fs := flag.NewFlagSet("exploration", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, explorationUsage())
	}
	var dotPath string
	var interactiveMode bool
	fs.StringVar(&dotPath, "dot", "", "Optional path to write the state DAG in Graphviz DOT format")
	fs.BoolVar(&interactiveMode, "interactive", true, "Launch interactive TUI (set false for headless DAG output)")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 1
	}

	dumpPath := fs.Arg(0)
	if dumpPath == "" {
		fmt.Fprintln(os.Stderr, explorationUsage())
		return 1
	}

	states, resolver, err := interactive.LoadInspectorDump(dumpPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load dump: %v\n", err)
		return 1
	}

	dag := tracecheck.BuildStateDAG(tracecheck.Result{ConvergedStates: states})
	dot := tracecheck.RenderStateDAGDOT(dag, tracecheck.GraphvizOpts{LabelEdges: true, DropSelfLoops: true})

	if dotPath != "" {
		if err := os.WriteFile(dotPath, []byte(dot), 0644); err != nil {
			fmt.Fprintf(os.Stderr, "write dot: %v\n", err)
			return 1
		}
		fmt.Fprintf(os.Stderr, "wrote DAG to %s\n", dotPath)
	}

	if !interactiveMode {
		if dotPath == "" {
			fmt.Print(dot)
		}
		// Always include node details for LLM analysis context
		fmt.Print(tracecheck.RenderStateDAGNodeDetails(dag))
		// Include ContentsHash to step mapping for cross-referencing
		fmt.Print(tracecheck.RenderContentsHashMapping(states))
		return 0
	}

	if _, err := interactive.RunStateInspectorTUIView(states, resolver, false, tracecheck.ExploreConfig{}); err != nil {
		fmt.Fprintf(os.Stderr, "run inspector: %v\n", err)
		return 1
	}

	return 0
}

func explorationUsage() string {
	return "usage: inspect exploration <dump> [--dot <path>] [--interactive=false]"
}

func runDependencyGraph(args []string) int {
	if len(args) == 1 && isHelpArg(args[0]) {
		fmt.Fprintln(os.Stderr, dependencyGraphUsage())
		return 0
	}
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, dependencyGraphUsage())
		return 1
	}

	dot, err := DependencyGraphDOT(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "load dependency graph: %v\n", err)
		return 1
	}

	pdfFile, err := os.CreateTemp("/tmp", "dependency-graph-*.pdf")
	if err != nil {
		fmt.Fprintf(os.Stderr, "create temp pdf: %v\n", err)
		return 1
	}

	cmd := exec.Command("dot", "-Tpdf")
	cmd.Stdin = strings.NewReader(dot)
	cmd.Stdout = pdfFile
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		_ = pdfFile.Close()
		fmt.Fprintf(os.Stderr, "render pdf: %v\n", err)
		return 1
	}

	if err := pdfFile.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "close pdf: %v\n", err)
		return 1
	}

	openCmdArgs := OpenArgs(pdfFile.Name())
	openCmd := exec.Command(openCmdArgs[0], openCmdArgs[1:]...)
	if err := openCmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "open pdf: %v\n", err)
		return 1
	}

	fmt.Fprintf(os.Stderr, "opened dependency graph from %s\n", args[0])
	fmt.Fprintf(os.Stderr, "pdf saved at %s\n", pdfFile.Name())
	return 0
}

func dependencyGraphUsage() string {
	return "usage: inspect dependency-graph <graph.json>"
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

func nodeIDStrings(values []analyze.NodeID) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, string(value))
	}
	return out
}

func DependencyGraphDOT(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	raw, err := analyze.ParseRawGraphJSON(data)
	if err != nil {
		return "", err
	}

	graph, err := analyze.BuildGraphFromRaw(raw)
	if err != nil {
		return "", err
	}

	return analyze.RenderDependencyGraphDOT(graph), nil
}

func OpenArgs(path string) []string {
	switch runtime.GOOS {
	case "darwin":
		return []string{"open", path}
	case "linux":
		return []string{"xdg-open", path}
	case "windows":
		return []string{"cmd", "/c", "start", "", path}
	default:
		return []string{"open", path}
	}
}

func inspectUsage() string {
	return "usage: inspect <dependency-graph|exploration|hotspots> <args>"
}
