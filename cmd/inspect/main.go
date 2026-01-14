package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/tgoodwin/kamera/pkg/interactive"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
)

func main() {
	var dumpPath string
	var dotPath string
	var interactiveMode bool
	flag.StringVar(&dumpPath, "dump", "", "Path to an inspector dump file to load")
	flag.StringVar(&dotPath, "dot", "", "Optional path to write the state DAG in Graphviz DOT format")
	flag.BoolVar(&interactiveMode, "interactive", true, "Launch interactive TUI (set false for headless DAG output)")
	flag.Parse()

	if dumpPath == "" {
		fmt.Fprintln(os.Stderr, "usage: inspect --dump <path> [--dot <path>]")
		os.Exit(1)
	}

	states, resolver, err := interactive.LoadInspectorDump(dumpPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load dump: %v\n", err)
		os.Exit(1)
	}

	dag := tracecheck.BuildStateDAG(tracecheck.Result{ConvergedStates: states})
	dot := tracecheck.RenderStateDAGDOT(dag, tracecheck.GraphvizOpts{LabelEdges: true, DropSelfLoops: true})

	if dotPath != "" {
		if err := os.WriteFile(dotPath, []byte(dot), 0644); err != nil {
			fmt.Fprintf(os.Stderr, "write dot: %v\n", err)
			os.Exit(1)
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
		return
	}

	if _, err := interactive.RunStateInspectorTUIView(states, resolver, false, tracecheck.ExploreConfig{}); err != nil {
		fmt.Fprintf(os.Stderr, "run inspector: %v\n", err)
		os.Exit(1)
	}
}
