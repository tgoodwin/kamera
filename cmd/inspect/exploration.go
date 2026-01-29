package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/tgoodwin/kamera/pkg/interactive"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
)

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
