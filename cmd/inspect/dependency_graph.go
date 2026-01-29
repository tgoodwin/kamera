package main

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"

	"github.com/tgoodwin/kamera/pkg/analyze"
)

func dependencyGraphDOT(path string) (string, error) {
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

func openArgs(path string) []string {
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

func runDependencyGraph(args []string) int {
	if len(args) == 1 && isHelpArg(args[0]) {
		fmt.Fprintln(os.Stderr, dependencyGraphUsage())
		return 0
	}
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, dependencyGraphUsage())
		return 1
	}

	dot, err := dependencyGraphDOT(args[0])
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

	openCmdArgs := openArgs(pdfFile.Name())
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
