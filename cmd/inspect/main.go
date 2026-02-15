package main

import (
	"errors"
	"fmt"
	"os"
)

func main() {
	code, err := runInspect(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	os.Exit(code)
}

func runInspect(args []string) (int, error) {
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

func isHelpArg(arg string) bool {
	return arg == "-h" || arg == "--help" || arg == "help"
}

func inspectUsage() string {
	return "usage: inspect <dependency-graph|exploration|hotspots> <args>"
}
