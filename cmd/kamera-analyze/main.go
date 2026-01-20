package main

import (
	"fmt"
	"os"

	"github.com/tgoodwin/kamera/pkg/analysis"
)

func main() {
	if len(os.Args) < 3 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	dumpPath := os.Args[2]

	dump, err := analysis.LoadDump(dumpPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading dump: %v\n", err)
		os.Exit(1)
	}

	switch cmd {
	case "diff":
		runDiff(dump)
	case "report":
		runReport(dump)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: kamera-analyze <command> <dump.jsonl>")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  diff    Show differences between converged states")
	fmt.Println("  report  Full backward-trace analysis report")
}

func runDiff(dump *analysis.Dump) {
	diff := analysis.DiffConvergedStates(dump)
	fmt.Print(analysis.FormatConvergedStateDiff(diff))
}

func runReport(dump *analysis.Dump) {
	// Module 0: Diff
	diff := analysis.DiffConvergedStates(dump)
	fmt.Println("=== Converged State Diff ===")
	fmt.Print(analysis.FormatConvergedStateDiff(diff))

	if len(diff.DifferingObjects) == 0 {
		fmt.Println("No differing objects - states are identical")
		return
	}

	// Module 1: Last Write for each differing object
	fmt.Println("=== Last Write Analysis ===")
	for _, objDiff := range diff.DifferingObjects {
		result := analysis.AnalyzeLastWrite(dump, objDiff.Key)
		fmt.Print(analysis.FormatLastWriteAnalysis(result))
	}
}
