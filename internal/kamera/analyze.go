package kamera

import (
	"fmt"
	"io"

	"github.com/tgoodwin/kamera/pkg/analysis"
)

func RunAnalyze(args []string, stdout, stderr io.Writer) int {
	if len(args) == 1 && isHelpArg(args[0]) {
		printAnalyzeUsage(stdout)
		return 0
	}
	if len(args) < 2 {
		printAnalyzeUsage(stdout)
		return 1
	}

	cmd := args[0]

	switch cmd {
	case "diff":
		dumpPath := args[1]
		dump, err := analysis.LoadDump(dumpPath)
		if err != nil {
			fmt.Fprintf(stderr, "Error loading dump: %v\n", err)
			return 1
		}
		runDiff(stdout, dump)
	case "report":
		dumpPath := args[1]
		dump, err := analysis.LoadDump(dumpPath)
		if err != nil {
			fmt.Fprintf(stderr, "Error loading dump: %v\n", err)
			return 1
		}
		runReport(stdout, dump)
	case "campaign-metrics":
		if err := runCampaignMetrics(stdout, stderr, args[1]); err != nil {
			fmt.Fprintf(stderr, "Error aggregating campaign metrics: %v\n", err)
			return 1
		}
	default:
		fmt.Fprintf(stderr, "Unknown command: %s\n", cmd)
		printAnalyzeUsage(stdout)
		return 1
	}

	return 0
}

func printAnalyzeUsage(stdout io.Writer) {
	fmt.Fprintln(stdout, "Usage: kamera analyze <command> <dump.jsonl>")
	fmt.Fprintln(stdout)
	fmt.Fprintln(stdout, "Commands:")
	fmt.Fprintln(stdout, "  diff    Show differences between converged states")
	fmt.Fprintln(stdout, "  report  Full backward-trace analysis report")
	fmt.Fprintln(stdout, "  campaign-metrics  Aggregate campaign metrics by invocation_id")
}

func runDiff(stdout io.Writer, dump *analysis.Dump) {
	diff := analysis.DiffConvergedStates(dump)
	fmt.Fprint(stdout, analysis.FormatConvergedStateDiff(diff))
}

func runReport(stdout io.Writer, dump *analysis.Dump) {
	// Module 0: Diff
	diff := analysis.DiffConvergedStates(dump)
	fmt.Fprintln(stdout, "=== Converged State Diff ===")
	fmt.Fprint(stdout, analysis.FormatConvergedStateDiff(diff))

	if len(diff.DifferingObjects) == 0 {
		fmt.Fprintln(stdout, "No differing objects - states are identical")
		return
	}

	// Module 1: Last Write for each differing object
	fmt.Fprintln(stdout, "=== Last Write Analysis ===")
	for _, objDiff := range diff.DifferingObjects {
		result := analysis.AnalyzeLastWrite(dump, objDiff.Key)
		fmt.Fprint(stdout, analysis.FormatLastWriteAnalysis(result))
	}
}
