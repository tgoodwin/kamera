package kamera

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/tgoodwin/kamera/pkg/analysis"
)

type campaignInvocationAggregate struct {
	InvocationID         string
	Dumps                int
	UniqueNodeVisits     int64
	TotalNodeVisits      int64
	UniqueResourceStates int64
	DurationNS           int64
	ErrorDumps           int
	AbortedStates        int
	MaxDepthAborted      int
}

func runCampaignMetrics(stdout, stderr io.Writer, targetPath string) error {
	paths, err := discoverDumpPaths(targetPath)
	if err != nil {
		return err
	}
	if len(paths) == 0 {
		return fmt.Errorf("no dump files found in %s", targetPath)
	}

	byInvocation := map[string]*campaignInvocationAggregate{}
	skippedMissingInvocation := 0
	skippedMissingMetrics := 0

	for _, path := range paths {
		dump, err := analysis.LoadDump(path)
		if err != nil {
			return fmt.Errorf("load dump %s: %w", path, err)
		}

		invocationID := dumpInvocationID(dump)
		if invocationID == "" {
			skippedMissingInvocation++
			continue
		}

		agg, ok := byInvocation[invocationID]
		if !ok {
			agg = &campaignInvocationAggregate{InvocationID: invocationID}
			byInvocation[invocationID] = agg
		}
		agg.Dumps++
		if isErrorDump(dump) {
			agg.ErrorDumps++
		}
		aborted, maxDepth := countAbortedStates(dump)
		agg.AbortedStates += aborted
		agg.MaxDepthAborted += maxDepth

		if dump.CampaignMetrics == nil {
			skippedMissingMetrics++
			continue
		}

		agg.UniqueNodeVisits += int64(dump.CampaignMetrics.UniqueNodeVisits)
		agg.TotalNodeVisits += int64(dump.CampaignMetrics.TotalNodeVisits)
		agg.UniqueResourceStates += int64(dump.CampaignMetrics.UniqueResourceStates)
		agg.DurationNS += dump.CampaignMetrics.DurationNS
	}

	if len(byInvocation) == 0 {
		return fmt.Errorf("no dumps with invocation_id in %s", targetPath)
	}

	keys := make([]string, 0, len(byInvocation))
	for key := range byInvocation {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	color := newCampaignOutputColor(stdout)
	fmt.Fprintf(stdout, "%sCampaign metrics by invocation%s\n", color.Header, color.Reset)
	fmt.Fprintln(stdout)
	for idx, key := range keys {
		agg := byInvocation[key]
		fmt.Fprintf(stdout, "%sinvocation:%s %s%s%s\n", color.Key, color.Reset, color.InvocationID, agg.InvocationID, color.Reset)
		tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintf(tw, "  unique node visits:\t%s\n", formatIntWithCommas(agg.UniqueNodeVisits))
		fmt.Fprintf(tw, "  total node visits:\t%s\n", formatIntWithCommas(agg.TotalNodeVisits))
		fmt.Fprintf(tw, "  unique resource states:\t%s\n", formatIntWithCommas(agg.UniqueResourceStates))
		fmt.Fprintf(tw, "  duration:\t%s\n", formatDurationCompact(agg.DurationNS))
		fmt.Fprintf(tw, "  aborted states:\t%s\n", colorizeAbortedCount(formatIntWithCommas(int64(agg.AbortedStates)), color))
		fmt.Fprintf(tw, "  max-depth aborted states:\t%s\n", colorizeAbortedCount(formatIntWithCommas(int64(agg.MaxDepthAborted)), color))
		_ = tw.Flush()
		if idx < len(keys)-1 {
			fmt.Fprintln(stdout)
		}
	}

	if skippedMissingInvocation > 0 {
		fmt.Fprintf(stderr, "skipped %d dump(s) without invocation_id\n", skippedMissingInvocation)
	}
	if skippedMissingMetrics > 0 {
		fmt.Fprintf(stderr, "found %d dump(s) without campaignMetrics (metric totals unchanged for those dumps)\n", skippedMissingMetrics)
	}

	return nil
}

func isErrorDump(dump *analysis.Dump) bool {
	if dump == nil || dump.Context == nil || dump.Context.Scenario == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(dump.Context.Scenario.Attributes["status"]), "error")
}

func countAbortedStates(dump *analysis.Dump) (aborted int, maxDepth int) {
	if dump == nil {
		return 0, 0
	}
	for _, state := range dump.States {
		errMsg := strings.TrimSpace(state.Error)
		if errMsg == "" {
			continue
		}
		aborted++
		if strings.Contains(strings.ToLower(errMsg), "max depth") {
			maxDepth++
		}
	}
	return aborted, maxDepth
}

func discoverDumpPaths(targetPath string) ([]string, error) {
	info, err := os.Stat(targetPath)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return []string{targetPath}, nil
	}

	entries, err := os.ReadDir(targetPath)
	if err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		ext := strings.ToLower(filepath.Ext(entry.Name()))
		if ext != ".jsonl" && ext != ".json" {
			continue
		}
		paths = append(paths, filepath.Join(targetPath, entry.Name()))
	}
	sort.Strings(paths)
	return paths, nil
}

func dumpInvocationID(dump *analysis.Dump) string {
	if dump == nil || dump.Context == nil || dump.Context.Scenario == nil {
		return ""
	}
	return strings.TrimSpace(dump.Context.Scenario.Attributes["invocation_id"])
}

func formatIntWithCommas(v int64) string {
	if v == 0 {
		return "0"
	}
	negative := v < 0
	if negative {
		v = -v
	}
	raw := strconv.FormatInt(v, 10)
	if len(raw) <= 3 {
		if negative {
			return "-" + raw
		}
		return raw
	}

	var b strings.Builder
	if negative {
		b.WriteByte('-')
	}
	for idx := 0; idx < len(raw); idx++ {
		if idx > 0 && (len(raw)-idx)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteByte(raw[idx])
	}
	return b.String()
}

func formatDurationCompact(v int64) string {
	if v <= 0 {
		return "0s"
	}
	d := time.Duration(v).Round(time.Second)
	if d <= 0 {
		return "0s"
	}
	return d.String()
}

type campaignOutputColor struct {
	Header       string
	Key          string
	InvocationID string
	Aborted      string
	Reset        string
}

func newCampaignOutputColor(w io.Writer) campaignOutputColor {
	if !supportsANSIColor(w) {
		return campaignOutputColor{}
	}
	return campaignOutputColor{
		Header:       "\033[1;36m",
		Key:          "\033[1;34m",
		InvocationID: "\033[33m",
		Aborted:      "\033[31m",
		Reset:        "\033[0m",
	}
}

func supportsANSIColor(w io.Writer) bool {
	if strings.TrimSpace(os.Getenv("NO_COLOR")) != "" {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(os.Getenv("TERM")), "dumb") {
		return false
	}
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}

func colorizeAbortedCount(count string, color campaignOutputColor) string {
	if color.Aborted == "" || color.Reset == "" {
		return count
	}
	return color.Aborted + count + color.Reset
}
