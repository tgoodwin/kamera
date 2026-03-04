package kamera

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/tgoodwin/kamera/pkg/analysis"
)

type campaignInvocationAggregate struct {
	InvocationID         string
	Dumps                int
	UniqueNodeVisits     int64
	TotalNodeVisits      int64
	UniqueResourceStates int64
	DurationNS           int64
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
		if dump.CampaignMetrics == nil {
			skippedMissingMetrics++
			continue
		}

		agg, ok := byInvocation[invocationID]
		if !ok {
			agg = &campaignInvocationAggregate{InvocationID: invocationID}
			byInvocation[invocationID] = agg
		}
		agg.Dumps++
		agg.UniqueNodeVisits += int64(dump.CampaignMetrics.UniqueNodeVisits)
		agg.TotalNodeVisits += int64(dump.CampaignMetrics.TotalNodeVisits)
		agg.UniqueResourceStates += int64(dump.CampaignMetrics.UniqueResourceStates)
		agg.DurationNS += dump.CampaignMetrics.DurationNS
	}

	if len(byInvocation) == 0 {
		return fmt.Errorf("no dumps with both invocation_id and campaignMetrics in %s", targetPath)
	}

	keys := make([]string, 0, len(byInvocation))
	for key := range byInvocation {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	tw := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "invocation_id\tdumps\tunique_node_visits\ttotal_node_visits\tunique_resource_states\tduration_ns")
	for _, key := range keys {
		agg := byInvocation[key]
		fmt.Fprintf(
			tw,
			"%s\t%d\t%d\t%d\t%d\t%d\n",
			agg.InvocationID,
			agg.Dumps,
			agg.UniqueNodeVisits,
			agg.TotalNodeVisits,
			agg.UniqueResourceStates,
			agg.DurationNS,
		)
	}
	_ = tw.Flush()

	if skippedMissingInvocation > 0 {
		fmt.Fprintf(stderr, "skipped %d dump(s) without invocation_id\n", skippedMissingInvocation)
	}
	if skippedMissingMetrics > 0 {
		fmt.Fprintf(stderr, "skipped %d dump(s) without campaignMetrics\n", skippedMissingMetrics)
	}

	return nil
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

