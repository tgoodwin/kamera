package explore

import (
	"crypto/sha256"
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tgoodwin/kamera/pkg/tracecheck"
)

// TrialMetric captures lightweight per-trial metrics for staleness interval
// phases, replacing the full ~700KB JSONL dump with a single CSV row.
type TrialMetric struct {
	ScenarioName   string
	PhaseName      string
	DurationNS     int64
	TotalStates    int
	ResourceStates int
	TerminalHash   string
	Converged      bool
	Reconciler     string
	Kind           string
	StaleAt        string
	CatchUpAt      string
	// ContentHashes contains the set of unique resource-state hashes observed
	// during this trial (one per distinct StateAfter in the execution path).
	ContentHashes []string
}

// TrialMetricsAccumulator collects TrialMetric records in a thread-safe manner.
type TrialMetricsAccumulator struct {
	mu      sync.Mutex
	metrics []TrialMetric
}

// NewTrialMetricsAccumulator creates a new empty accumulator.
func NewTrialMetricsAccumulator() *TrialMetricsAccumulator {
	return &TrialMetricsAccumulator{}
}

// Add appends a metric record.
func (a *TrialMetricsAccumulator) Add(m TrialMetric) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.metrics = append(a.metrics, m)
}

// Len returns the number of collected metrics.
func (a *TrialMetricsAccumulator) Len() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.metrics)
}

// WriteCSV writes all collected metrics to a CSV file.
func (a *TrialMetricsAccumulator) WriteCSV(path string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create CSV: %w", err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	header := []string{
		"scenario_name", "phase_name", "duration_ns", "total_states",
		"resource_states", "terminal_hash", "converged",
		"reconciler", "kind", "stale_at", "catch_up_at",
		"content_hashes",
	}
	if err := w.Write(header); err != nil {
		return fmt.Errorf("write CSV header: %w", err)
	}

	for _, m := range a.metrics {
		row := []string{
			m.ScenarioName,
			m.PhaseName,
			strconv.FormatInt(m.DurationNS, 10),
			strconv.Itoa(m.TotalStates),
			strconv.Itoa(m.ResourceStates),
			m.TerminalHash,
			strconv.FormatBool(m.Converged),
			m.Reconciler,
			m.Kind,
			m.StaleAt,
			m.CatchUpAt,
			strings.Join(m.ContentHashes, ";"),
		}
		if err := w.Write(row); err != nil {
			return fmt.Errorf("write CSV row: %w", err)
		}
	}
	return nil
}

// metricsFromPhase extracts a TrialMetric from a completed scenario phase.
func metricsFromPhase(
	scenario Scenario,
	phaseName string,
	phaseCtx ScenarioContext,
	stats *tracecheck.ExploreStats,
	res *tracecheck.Result,
	duration time.Duration,
) TrialMetric {
	m := TrialMetric{
		ScenarioName: scenario.Name,
		PhaseName:    phaseName,
		DurationNS:   duration.Nanoseconds(),
	}

	if stats != nil {
		m.TotalStates = stats.TotalNodeVisits
		m.ResourceStates = stats.UniqueResourceStates
	}

	if res != nil {
		m.Converged = len(res.ConvergedStates) > 0
		if len(res.ConvergedStates) > 0 {
			m.TerminalHash = res.ConvergedStates[0].ID
		} else if len(res.AbortedStates) > 0 {
			m.TerminalHash = res.AbortedStates[0].ID
		}
		// Collect unique content hashes from all paths
		seen := make(map[string]struct{})
		allStates := append(res.ConvergedStates, res.AbortedStates...)
		for _, rs := range allStates {
			for _, path := range rs.Paths {
				for _, step := range path {
					if step == nil {
						continue
					}
					h := hashObjectVersions(step.StateAfter)
					if _, ok := seen[h]; !ok {
						seen[h] = struct{}{}
						m.ContentHashes = append(m.ContentHashes, h)
					}
				}
			}
		}
	}

	if phaseCtx.Attributes != nil {
		m.Reconciler = phaseCtx.Attributes["perturbation.reconciler"]
		m.Kind = phaseCtx.Attributes["perturbation.kind"]
		m.StaleAt = phaseCtx.Attributes["perturbation.stale_at"]
		m.CatchUpAt = phaseCtx.Attributes["perturbation.catch_up_at"]
	}

	return m
}

// hashObjectVersions produces a short deterministic hash of an ObjectVersions map.
func hashObjectVersions(ov tracecheck.ObjectVersions) string {
	if len(ov) == 0 {
		return ""
	}
	keys := make([]string, 0, len(ov))
	for k, v := range ov {
		keys = append(keys, fmt.Sprintf("%s/%s=%s", k.ResourceKey.Group, k.Name, v.Value))
	}
	sort.Strings(keys)
	h := sha256.Sum256([]byte(strings.Join(keys, "\n")))
	return fmt.Sprintf("%x", h[:8])
}
