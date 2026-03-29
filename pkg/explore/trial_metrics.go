package explore

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
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
	}

	if phaseCtx.Attributes != nil {
		m.Reconciler = phaseCtx.Attributes["perturbation.reconciler"]
		m.Kind = phaseCtx.Attributes["perturbation.kind"]
		m.StaleAt = phaseCtx.Attributes["perturbation.stale_at"]
		m.CatchUpAt = phaseCtx.Attributes["perturbation.catch_up_at"]
	}

	return m
}
