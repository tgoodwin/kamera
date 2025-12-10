package tracecheck

import (
	"fmt"
	"time"
)

type ExploreStats struct {
	startTime              *time.Time
	endTime                *time.Time
	AbortedPaths           int
	UniqueNodeVisits       int
	TotalNodeVisits        int
	SkippedNodeVisits      int
	SkippedPaths           int
	SkippedOrderExpansions int
	SkippedSubtrees        int
	SkippedNoOpOrderings   int
	EarlyConvergence       int
	NoOpReconciles         int
	CachePredictedSkips    int

	VisitsByDepth map[int]int

	RestartsPerReconciler map[ReconcilerID]int

	// reconcile step timing
	TotalReconcileSteps int
	TotalStepLatency    time.Duration
	MaxStepLatency      time.Duration
	StepLatencyByRecon  map[ReconcilerID]*latencyStat
}

type latencyStat struct {
	Count int
	Total time.Duration
	Max   time.Duration
	Min   time.Duration
}

func NewExploreStats() *ExploreStats {
	return &ExploreStats{
		startTime:    nil,
		AbortedPaths: 0,

		VisitsByDepth:         make(map[int]int),
		RestartsPerReconciler: make(map[ReconcilerID]int),
		StepLatencyByRecon:    make(map[ReconcilerID]*latencyStat),
	}
}

func (s *ExploreStats) RecordVisit(depth int, pendingCount int, queueSize int) {
	if s.VisitsByDepth == nil {
		s.VisitsByDepth = make(map[int]int)
	}
	s.VisitsByDepth[depth]++
	// pendingCount and queueSize available for future analysis
}

func (s *ExploreStats) Start() {
	startTime := time.Now()
	s.startTime = &startTime
}

func (s *ExploreStats) Finish() {
	endTime := time.Now()
	s.endTime = &endTime
}

func (s *ExploreStats) RecordStep(reconcilerID ReconcilerID, d time.Duration) {
	s.TotalReconcileSteps++
	s.TotalStepLatency += d
	if d > s.MaxStepLatency {
		s.MaxStepLatency = d
	}
	ls, ok := s.StepLatencyByRecon[reconcilerID]
	if !ok {
		ls = &latencyStat{Min: d}
		s.StepLatencyByRecon[reconcilerID] = ls
	}
	ls.Count++
	ls.Total += d
	if d > ls.Max {
		ls.Max = d
	}
	if d < ls.Min {
		ls.Min = d
	}
}

func (ls *latencyStat) Avg() time.Duration {
	if ls == nil || ls.Count == 0 {
		return 0
	}
	return time.Duration(int64(ls.Total) / int64(ls.Count))
}

func (s *ExploreStats) Print() {
	if s.endTime == nil {
		s.Finish()
	}
	var avgStep time.Duration
	if s.TotalReconcileSteps > 0 {
		avgStep = time.Duration(int64(s.TotalStepLatency) / int64(s.TotalReconcileSteps))
	}
	if logger.GetSink() != nil {
		logger.Info("explore stats",
			"totalTime", s.endTime.Sub(*s.startTime),
			"totalNodeVisits", s.TotalNodeVisits,
			"uniqueNodeVisits", s.UniqueNodeVisits,
			"skippedPaths", s.SkippedPaths,
			"abortedPaths", s.AbortedPaths,
			"reconcileSteps", s.TotalReconcileSteps,
			"avgStepLatency", avgStep,
			"maxStepLatency", s.MaxStepLatency,
			"stepLatencyByReconciler", s.latencyByReconcilerSummary(),
		)
	}
	fmt.Printf("Total time: %v\n", s.endTime.Sub(*s.startTime))
	fmt.Printf("Total node visits: %d\n", s.TotalNodeVisits)
	fmt.Printf("Unique node visits: %d\n", s.UniqueNodeVisits)
	fmt.Printf("Skipped node visits: %d\n", s.SkippedNodeVisits)
	fmt.Printf("Skipped paths: %d\n", s.SkippedPaths)
	fmt.Printf("Skipped order expansions: %d\n", s.SkippedOrderExpansions)
	fmt.Printf("Skipped subtrees: %d\n", s.SkippedSubtrees)
	fmt.Printf("Skipped no-op orderings: %d\n", s.SkippedNoOpOrderings)
	fmt.Printf("Early convergence: %d\n", s.EarlyConvergence)
	fmt.Printf("No-op reconciles: %d\n", s.NoOpReconciles)
	fmt.Printf("Cache predicted skips: %d\n", s.CachePredictedSkips)
	fmt.Printf("Aborted paths: %d\n", s.AbortedPaths)
	fmt.Printf("Reconcile steps: %d\n", s.TotalReconcileSteps)
	fmt.Printf("Avg step latency: %v\n", avgStep)
	fmt.Printf("Max step latency: %v\n", s.MaxStepLatency)
	fmt.Println("Step latency by reconciler:")
	s.printLatencyTable()
}

func (s *ExploreStats) latencyByReconcilerSummary() map[string]map[string]any {
	out := make(map[string]map[string]any, len(s.StepLatencyByRecon))
	for reconID, ls := range s.StepLatencyByRecon {
		if ls == nil || ls.Count == 0 {
			continue
		}
		out[string(reconID)] = map[string]any{
			"count": ls.Count,
			"avg":   ls.Avg(),
			"max":   ls.Max,
			"min":   ls.Min,
		}
	}
	return out
}

func (s *ExploreStats) printLatencyTable() {
	if len(s.StepLatencyByRecon) == 0 {
		fmt.Println("  (no steps recorded)")
		return
	}
	fmt.Printf("  %-25s %-6s %-12s %-12s %-12s\n", "Reconciler", "Count", "Avg", "Max", "Min")
	for reconID, ls := range s.StepLatencyByRecon {
		if ls == nil || ls.Count == 0 {
			continue
		}
		fmt.Printf("  %-25s %-6d %-12v %-12v %-12v\n", reconID, ls.Count, ls.Avg(), ls.Max, ls.Min)
	}
}
