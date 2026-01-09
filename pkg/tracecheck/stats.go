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
	UniqueLogicalStates    int
	TotalNodeVisits        int
	SkippedNodeVisits      int
	SkippedPaths           int
	SkippedOrderExpansions int
	SkippedSubtrees        int // entire subtrees skipped (same logical state already explored)
	EarlyConvergence       int // states treated as converged because all pending are known no-ops
	NoOpReconciles         int // reconciles that produced no changes
	SkippedNoOpOrderings   int // orderings skipped because they put a known no-op first
	CachePredictedSkips    int // reconciles skipped via cache prediction (would be duplicates)
	SubtreeCompletionSkips int // logical states skipped because subtree was already fully explored
	SubtreeDiamondSkips    int // logical states skipped because already being explored (diamond convergence)

	RestartsPerReconciler map[ReconcilerID]int

	// reconcile step timing
	TotalReconcileSteps int
	TotalStepLatency    time.Duration
	MaxStepLatency      time.Duration
	StepLatencyByRecon  map[ReconcilerID]*latencyStat

	// depth tracking
	VisitsByDepth       map[int]int
	PendingCountByDepth map[int][]int // track pending reconcile counts at each depth
	MaxQueueDepth       int
	QueueDepthSamples   []int // sample queue depth periodically
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

		RestartsPerReconciler: make(map[ReconcilerID]int),
		StepLatencyByRecon:    make(map[ReconcilerID]*latencyStat),

		VisitsByDepth:       make(map[int]int),
		PendingCountByDepth: make(map[int][]int),
		QueueDepthSamples:   make([]int, 0),
	}
}

func (s *ExploreStats) RecordVisit(depth int, pendingCount int, queueDepth int) {
	s.VisitsByDepth[depth]++
	s.PendingCountByDepth[depth] = append(s.PendingCountByDepth[depth], pendingCount)
	if queueDepth > s.MaxQueueDepth {
		s.MaxQueueDepth = queueDepth
	}
	// sample queue depth every 100 visits
	if s.TotalNodeVisits%100 == 0 {
		s.QueueDepthSamples = append(s.QueueDepthSamples, queueDepth)
	}
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

	fmt.Printf("Total time: %v\n", s.endTime.Sub(*s.startTime))
	fmt.Printf("Total node visits: %d\n", s.TotalNodeVisits)
	fmt.Printf("Unique node visits: %d\n", s.UniqueNodeVisits)
	fmt.Printf("Unique logical states: %d\n", s.UniqueLogicalStates)
	fmt.Printf("Skipped node visits: %d\n", s.SkippedNodeVisits)
	fmt.Printf("Skipped paths: %d\n", s.SkippedPaths)
	fmt.Printf("Skipped order expansions: %d\n", s.SkippedOrderExpansions)
	fmt.Printf("Skipped subtrees: %d\n", s.SkippedSubtrees)
	fmt.Printf("Subtree completion skips: %d\n", s.SubtreeCompletionSkips)
	fmt.Printf("Subtree diamond skips: %d\n", s.SubtreeDiamondSkips)
	fmt.Printf("Early convergence: %d\n", s.EarlyConvergence)
	fmt.Printf("No-op reconciles: %d\n", s.NoOpReconciles)
	fmt.Printf("Skipped no-op orderings: %d\n", s.SkippedNoOpOrderings)
	fmt.Printf("Cache predicted skips: %d\n", s.CachePredictedSkips)
	fmt.Printf("Aborted paths: %d\n", s.AbortedPaths)
	fmt.Println("Optimization fires:")
	fmt.Printf("  orderingPruning branch skips: %d\n", s.SkippedOrderExpansions)
	fmt.Printf("  orderingPruning no-op skips: %d\n", s.SkippedNoOpOrderings)
	fmt.Printf("  completedPathDedup skips: %d\n", s.SkippedPaths)
	fmt.Printf("  cachePrediction skips: %d\n", s.CachePredictedSkips)
	fmt.Printf("  earlyConvergence skips: %d\n", s.EarlyConvergence)
	fmt.Printf("  subtreeCompletion skips: %d\n", s.SubtreeCompletionSkips)
	fmt.Printf("  subtreeCompletion diamond skips: %d\n", s.SubtreeDiamondSkips)
	fmt.Printf("Reconcile steps: %d\n", s.TotalReconcileSteps)
	fmt.Printf("Avg step latency: %v\n", avgStep)
	fmt.Printf("Max step latency: %v\n", s.MaxStepLatency)
	fmt.Println("Step latency by reconciler:")
	s.printLatencyTable()

	// Print depth distribution
	s.printDepthDistribution()
}

func (s *ExploreStats) printDepthDistribution() {
	if len(s.VisitsByDepth) == 0 {
		return
	}

	fmt.Println("\nVisits by depth:")
	// Find min and max depth
	minDepth, maxDepth := -1, -1
	for d := range s.VisitsByDepth {
		if minDepth == -1 || d < minDepth {
			minDepth = d
		}
		if d > maxDepth {
			maxDepth = d
		}
	}

	// Print in 10-depth buckets for readability
	fmt.Printf("  %-15s %-10s %-15s\n", "Depth Range", "Visits", "Avg Pending")
	for bucketStart := minDepth; bucketStart <= maxDepth; bucketStart += 10 {
		bucketEnd := bucketStart + 9
		if bucketEnd > maxDepth {
			bucketEnd = maxDepth
		}

		totalVisits := 0
		totalPending := 0
		pendingCount := 0
		for d := bucketStart; d <= bucketEnd; d++ {
			if v, ok := s.VisitsByDepth[d]; ok {
				totalVisits += v
			}
			if pending, ok := s.PendingCountByDepth[d]; ok {
				for _, p := range pending {
					totalPending += p
					pendingCount++
				}
			}
		}

		avgPending := 0.0
		if pendingCount > 0 {
			avgPending = float64(totalPending) / float64(pendingCount)
		}

		fmt.Printf("  %-15s %-10d %-15.2f\n", fmt.Sprintf("%d-%d", bucketStart, bucketEnd), totalVisits, avgPending)
	}

	fmt.Printf("\nMax queue depth: %d\n", s.MaxQueueDepth)
	if len(s.QueueDepthSamples) > 0 {
		fmt.Printf("Queue depth samples (every 100 visits): %v\n", s.QueueDepthSamples)
	}
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
