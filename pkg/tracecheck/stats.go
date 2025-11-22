package tracecheck

import (
	"fmt"
	"time"
)

type ExploreStats struct {
	startTime        *time.Time
	endTime          *time.Time
	AbortedPaths     int
	UniqueNodeVisits int
	TotalNodeVisits  int

	RestartsPerReconciler map[string]int

	// reconcile step timing
	TotalReconcileSteps int
	TotalStepLatency    time.Duration
	MaxStepLatency      time.Duration
	StepLatencyByRecon  map[string]*latencyStat

	// TODO remove before merging: temporary state rehydration instrumentation
	RehydrationSamples  int
	TotalRehydratedObjs int
	MaxRehydratedObjs   int
	MinRehydratedObjs   int
	RehydrationsByRecon map[string]*rehydrationStat
}

type latencyStat struct {
	Count int
	Total time.Duration
	Max   time.Duration
	Min   time.Duration
}

type rehydrationStat struct {
	Count int
	Total int
	Max   int
	Min   int
}

func NewExploreStats() *ExploreStats {
	return &ExploreStats{
		startTime:    nil,
		AbortedPaths: 0,

		RestartsPerReconciler: make(map[string]int),
		StepLatencyByRecon:    make(map[string]*latencyStat),
		RehydrationsByRecon:   make(map[string]*rehydrationStat),
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

func (s *ExploreStats) RecordStep(reconcilerID string, d time.Duration) {
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

// TODO remove before merging: temporary instrumentation for rehydration volume
func (s *ExploreStats) RecordRehydration(reconcilerID string, objs int) {
	s.RehydrationSamples++
	s.TotalRehydratedObjs += objs
	if s.RehydrationSamples == 1 || objs > s.MaxRehydratedObjs {
		s.MaxRehydratedObjs = objs
	}
	if s.RehydrationSamples == 1 || objs < s.MinRehydratedObjs {
		s.MinRehydratedObjs = objs
	}

	rs, ok := s.RehydrationsByRecon[reconcilerID]
	if !ok {
		rs = &rehydrationStat{Min: objs, Max: objs}
		s.RehydrationsByRecon[reconcilerID] = rs
	}
	rs.Count++
	rs.Total += objs
	if objs > rs.Max {
		rs.Max = objs
	}
	if objs < rs.Min {
		rs.Min = objs
	}
}

func (ls *latencyStat) Avg() time.Duration {
	if ls == nil || ls.Count == 0 {
		return 0
	}
	return time.Duration(int64(ls.Total) / int64(ls.Count))
}

func (rs *rehydrationStat) Avg() float64 {
	if rs == nil || rs.Count == 0 {
		return 0
	}
	return float64(rs.Total) / float64(rs.Count)
}

func (s *ExploreStats) Print() {
	if s.endTime == nil {
		s.Finish()
	}
	var avgStep time.Duration
	if s.TotalReconcileSteps > 0 {
		avgStep = time.Duration(int64(s.TotalStepLatency) / int64(s.TotalReconcileSteps))
	}
	var avgRehydrated float64
	if s.RehydrationSamples > 0 {
		avgRehydrated = float64(s.TotalRehydratedObjs) / float64(s.RehydrationSamples)
	}
	if logger.GetSink() != nil {
		logger.Info("explore stats",
			"totalTime", s.endTime.Sub(*s.startTime),
			"totalNodeVisits", s.TotalNodeVisits,
			"uniqueNodeVisits", s.UniqueNodeVisits,
			"abortedPaths", s.AbortedPaths,
			"reconcileSteps", s.TotalReconcileSteps,
			"avgStepLatency", avgStep,
			"maxStepLatency", s.MaxStepLatency,
			"stepLatencyByReconciler", s.latencyByReconcilerSummary(),
			"avgRehydratedObjs", avgRehydrated,
			"maxRehydratedObjs", s.MaxRehydratedObjs,
			"minRehydratedObjs", s.MinRehydratedObjs,
			"rehydrationByReconciler", s.rehydrationByReconcilerSummary(),
		)
		// return
	}
	fmt.Printf("Total time: %v\n", s.endTime.Sub(*s.startTime))
	fmt.Printf("Total node visits: %d\n", s.TotalNodeVisits)
	fmt.Printf("Unique node visits: %d\n", s.UniqueNodeVisits)
	fmt.Printf("Aborted paths: %d\n", s.AbortedPaths)
	fmt.Printf("Reconcile steps: %d\n", s.TotalReconcileSteps)
	fmt.Printf("Avg step latency: %v\n", avgStep)
	fmt.Printf("Max step latency: %v\n", s.MaxStepLatency)
	fmt.Printf("Avg rehydrated objs: %.2f\n", avgRehydrated)
	fmt.Printf("Max rehydrated objs: %d\n", s.MaxRehydratedObjs)
	fmt.Printf("Min rehydrated objs: %d\n", s.MinRehydratedObjs)
	fmt.Println("Step latency by reconciler:")
	s.printLatencyTable()
	fmt.Println("Rehydrated objects by reconciler:")
	s.printRehydrationTable()
}

func (s *ExploreStats) latencyByReconcilerSummary() map[string]map[string]any {
	out := make(map[string]map[string]any, len(s.StepLatencyByRecon))
	for reconID, ls := range s.StepLatencyByRecon {
		if ls == nil || ls.Count == 0 {
			continue
		}
		out[reconID] = map[string]any{
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

// TODO remove before merging: temporary rehydration instrumentation summary
func (s *ExploreStats) rehydrationByReconcilerSummary() map[string]map[string]any {
	out := make(map[string]map[string]any, len(s.RehydrationsByRecon))
	for reconID, rs := range s.RehydrationsByRecon {
		if rs == nil || rs.Count == 0 {
			continue
		}
		out[reconID] = map[string]any{
			"count": rs.Count,
			"avg":   rs.Avg(),
			"max":   rs.Max,
			"min":   rs.Min,
		}
	}
	return out
}

// TODO remove before merging: temporary rehydration instrumentation output
func (s *ExploreStats) printRehydrationTable() {
	if len(s.RehydrationsByRecon) == 0 {
		fmt.Println("  (no rehydration samples)")
		return
	}
	fmt.Printf("  %-25s %-6s %-12s %-12s %-12s\n", "Reconciler", "Count", "Avg", "Max", "Min")
	for reconID, rs := range s.RehydrationsByRecon {
		if rs == nil || rs.Count == 0 {
			continue
		}
		fmt.Printf("  %-25s %-6d %-12.2f %-12d %-12d\n", reconID, rs.Count, rs.Avg(), rs.Max, rs.Min)
	}
}
