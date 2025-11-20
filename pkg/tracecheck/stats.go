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
}

func NewExploreStats() *ExploreStats {
	return &ExploreStats{
		startTime:    nil,
		AbortedPaths: 0,

		RestartsPerReconciler: make(map[string]int),
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

func (s *ExploreStats) Print() {
	if s.endTime == nil {
		s.Finish()
	}
	if logger.GetSink() != nil {
		logger.Info("explore stats",
			"totalTime", s.endTime.Sub(*s.startTime),
			"totalNodeVisits", s.TotalNodeVisits,
			"uniqueNodeVisits", s.UniqueNodeVisits,
			"abortedPaths", s.AbortedPaths,
		)
		return
	}
	fmt.Printf("Total time: %v\n", s.endTime.Sub(*s.startTime))
	fmt.Printf("Total node visits: %d\n", s.TotalNodeVisits)
	fmt.Printf("Unique node visits: %d\n", s.UniqueNodeVisits)
	fmt.Printf("Aborted paths: %d\n", s.AbortedPaths)
}
