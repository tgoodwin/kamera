package tracecheck

import (
	"fmt"
	"time"
)

type ExploreStats struct {
	startTime    *time.Time
	endTime      *time.Time
	AbortedPaths int
	NodeVisits   int
}

func NewExploreStats() *ExploreStats {
	return &ExploreStats{
		startTime:    nil,
		AbortedPaths: 0,
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
	fmt.Printf("Total time: %v\n", s.endTime.Sub(*s.startTime))
	fmt.Printf("Total node visits: %d\n", s.NodeVisits)
	fmt.Printf("Aborted paths: %d\n", s.AbortedPaths)
}
