package explore

import (
	"time"

	"github.com/tgoodwin/kamera/pkg/analysis"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
)

func campaignMetricsForDump(stats *tracecheck.ExploreStats, duration time.Duration) *analysis.CampaignMetrics {
	if stats == nil {
		if duration <= 0 {
			return nil
		}
		return &analysis.CampaignMetrics{
			DurationNS: duration.Nanoseconds(),
		}
	}

	metrics := &analysis.CampaignMetrics{
		UniqueNodeVisits:     stats.UniqueNodeVisits,
		TotalNodeVisits:      stats.TotalNodeVisits,
		UniqueResourceStates: stats.UniqueResourceStates,
	}
	if duration > 0 {
		metrics.DurationNS = duration.Nanoseconds()
	}
	return metrics
}

