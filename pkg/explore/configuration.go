package explore

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/tgoodwin/kamera/pkg/tracecheck"
)

// fileOptimizationConfig models JSON-serializable optimization toggles.
type fileOptimizationConfig struct {
	EarlyConvergence        *bool `json:"earlyConvergence,omitempty"`
	CompletedPathDedup      *bool `json:"completedPathDedup,omitempty"`
	OrderingPruning         *bool `json:"orderingPruning,omitempty"`
	DisableNoOpOrderingSkip *bool `json:"disableNoOpOrderingSkip,omitempty"`
	CachePrediction         *bool `json:"cachePrediction,omitempty"`
	SubtreeCompletion       *bool `json:"subtreeCompletion,omitempty"`
	OnlyPermuteTriggered    *bool `json:"onlyPermuteTriggered,omitempty"`
}

// fileExploreConfig is the JSON-friendly representation for ExploreConfig.
type fileExploreConfig struct {
	MaxDepth        *int                   `json:"maxDepth,omitempty"`
	RecordPerfStats *bool                  `json:"recordPerfStats,omitempty"`
	Timeout         string                 `json:"timeout,omitempty"`
	Optimizations   fileOptimizationConfig `json:"optimizations,omitempty"`
}

// apply overlays the file config onto the provided base ExploreConfig.
func (f fileExploreConfig) apply(base tracecheck.ExploreConfig) (tracecheck.ExploreConfig, error) {
	cfg := base.Clone()

	if f.MaxDepth != nil {
		cfg.MaxDepth = *f.MaxDepth
	}
	if f.RecordPerfStats != nil {
		cfg.RecordPerfStats = *f.RecordPerfStats
	}
	if f.Timeout != "" {
		dur, err := time.ParseDuration(f.Timeout)
		if err != nil {
			return cfg, fmt.Errorf("parse timeout duration: %w", err)
		}
		cfg.Timeout = dur
	}
	cfg.Optimizations = f.applyOptimizations(cfg.Optimizations)

	return cfg, nil
}

func (f fileExploreConfig) applyOptimizations(base tracecheck.OptimizationConfig) tracecheck.OptimizationConfig {
	opt := base
	if f.Optimizations.EarlyConvergence != nil {
		opt.EarlyConvergence = *f.Optimizations.EarlyConvergence
	}
	if f.Optimizations.CompletedPathDedup != nil {
		opt.CompletedPathDedup = *f.Optimizations.CompletedPathDedup
	}
	if f.Optimizations.OrderingPruning != nil {
		opt.OrderingPruning = *f.Optimizations.OrderingPruning
	}
	if f.Optimizations.DisableNoOpOrderingSkip != nil {
		opt.DisableNoOpOrderingSkip = *f.Optimizations.DisableNoOpOrderingSkip
	}
	if f.Optimizations.CachePrediction != nil {
		opt.CachePrediction = *f.Optimizations.CachePrediction
	}
	if f.Optimizations.SubtreeCompletion != nil {
		opt.SubtreeCompletion = *f.Optimizations.SubtreeCompletion
	}
	if f.Optimizations.OnlyPermuteTriggered != nil {
		opt.OnlyPermuteTriggered = *f.Optimizations.OnlyPermuteTriggered
	}
	return opt
}

// LoadExploreConfigFromFile loads a JSON config file and overlays it onto the provided base config.
func LoadExploreConfigFromFile(path string, base tracecheck.ExploreConfig) (tracecheck.ExploreConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return base, fmt.Errorf("read config file: %w", err)
	}

	var fc fileExploreConfig
	if err := json.Unmarshal(data, &fc); err != nil {
		return base, fmt.Errorf("unmarshal config: %w", err)
	}

	return fc.apply(base)
}
