package explore

import (
	"fmt"
	"strings"

	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
)

// ApplyInputTuning overlays declarative tuning from a coverage.InputTuning onto a
// base ExploreConfig. It is the canonical translation between the JSON-facing tuning
// format and the internal ExploreConfig/PerturbationConfig types.
//
// All example harnesses should call this function rather than implementing their own
// translation logic.
func ApplyInputTuning(base tracecheck.ExploreConfig, tuning coverage.InputTuning) (tracecheck.ExploreConfig, error) {
	cfg := base.Clone()

	if tuning.MaxDepth > 0 {
		cfg.MaxDepth = tuning.MaxDepth
	}

	if len(tuning.PermuteControllers) > 0 {
		if cfg.Perturbations.PermuteOrder == nil {
			cfg.Perturbations.PermuteOrder = make(map[tracecheck.ReconcilerID]bool)
		}
		for _, controllerID := range tuning.PermuteControllers {
			cfg.Perturbations.PermuteOrder[tracecheck.ReconcilerID(controllerID)] = true
		}
	}

	if tuning.PermuteDepthRange != nil {
		cfg.Perturbations.PermuteDepthRange = &tracecheck.PermuteDepthRange{
			Min: tuning.PermuteDepthRange.Min,
			Max: tuning.PermuteDepthRange.Max,
		}
	}

	if tuning.PermuteAfterEvent != nil {
		cfg.Perturbations.PermuteAfterEvent = &tracecheck.PermuteEventTrigger{
			OpType: event.OperationType(tuning.PermuteAfterEvent.OpType),
			Kind:   tuning.PermuteAfterEvent.Kind,
		}
	}

	if len(tuning.StaleReads) > 0 {
		if cfg.Perturbations.Staleness == nil {
			cfg.Perturbations.Staleness = make(map[tracecheck.ReconcilerID]tracecheck.StalenessConfig)
		}
		for controllerID, kinds := range tuning.StaleReads {
			id := tracecheck.ReconcilerID(controllerID)
			st := cfg.Perturbations.Staleness[id]
			if st.StaleReadBounds == nil {
				st.StaleReadBounds = make(tracecheck.LookbackLimits)
			}
			for _, kind := range kinds {
				trimmed := strings.TrimSpace(kind)
				if trimmed == "" {
					continue
				}
				lookback := tuning.StaleLookback[trimmed]
				if lookback <= 0 {
					lookback = 1
				}
				st.StaleReadBounds[trimmed] = tracecheck.LookbackLimit(lookback)
			}
			cfg.Perturbations.Staleness[id] = st
		}
	}

	if len(tuning.UserActionReadyDepths) > 0 {
		if cfg.Perturbations.UserActionReadyDepths == nil {
			cfg.Perturbations.UserActionReadyDepths = make(map[int]int)
		}
		for idxStr, depth := range tuning.UserActionReadyDepths {
			idx := 0
			fmt.Sscanf(idxStr, "%d", &idx)
			cfg.Perturbations.UserActionReadyDepths[idx] = depth
		}
	}

	if len(tuning.StalenessIntervals) > 0 {
		intervals := make([]tracecheck.StalenessInterval, 0, len(tuning.StalenessIntervals))
		for _, si := range tuning.StalenessIntervals {
			intervals = append(intervals, tracecheck.StalenessInterval{
				ReconcilerID:     tracecheck.ReconcilerID(si.Reconciler),
				Kind:             si.Kind,
				StaleAt:          si.StaleAt,
				CatchUpAt:        si.CatchUpAt,
				Lag:              si.Lag,
				FreezeAtSequence: si.FreezeAtSequence,
			})
		}
		cfg.Perturbations.StalenessIntervals = intervals
	}

	if len(tuning.FaultInjection) > 0 {
		faults := make([]tracecheck.FaultInjectionConfig, 0, len(tuning.FaultInjection))
		for _, fi := range tuning.FaultInjection {
			faults = append(faults, tracecheck.FaultInjectionConfig{
				ReconcilerID:      tracecheck.ReconcilerID(fi.Reconciler),
				CrashAfterEffect:  fi.CrashAfterEffect,
				RecoverAtDepth:    fi.RecoverAtDepth,
				TriggerOnce:       fi.TriggerOnce,
				TriggerAfterDepth: fi.TriggerAfterDepth,
			})
		}
		cfg.Perturbations.FaultInjection = faults
	}

	mode := strings.TrimSpace(tuning.Search.Mode)
	if mode != "" {
		parsed, err := tracecheck.ParseSearchMode(mode)
		if err != nil {
			return cfg, fmt.Errorf("parse tuning.search.mode: %w", err)
		}
		cfg.SearchMode = parsed
	}

	mc := tuning.Search.MonteCarlo
	if cfg.SearchMode != tracecheck.SearchModeMonteCarlo {
		if mc.Seed != nil || mc.Trials != nil || mc.TrialIndex != nil || mc.ScenarioGroup != nil {
			cfg.SearchMode = tracecheck.SearchModeMonteCarlo
		}
	}
	if cfg.SearchMode == tracecheck.SearchModeMonteCarlo {
		if mc.Seed != nil {
			cfg.MonteCarlo.Seed = *mc.Seed
		}
		if mc.Trials != nil {
			cfg.MonteCarlo.Trials = *mc.Trials
		}
		if mc.TrialIndex != nil {
			cfg.MonteCarlo.TrialIndex = *mc.TrialIndex
		}
		if mc.ScenarioGroup != nil {
			cfg.MonteCarlo.ScenarioGroup = *mc.ScenarioGroup
		}
	}

	return cfg, nil
}
