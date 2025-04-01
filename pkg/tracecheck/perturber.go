package tracecheck

import (
	"github.com/samber/lo"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
)

type filterPred func(view *StateSnapshot) bool

type PriorityHandler interface {
	ApplyPriorities(views []*StateSnapshot) []*StateSnapshot
	PrioritizeViews(views []*StateSnapshot) []*StateSnapshot
}

type PerturbationManager struct {
	store                    *snapshot.Store
	prioritizationStrategies []prioritizer
	filterPreds              []filterPred
}

func NewPerturbationManager(store *snapshot.Store) *PerturbationManager {
	pm := &PerturbationManager{
		store:                    store,
		prioritizationStrategies: []prioritizer{},
	}

	// TODO remove
	pm.AddPrioritizationStrategy(cass188Prioritize)
	pm.AddFilterPred(func(view *StateSnapshot) bool {
		return view.priority != Skip
	})

	return pm
}

func (p *PerturbationManager) AddPrioritizationStrategy(strategy prioritizer) {
	p.prioritizationStrategies = append(p.prioritizationStrategies, strategy)
}

func (p *PerturbationManager) AddFilterPred(pred filterPred) {
	p.filterPreds = append(p.filterPreds, pred)
}

func (p *PerturbationManager) ApplyPriorities(views []*StateSnapshot) []*StateSnapshot {
	if p.prioritizationStrategies == nil {
		return views
	}
	// Apply each prioritization strategy to each view
	for _, strategy := range p.prioritizationStrategies {
		views = lo.Map(views, func(view *StateSnapshot, _ int) *StateSnapshot {
			// TODO strategies may conflict and we do not provide protection
			// against that
			return strategy(view)
		})
	}
	return views
}

func (p *PerturbationManager) PrioritizeViews(views []*StateSnapshot) []*StateSnapshot {
	if p.filterPreds == nil {
		return views
	}
	// Apply all prioritization strategies to the views
	// TODO generalize
	for _, filter := range p.filterPreds {
		views = lo.Filter(views, func(v *StateSnapshot, _ int) bool {
			return filter(v)
		})
	}
	return views
}
