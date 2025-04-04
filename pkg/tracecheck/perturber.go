package tracecheck

import (
	"github.com/samber/lo"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
)

type PriorityStrategyBuilder struct {
	filters         []filterPred
	prioritizations []prioritizer
}

func NewPriorityBuilder() *PriorityStrategyBuilder {
	return &PriorityStrategyBuilder{
		filters:         []filterPred{},
		prioritizations: []prioritizer{},
	}
}

func (pb *PriorityStrategyBuilder) AddFilterPred(pred filterPred) {
	pb.filters = append(pb.filters, pred)
}

func (pb *PriorityStrategyBuilder) AddStrategy(prioritizer prioritizer) {
	pb.prioritizations = append(pb.prioritizations, prioritizer)
}

func (pb *PriorityStrategyBuilder) Build(store *snapshot.Store) *PriorityManager {
	pm := &PriorityManager{
		prioritizationStrategies: pb.prioritizations,
		filterPreds:              pb.filters,
	}
	return pm
}

type filterPred func(view *StateSnapshot) bool

type PriorityHandler interface {
	ApplyPriorities(views []*StateSnapshot) []*StateSnapshot
	PrioritizeViews(views []*StateSnapshot) []*StateSnapshot
}

type PriorityManager struct {
	store                    *snapshot.Store
	prioritizationStrategies []prioritizer
	filterPreds              []filterPred
}

func (p *PriorityManager) ApplyPriorities(views []*StateSnapshot) []*StateSnapshot {
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

func (p *PriorityManager) PrioritizeViews(views []*StateSnapshot) []*StateSnapshot {
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
