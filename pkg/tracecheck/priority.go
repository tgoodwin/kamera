package tracecheck

import (
	"github.com/samber/lo"
)

type Priority int

const (
	Default Priority = 0
	Skip    Priority = -1
	High    Priority = 100
)

type prioritizer func(view *StateSnapshot) *StateSnapshot

func prioritizeViews(views []*StateSnapshot, prioritize prioritizer) []*StateSnapshot {
	return lo.Map(views, func(view *StateSnapshot, _ int) *StateSnapshot {
		return prioritize(view)
	})
}
