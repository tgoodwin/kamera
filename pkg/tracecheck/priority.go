package tracecheck

import (
	"fmt"

	"github.com/samber/lo"
	"github.com/tgoodwin/sleeve/pkg/util"
)

type Priority int

const (
	Default Priority = 0
	Skip    Priority = -1
	High    Priority = 100
)

type prioritizer func(view *StateSnapshot) *StateSnapshot

func cass188Prioritize(view *StateSnapshot) *StateSnapshot {
	// This is a placeholder for the actual prioritization logic.
	// Replace with the actual logic as per your requirements.
	fmt.Println("WARNING there is hard-coded filtering here - see cass188Prioritize")
	view.priority = Skip
	for _, v := range view.Observable() {
		if util.ShortenHash(v.Value) == "2hquvmr5" {
			view.priority = High
		}
	}
	return view
}

func prioritizeViews(views []*StateSnapshot, prioritize prioritizer) []*StateSnapshot {
	return lo.Map(views, func(view *StateSnapshot, _ int) *StateSnapshot {
		return prioritize(view)
	})
}
