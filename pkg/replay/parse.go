package replay

import (
	"strings"

	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/tag"
)

func filterSleeveLines(lines []string) []string {
	return lo.FilterMap(lines, func(l string, _ int) (string, bool) {
		parts := strings.SplitN(l, tag.LoggerName, 2)
		isSleeveLine := strings.Contains(l, tag.LoggerName)
		if len(parts) > 1 {
			l = strings.TrimSpace(parts[1])
		}
		return l, isSleeveLine
	})
}

func ParseRecordsFromLines(lines []string) ([]snapshot.Record, error) {
	sleeveLines := filterSleeveLines(lines)
	versionLines := lo.FilterMap(sleeveLines, func(l string, _ int) (string, bool) {
		return tag.StripLogKey(l), strings.Contains(l, tag.ObjectVersionKey)
	})
	var loadErr error
	records := lo.Map(versionLines, func(v string, _ int) snapshot.Record {
		r, err := snapshot.LoadFromString(v)
		if err != nil {
			loadErr = errors.Wrap(err, "failed to load record from string")
		}
		return r
	})
	if loadErr != nil {
		return nil, loadErr
	}
	return records, nil
}

func ParseEventsFromLines(lines []string) ([]event.Event, error) {
	sleeveLines := filterSleeveLines(lines)
	eventLines := lo.FilterMap(sleeveLines, func(l string, _ int) (string, bool) {
		return tag.StripLogKey(l), strings.Contains(l, tag.ControllerOperationKey)
	})
	var loadErr error
	events := lo.Map(eventLines, func(l string, _ int) event.Event {
		var e event.Event
		if err := e.UnmarshalJSON([]byte(l)); err != nil {
			loadErr = errors.Wrap(err, "failed to unmarshal event from json")
		}
		return e
	})
	if loadErr != nil {
		return nil, loadErr
	}
	return events, nil
}
