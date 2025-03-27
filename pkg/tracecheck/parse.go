package tracecheck

// Parse takes a string and returns a slice of strings, where each string is a
// line in the input string.

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"slices"

	"github.com/pkg/errors"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"github.com/tgoodwin/sleeve/pkg/tag"
)

// ParseJSONLFile reads a JSONL file and parses each record into either a snapshot.Record or an event.Event.
// the input is assumed to be the output of cmd/collect/main.go
func (b *ExplorerBuilder) ParseJSONLTrace(filePath string) ([]StateEvent, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	eventsByID := make(map[string]*event.Event)
	recordsByOperationID := make(map[string]*snapshot.Record)

	scanner := bufio.NewScanner(file)
	entriesParsed := 0
	for scanner.Scan() {
		line := scanner.Text()

		var record snapshot.Record
		if err := json.Unmarshal([]byte(line), &record); err == nil &&
			// heuristic for identifying JSON lines that fit the snapshot.Record schema
			record.OperationID != "" && len(record.Value) > 0 {
			// extract the object
			obj, err := record.ToUnstructured()
			if err != nil {
				fmt.Println("record.Value:", string(record.Value))
				return nil, errors.Wrap(err, "failed to unmarshal record to unstructured")
			}

			// do some validation on the object labels
			sleeveObjectID := tag.GetSleeveObjectID(obj)
			if sleeveObjectID == "" {
				panic(fmt.Sprintf("no sleeve object ID for kind: %s, namespace: %s, name: %s", obj.GetKind(), obj.GetNamespace(), obj.GetName()))
			}
			if err := b.snapStore.StoreObject(obj); err != nil {
				return nil, fmt.Errorf("failed to store object: %v", err)
			}

			recordsByOperationID[record.OperationID] = &record
			entriesParsed++
			continue
		}

		var evt event.Event
		if err := json.Unmarshal([]byte(line), &evt); err == nil && evt.ID != "" {
			eventsByID[evt.ID] = &evt
			entriesParsed++
			continue
		}

		fmt.Printf("Skipping invalid line: %s\n", line)
		return nil, fmt.Errorf("invalid line: %s", line)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	fmt.Println("# events:", len(eventsByID))
	fmt.Println("# records:", len(recordsByOperationID))
	fmt.Println("# entries parsed:", entriesParsed)

	stateEvents := make([]StateEvent, 0)

	for id, evt := range eventsByID {
		record, ok := recordsByOperationID[id]
		if !ok {
			// return nil, fmt.Errorf("event %s has no associated record", id)
			fmt.Printf("event %s has no associated record\n", id)
			continue
		}
		obj, err := record.ToUnstructured()
		if err != nil {
			return nil, fmt.Errorf("error unmarshaling JSON to unstructured: record operationID: %v, err: %w", record.OperationID, err)
		}
		ns := obj.GetNamespace()
		name := obj.GetName()
		sleeveObjectID := tag.GetSleeveObjectID(obj)
		stateEvent := StateEvent{
			Event:       evt,
			ReconcileID: evt.ReconcileID,
			Timestamp:   evt.Timestamp,
			Effect: newEffect(
				snapshot.NewCompositeKey(obj.GetKind(), ns, name, sleeveObjectID),
				snapshot.NewDefaultHash(string(record.Value)),
				event.OperationType(evt.OpType),
			),
		}
		stateEvents = append(stateEvents, stateEvent)
	}

	return stateEvents, nil
}

func assignResourceVersions(in []StateEvent) []StateEvent {
	stateEvents := slices.Clone(in)
	sort.Slice(stateEvents, func(i, j int) bool {
		return stateEvents[i].Timestamp < stateEvents[j].Timestamp
	})
	// Assign resource version sequence #s to each event
	// and start at 1 to not interact with zero values in downstream code
	var globalRV int64 = 1
	for i, t := range stateEvents {
		newEvent := t
		if event.IsWriteOp(t.Effect.OpType) {
			globalRV++
		}
		newEvent.Sequence = globalRV
		stateEvents[i] = newEvent
	}
	return stateEvents
}
