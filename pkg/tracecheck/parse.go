package tracecheck

// Parse takes a string and returns a slice of strings, where each string is a
// line in the input string.

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"

	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
)

// ParseJSONLFile reads a JSONL file and parses each record into either a snapshot.Record or an event.Event.
// the input is assumed to be the output of cmd/collect/main.go
func ParseJSONLTrace(filePath string) ([]StateEvent, error) {
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
			record.OperationID != "" && record.Value != "" {
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
	sequenesByKind := make(map[string]int64)
	for id, evt := range eventsByID {
		record, ok := recordsByOperationID[id]
		if !ok {
			// return nil, fmt.Errorf("event %s has no associated record", id)
			fmt.Printf("event %s has no associated record\n", id)
			continue
		}
		if event.IsWriteOp(event.OperationType(evt.OpType)) {
			sequenesByKind[evt.Kind]++
		}
		evtSequenceNum := sequenesByKind[evt.Kind]
		obj := record.ToUnstructured()
		ns := obj.GetNamespace()
		name := obj.GetName()
		stateEvent := StateEvent{
			Event:       evt,
			ReconcileID: evt.ReconcileID,
			Timestamp:   evt.Timestamp,
			Effect: newEffect(
				snapshot.NewCompositeKey(obj.GetKind(), ns, name, record.ObjectID),
				snapshot.NewDefaultHash(record.Value),
				event.OperationType(evt.OpType),
			),
			Sequence: evtSequenceNum,
		}
		stateEvents = append(stateEvents, stateEvent)
	}

	return stateEvents, nil
}
