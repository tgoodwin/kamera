package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/util"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Event struct {
	ID           string            `json:"id"`
	Timestamp    string            `json:"timestamp"`
	ReconcileID  string            `json:"reconcile_id"`
	ControllerID string            `json:"controller_id"`
	RootEventID  string            `json:"root_event_id"`
	OpType       string            `json:"op_type"`
	Kind         string            `json:"kind"`
	ObjectID     string            `json:"object_id"`
	Version      string            `json:"version"`
	Labels       map[string]string `json:"labels,omitempty"`
}

// Ensure Event implements the json.Marshaler and json.Unmarshaler interfaces
var _ json.Marshaler = (*Event)(nil)
var _ json.Unmarshaler = (*Event)(nil)

func NewOperation(obj client.Object, reconcileID, controllerID, rootEventID string, op OperationType) (*Event, error) {
	id := uuid.New().String()
	e := &Event{
		ID:           id,
		Timestamp:    FormatTimeStr(time.Now()),
		ReconcileID:  reconcileID,
		ControllerID: controllerID,
		RootEventID:  rootEventID,
		OpType:       string(op),
		Kind:         util.GetKind(obj),

		// TODO CHANGE TO SLEEVE OBJECT ID
		ObjectID: string(obj.GetUID()),
		Version:  obj.GetResourceVersion(),
		Labels:   tag.GetSleeveLabels(obj),
	}
	// post construction validation
	if _, err := e.GetChangeID(); err != nil {
		return nil, fmt.Errorf("failed to get change ID: %w", err)
	}
	return e, nil
}

func (e *Event) CausalKey() CausalKey {
	return CausalKey{
		Kind:     e.Kind,
		ObjectID: e.ObjectID,
		ChangeID: e.MustGetChangeID(),
	}
}

func (e *Event) GetChangeID() (ChangeID, error) {
	if e.OpType == string(MARK_FOR_DELETION) {
		if deleteID, ok := e.Labels[tag.DeletionID]; ok {
			return ChangeID(deleteID), nil
		}
		return ChangeID(""), errors.New("DELETE event does not have a deletion ID")
	}
	if changeID, ok := e.Labels[tag.ChangeID]; ok {
		return ChangeID(changeID), nil
	}

	// when there has not been a change yet, only reads
	if rootID, ok := e.Labels[tag.TraceyRootID]; ok {
		return ChangeID(rootID), nil
	}
	// case where its a top-level GET event from a declared resource that has only
	// been tagged by the webhook with a tracey-uid and has not been processed by a sleeve reconciler
	if rootID, ok := e.Labels[tag.TraceyWebhookLabel]; ok {
		return ChangeID(rootID), nil
	}
	return ChangeID(""), errors.New("Event does not have a change ID")
}

func (e *Event) MustGetChangeID() ChangeID {
	if e == nil {
		panic("event is nil")
	}
	changeID, err := e.GetChangeID()
	if err != nil {
		fmt.Printf("event with no change ID: %+v\n", e)
		if os.Getenv("STRICT") == "true" {
			panic(err)
		}
	}
	return changeID
}

func (e *Event) UnmarshalJSON(data []byte) error {
	type Alias Event
	aux := &struct {
		*Alias
		Labels map[string]string `json:"-"`
	}{
		Alias:  (*Alias)(e),
		Labels: make(map[string]string),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return fmt.Errorf("failed to unmarshal Event: %w", err)
	}

	// Populate the Labels map with keys that have the prefix "label:"
	var rawMap map[string]interface{}
	if err := json.Unmarshal(data, &rawMap); err != nil {
		return fmt.Errorf("failed to unmarshal raw data: %w", err)
	}

	for key, value := range rawMap {
		if len(key) > 6 && key[:6] == "label:" {
			if strValue, ok := value.(string); ok {
				aux.Labels[key[6:]] = strValue
			}
		}
	}

	e.Labels = aux.Labels
	return nil
}

func (e Event) MarshalJSON() ([]byte, error) {
	type Alias Event
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(&e),
	}

	// Marshal the alias struct to JSON
	auxData, err := json.Marshal(aux)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal Event: %w", err)
	}
	// Unmarshal the JSON back into a map
	var dataMap map[string]interface{}
	if err := json.Unmarshal(auxData, &dataMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal aux data: %w", err)
	}

	// Add the labels with the "label:" prefix in a sorted order
	labelKeys := make([]string, 0, len(e.Labels))
	for key := range e.Labels {
		labelKeys = append(labelKeys, key)
	}
	sort.Strings(labelKeys)

	for _, key := range labelKeys {
		dataMap["label:"+key] = e.Labels[key]
	}
	delete(dataMap, "labels")

	// Marshal the final map to JSON
	return json.Marshal(dataMap)
}

func (e Event) VersionKey() snapshot.VersionKey {
	return snapshot.VersionKey{
		Kind:     e.Kind,
		ObjectID: e.ObjectID,
		Version:  string(e.MustGetChangeID()),
	}
}

func Earliest(events []Event) Event {
	earliest := events[0]
	for _, e := range events {
		if e.Timestamp < earliest.Timestamp {
			earliest = e
		}
	}
	return earliest
}
