package event

import (
	"encoding/json"
	"reflect"
	"testing"
)

var mockEventJSON = `{
	"id": "event-id",
	"timestamp": "2021-08-02T15:04:05Z",
	"reconcile_id": "reconcile-id",
	"controller_id": "controller-id",
	"root_event_id": "root-event-id",
	"op_type": "GET",
	"kind": "Foo",
	"object_id": "foo-1",
	"version": "1",
	"label:reconcile-id": "reconcile-id",
	"label:controller-id": "controller-id",
	"label:root-event-id": "root-event-id",
	"label:change-id": "change-id"
}`

func TestUnmarshal(t *testing.T) {
	expected := &Event{
		ID:           "event-id",
		Timestamp:    "2021-08-02T15:04:05Z",
		ReconcileID:  "reconcile-id",
		ControllerID: "controller-id",
		RootEventID:  "root-event-id",
		OpType:       "GET",
		Kind:         "Foo",
		ObjectID:     "foo-1",
		Version:      "1",
		Labels: map[string]string{
			"reconcile-id":  "reconcile-id",
			"controller-id": "controller-id",
			"root-event-id": "root-event-id",
			"change-id":     "change-id",
		},
	}
	actual := &Event{}
	if err := actual.UnmarshalJSON([]byte(mockEventJSON)); err != nil {
		t.Fatalf("failed to unmarshal Event: %v", err)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("expected %v, got %v", expected, actual)
	}
}

func TestMarshal(t *testing.T) {
	expected := mockEventJSON
	actual := &Event{
		ID:           "event-id",
		Timestamp:    "2021-08-02T15:04:05Z",
		ReconcileID:  "reconcile-id",
		ControllerID: "controller-id",
		RootEventID:  "root-event-id",
		OpType:       "GET",
		Kind:         "Foo",
		ObjectID:     "foo-1",
		Version:      "1",
		Labels: map[string]string{
			"reconcile-id":  "reconcile-id",
			"controller-id": "controller-id",
			"root-event-id": "root-event-id",
			"change-id":     "change-id",
		},
	}
	actualJSON, err := actual.MarshalJSON()
	if err != nil {
		t.Fatalf("failed to marshal Event: %v", err)
	}
	// compare the JSON representation of the actual and expected events
	// without enforcing a specific order of keys
	var actualMap, expectedMap map[string]interface{}
	if err := json.Unmarshal(actualJSON, &actualMap); err != nil {
		t.Fatalf("failed to unmarshal actual JSON: %v", err)
	}
	if err := json.Unmarshal([]byte(expected), &expectedMap); err != nil {
		t.Fatalf("failed to unmarshal expected JSON: %v", err)
	}
	if !reflect.DeepEqual(actualMap, expectedMap) {
		t.Errorf("expected %v, got %v", expectedMap, actualMap)
	}
}
