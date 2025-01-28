package snapshot

import (
	"testing"

	"github.com/tgoodwin/sleeve/pkg/tag"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestJSONHasher_Hash(t *testing.T) {
	hasher := &JSONHasher{}
	obj := &unstructured.Unstructured{}
	obj.SetLabels(map[string]string{"key": "value"})

	hash := hasher.Hash(obj)
	if hash == "" {
		t.Errorf("expected non-empty hash, got empty")
	}
}

func TestAnonymizingHasher_Hash(t *testing.T) {
	labelReplacements := map[string]string{
		"key1": "replacement1",
		"key2": "replacement2",
	}
	hasher := NewAnonymizingHasher(labelReplacements)
	obj := &unstructured.Unstructured{}
	obj.SetLabels(map[string]string{"key1": "value1", "key2": "value2", "key3": "value3"})

	hash := hasher.Hash(obj)
	if hash == "" {
		t.Errorf("expected non-empty hash, got empty")
	}

	labels := obj.GetLabels()
	if labels["key1"] != "replacement1" {
		t.Errorf("expected key1 to be replaced with replacement1, got %s", labels["key1"])
	}
	if labels["key2"] != "replacement2" {
		t.Errorf("expected key2 to be replaced with replacement2, got %s", labels["key2"])
	}
	if labels["key3"] != "value3" {
		t.Errorf("expected key3 to remain value3, got %s", labels["key3"])
	}
}

func TestAnonymizingHasher_DefaultLabelReplacements(t *testing.T) {
	hasher := NewAnonymizingHasher(DefaultLabelReplacements)
	obj := &unstructured.Unstructured{}
	obj.SetLabels(map[string]string{
		tag.TraceyReconcileID: "some-reconcile-id",
		tag.ChangeID:          "some-change-id",
		tag.TraceyObjectID:    "some-object-id",
		tag.DeletionID:        "some-deletion-id",
		"other":               "value",
	})

	hash := hasher.Hash(obj)
	if hash == "" {
		t.Errorf("expected non-empty hash, got empty")
	}

	labels := obj.GetLabels()
	if labels[tag.TraceyReconcileID] != "RECONCILE_ID" {
		t.Errorf("expected TraceyReconcileID to be replaced with RECONCILE_ID, got %s", labels[tag.TraceyReconcileID])
	}
	if labels[tag.ChangeID] != "CHANGE_ID" {
		t.Errorf("expected ChangeID to be replaced with CHANGE_ID, got %s", labels[tag.ChangeID])
	}
	if labels[tag.TraceyObjectID] != "OBJECT_ID" {
		t.Errorf("expected TraceyObjectID to be replaced with OBJECT_ID, got %s", labels[tag.TraceyObjectID])
	}
	if labels[tag.DeletionID] != "DELETION_ID" {
		t.Errorf("expected DeletionID to be replaced with DELETION_ID, got %s", labels[tag.DeletionID])
	}
	if labels["other"] != "value" {
		t.Errorf("expected other to remain value, got %s", labels["other"])
	}
}
