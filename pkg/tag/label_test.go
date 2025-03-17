package tag

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestAddSleeveObjectID(t *testing.T) {
	// Test when the label does not exist
	obj := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{},
		},
	}

	AddSleeveObjectID(obj)

	// Check if the TraceyObjectID label is set
	labels := obj.GetLabels()
	objectID, exists := labels[TraceyObjectID]
	assert.True(t, exists, "TraceyObjectID label should be set")
	_, err := uuid.Parse(objectID)
	assert.NoError(t, err, "TraceyObjectID label should be a valid UUID")

	// Test idempotency: label should not be overwritten if it already exists
	existingUUID := uuid.New().String()
	obj.SetLabels(map[string]string{
		TraceyObjectID: existingUUID,
	})

	AddSleeveObjectID(obj)

	// Check if the TraceyObjectID label is not overwritten
	labels = obj.GetLabels()
	objectID, exists = labels[TraceyObjectID]
	assert.True(t, exists, "TraceyObjectID label should still be set")
	assert.Equal(t, existingUUID, objectID, "TraceyObjectID label should not be overwritten")
}

func TestAddDeletionID(t *testing.T) {
	// Test when the label does not exist
	obj := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{},
		},
	}

	AddDeletionID(obj)

	// Check if the DeletionID label is set
	labels := obj.GetLabels()
	deletionID, exists := labels[DeletionID]
	assert.True(t, exists, "DeletionID label should be set")
	_, err := uuid.Parse(deletionID)
	assert.NoError(t, err, "DeletionID label should be a valid UUID")

	// Test idempotency: label should not be overwritten if it already exists
	existingUUID := uuid.New().String()
	obj.SetLabels(map[string]string{
		DeletionID: existingUUID,
	})

	AddDeletionID(obj)

	// Check if the DeletionID label is not overwritten
	labels = obj.GetLabels()
	deletionID, exists = labels[DeletionID]
	assert.True(t, exists, "DeletionID label should still be set")
	assert.Equal(t, existingUUID, deletionID, "DeletionID label should not be overwritten")
}

func TestLabelChange(t *testing.T) {
	// Create a fake object
	obj := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				"existing-label": "existing-value",
			},
		},
	}

	// Call LabelChange and get the revert function
	LabelChange(obj)

	// Check if the ChangeID label is set
	labels := obj.GetLabels()
	changeID, exists := labels[ChangeID]
	assert.True(t, exists, "ChangeID label should be set")
	_, err := uuid.Parse(changeID)
	assert.NoError(t, err, "ChangeID label should be a valid UUID")
}

func TestGetSleeveLabels(t *testing.T) {
	// Create a fake object with some labels
	obj := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				"discrete.events/label1": "value1",
				"discrete.events/label2": "value2",
				"other.label":            "othervalue",

				// todo use the same prefix here
				"tracey-uid": "tracey-uid",
			},
		},
	}

	// Call GetSleeveLabels
	sleeveLabels := GetSleeveLabels(obj)

	// Verify that only labels with the "discrete.events" prefix are returned
	assert.Len(t, sleeveLabels, 3, "Only labels with the 'discrete.events' prefix should be returned")
	assert.Equal(t, "value1", sleeveLabels["discrete.events/label1"], "Label value mismatch for 'discrete.events/label1'")
	assert.Equal(t, "value2", sleeveLabels["discrete.events/label2"], "Label value mismatch for 'discrete.events/label2'")
	assert.Equal(t, "tracey-uid", sleeveLabels["tracey-uid"], "Label value mismatch for 'tracey-uid'")
	assert.NotContains(t, sleeveLabels, "other.label", "'other.label' should not be included in the result")
}
