package tag

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

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
func TestAddDeletionID(t *testing.T) {
	// Create a fake object with an existing DeletionID label
	existingUUID := uuid.New().String()
	obj := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				DeletionID: existingUUID,
			},
		},
	}

	// Call AddDeletionID
	AddDeletionID(obj)

	// Check if the DeletionID label is not overwritten
	labels := obj.GetLabels()
	deletionID, exists := labels[DeletionID]
	assert.True(t, exists, "DeletionID label should be set")
	assert.Equal(t, existingUUID, deletionID, "DeletionID label should not be overwritten")
}
