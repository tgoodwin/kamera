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
	revertFunc := LabelChange(obj)

	// Check if the ChangeID label is set
	labels := obj.GetLabels()
	changeID, exists := labels[ChangeID]
	assert.True(t, exists, "ChangeID label should be set")
	_, err := uuid.Parse(changeID)
	assert.NoError(t, err, "ChangeID label should be a valid UUID")

	// Call the revert function
	revertFunc(obj)

	// Check if the labels are reverted to the original state
	labels = obj.GetLabels()
	assert.Equal(t, "existing-value", labels["existing-label"], "existing-label should be reverted to its original value")
	_, exists = labels[ChangeID]
	assert.False(t, exists, "ChangeID label should be removed after revert")
}
