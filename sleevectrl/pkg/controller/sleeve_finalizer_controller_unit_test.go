package controller

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func makeDeletionCandidate(finalizers []string, withDeletionTimestamp bool) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion("v1")
	obj.SetKind("Pod")
	obj.SetNamespace("default")
	obj.SetName("pod-1")
	obj.SetFinalizers(finalizers)
	if withDeletionTimestamp {
		ts := metav1.NewTime(time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC))
		obj.SetDeletionTimestamp(&ts)
	}
	return obj
}

func TestCleanupEligibleForRemovalRequiresDeletionTimestamp(t *testing.T) {
	obj := makeDeletionCandidate(nil, false)
	assert.False(t, cleanupEligibleForRemoval(obj))
}

func TestCleanupEligibleForRemovalRequiresNoFinalizers(t *testing.T) {
	obj := makeDeletionCandidate([]string{"example.com/finalizer"}, true)
	assert.False(t, cleanupEligibleForRemoval(obj))
}

func TestCleanupEligibleForRemovalTrueWhenDeleteTimestampAndNoFinalizers(t *testing.T) {
	obj := makeDeletionCandidate(nil, true)
	assert.True(t, cleanupEligibleForRemoval(obj))
}

func TestSetObjectTypeMetaFromCanonicalKind_CustomGroup(t *testing.T) {
	obj := &unstructured.Unstructured{}

	err := setObjectTypeMetaFromCanonicalKind(obj, "example.promise.syntasso.io/EasyApp")
	assert.NoError(t, err)
	assert.Equal(t, "EasyApp", obj.GetKind())
	assert.Equal(t, "example.promise.syntasso.io/v1", obj.GetAPIVersion())
}

func TestSetObjectTypeMetaFromCanonicalKind_CoreGroup(t *testing.T) {
	obj := &unstructured.Unstructured{}

	err := setObjectTypeMetaFromCanonicalKind(obj, "core/Pod")
	assert.NoError(t, err)
	assert.Equal(t, "Pod", obj.GetKind())
	assert.Equal(t, "v1", obj.GetAPIVersion())
}
