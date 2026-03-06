package replay

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/tag"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type recordingRecorder struct {
	opType event.OperationType
	obj    client.Object
}

func (r *recordingRecorder) RecordEffect(_ context.Context, obj client.Object, opType event.OperationType, _ *PreconditionInfo) error {
	r.opType = opType
	r.obj = obj.DeepCopyObject().(client.Object)
	return nil
}

type noopFrameReader struct{}

func (noopFrameReader) GetCacheFrame(_ string) (CacheFrame, error) {
	return CacheFrame{}, nil
}

func TestClientPatchApplyUsesApplyOp(t *testing.T) {
	recorder := &recordingRecorder{}
	c := NewClient("test", nil, noopFrameReader{}, recorder)

	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion("v1")
	obj.SetKind("ConfigMap")
	obj.SetNamespace("default")
	obj.SetName("example")

	err := c.Patch(context.Background(), obj, client.Apply)
	assert.NoError(t, err)
	assert.Equal(t, event.APPLY, recorder.opType)
}

func TestClientStatusPatchApplyUsesApplyOp(t *testing.T) {
	recorder := &recordingRecorder{}
	c := NewClient("test", nil, noopFrameReader{}, recorder)

	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion("v1")
	obj.SetKind("ConfigMap")
	obj.SetNamespace("default")
	obj.SetName("example")

	err := c.Status().Patch(context.Background(), obj, client.Apply)
	assert.NoError(t, err)
	assert.Equal(t, event.APPLY, recorder.opType)
}

func TestClientCreateAssignsDeterministicUIDWhenMissing(t *testing.T) {
	recorder := &recordingRecorder{}
	c := NewClient("test", nil, noopFrameReader{}, recorder)

	obj := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ConfigMap",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "example",
		},
	}

	err := c.Create(context.Background(), obj)
	require.NoError(t, err)
	require.NotNil(t, recorder.obj)

	expectedUID := types.UID(tag.GetSleeveObjectID(obj))
	assert.NotEmpty(t, obj.GetUID())
	assert.Equal(t, expectedUID, obj.GetUID())
	assert.Equal(t, expectedUID, recorder.obj.GetUID())
}
