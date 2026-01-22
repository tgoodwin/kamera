package replay

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tgoodwin/kamera/pkg/event"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type recordingRecorder struct {
	opType event.OperationType
}

func (r *recordingRecorder) RecordEffect(_ context.Context, _ client.Object, opType event.OperationType, _ *PreconditionInfo) error {
	r.opType = opType
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
