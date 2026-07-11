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
	applycorev1 "k8s.io/client-go/applyconfigurations/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type recordingRecorder struct {
	opType        event.OperationType
	obj           client.Object
	options       *EffectOptions
	preconditions *PreconditionInfo
}

func (r *recordingRecorder) RecordEffect(_ context.Context, obj client.Object, opType event.OperationType, preconditions *PreconditionInfo, options *EffectOptions) error {
	r.opType = opType
	r.obj = obj.DeepCopyObject().(client.Object)
	r.options = options
	r.preconditions = preconditions
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
	obj.Object["data"] = map[string]interface{}{"key": "value"}

	err := c.Patch(context.Background(), obj, client.Apply, client.FieldOwner("test-manager"), client.ForceOwnership)
	assert.NoError(t, err)
	assert.Equal(t, event.APPLY, recorder.opType)
	require.Equal(t, map[string]interface{}{"key": "value"}, recorder.obj.(*unstructured.Unstructured).Object["data"])
	require.NotNil(t, recorder.preconditions)
	assert.Equal(t, "test-manager", recorder.preconditions.FieldManager)
	assert.True(t, recorder.preconditions.Force)
	require.NotNil(t, recorder.options)
	assert.Equal(t, types.ApplyPatchType, recorder.options.PatchType)
}

func TestClientTypedApplyPreservesCompleteConfiguration(t *testing.T) {
	recorder := &recordingRecorder{}
	c := NewClient("test", nil, noopFrameReader{}, recorder)
	configuration := applycorev1.ConfigMap("example", "default").WithData(map[string]string{"key": "value"})

	err := c.Apply(context.Background(), configuration, client.FieldOwner("typed-manager"))
	require.NoError(t, err)
	require.Equal(t, event.APPLY, recorder.opType)
	applyObj := recorder.obj.(*unstructured.Unstructured)
	require.Equal(t, "value", applyObj.Object["data"].(map[string]interface{})["key"])
	require.NotNil(t, recorder.preconditions)
	require.Equal(t, "typed-manager", recorder.preconditions.FieldManager)
}

func TestClientRawYAMLApplyPreservesPatchBody(t *testing.T) {
	recorder := &recordingRecorder{}
	c := NewClient("test", nil, noopFrameReader{}, recorder)
	target := &unstructured.Unstructured{}
	target.SetAPIVersion("v1")
	target.SetKind("ConfigMap")
	target.SetNamespace("default")
	target.SetName("example")
	patch := client.RawPatch(types.ApplyPatchType, []byte(`
apiVersion: v1
kind: ConfigMap
metadata:
  name: example
  namespace: default
data:
  key: value
`))

	err := c.Patch(context.Background(), target, patch, client.FieldOwner("raw-manager"))
	require.NoError(t, err)
	applyObj := recorder.obj.(*unstructured.Unstructured)
	require.Equal(t, "value", applyObj.Object["data"].(map[string]interface{})["key"])
	require.Equal(t, "raw-manager", recorder.preconditions.FieldManager)
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
	require.NotNil(t, recorder.options)
	assert.Equal(t, "status", recorder.options.Subresource)
}

func TestClientStatusUpdateUsesStatusSubresource(t *testing.T) {
	recorder := &recordingRecorder{}
	c := NewClient("test", nil, noopFrameReader{}, recorder)

	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion("v1")
	obj.SetKind("ConfigMap")
	obj.SetNamespace("default")
	obj.SetName("example")

	err := c.Status().Update(context.Background(), obj)
	assert.NoError(t, err)
	assert.Equal(t, event.UPDATE, recorder.opType)
	require.NotNil(t, recorder.options)
	assert.Equal(t, "status", recorder.options.Subresource)
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
