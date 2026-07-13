package replay

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/util"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type recordingRecorder struct {
	opType  event.OperationType
	obj     client.Object
	options *EffectOptions
}

func (r *recordingRecorder) RecordEffect(_ context.Context, obj client.Object, opType event.OperationType, _ *PreconditionInfo, options *EffectOptions) error {
	r.opType = opType
	r.obj = obj.DeepCopyObject().(client.Object)
	r.options = options
	return nil
}

type noopFrameReader struct{}

func (noopFrameReader) GetCacheFrame(_ string) (CacheFrame, error) {
	return CacheFrame{}, nil
}

type staticFrameReader struct {
	frame CacheFrame
}

func (r staticFrameReader) GetCacheFrame(_ string) (CacheFrame, error) {
	return r.frame, nil
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

func TestClientListReturnsNamespaceNameOrder(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	objects := map[types.NamespacedName]*unstructured.Unstructured{}
	for _, key := range []types.NamespacedName{
		{Namespace: "z", Name: "first"},
		{Namespace: "a", Name: "second"},
		{Namespace: "a", Name: "first"},
	} {
		pod := &unstructured.Unstructured{}
		pod.SetAPIVersion("v1")
		pod.SetKind("Pod")
		pod.SetNamespace(key.Namespace)
		pod.SetName(key.Name)
		objects[key] = pod
	}

	reader := staticFrameReader{frame: CacheFrame{
		util.CanonicalGroupKind("", "Pod"): objects,
	}}
	c := NewClient("test", scheme, reader, &recordingRecorder{})
	list := &corev1.PodList{}
	ctx := WithFrameID(context.Background(), "frame-1")
	require.NoError(t, c.List(ctx, list))
	require.Len(t, list.Items, 3)

	got := make([]types.NamespacedName, 0, len(list.Items))
	for _, pod := range list.Items {
		got = append(got, types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name})
	}
	assert.Equal(t, []types.NamespacedName{
		{Namespace: "a", Name: "first"},
		{Namespace: "a", Name: "second"},
		{Namespace: "z", Name: "first"},
	}, got)
}
