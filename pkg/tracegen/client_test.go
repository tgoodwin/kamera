package tracegen

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/tgoodwin/sleeve/mocks"
	"github.com/tgoodwin/sleeve/pkg/tag"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func getFrameID(ctx context.Context) string {
	return ctx.Value(reconcileIDKey{}).(string)
}

func Test_Get(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockClient(ctrl)
	mockEmitter := mocks.NewMockEmitter(ctrl)
	c := &Client{
		Client:  mockClient,
		emitter: mockEmitter,
		config:  NewConfig(),
		tracker: &ContextTracker{
			rc:         &ReconcileContext{},
			emitter:    mockEmitter,
			getFrameID: getFrameID,
		},
	}
	c.reconcilerID = "test-reconcilerID"

	ctx := context.WithValue(context.TODO(), reconcileIDKey{}, "test-reconcileID")
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "test-pod",
		},
	}
	origLabels := map[string]string{"key": "value", "tracey-uid": "test-uid"}
	pod.SetLabels(origLabels)
	key := types.NamespacedName{Namespace: "default", Name: "test-pod"}

	mockClient.EXPECT().Get(ctx, key, pod).Return(nil).AnyTimes()
	mockEmitter.EXPECT().LogOperation(ctx, gomock.Any()).Times(1)
	mockEmitter.EXPECT().LogObjectVersion(ctx, gomock.Any()).Times(1)

	err := c.Get(ctx, key, pod)
	assert.NoError(t, err)

	assert.Equal(t, c.tracker.rc.reconcileID, "test-reconcileID")
	assert.Equal(t, c.tracker.rc.rootID, "test-uid")
}

func Test_List(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockClient(ctrl)
	mockEmitter := mocks.NewMockEmitter(ctrl)
	c := &Client{
		Client:  mockClient,
		emitter: mockEmitter,
		config:  NewConfig(),
		tracker: &ContextTracker{
			rc:         &ReconcileContext{},
			emitter:    mockEmitter,
			getFrameID: getFrameID,
		},
	}
	c.reconcilerID = "test-reconcilerID"

	ctx := context.WithValue(context.TODO(), reconcileIDKey{}, "test-reconcileID")
	podList := &corev1.PodList{
		Items: []corev1.Pod{
			{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod-1",
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod-2",
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod-3",
				},
			},
		},
	}

	traceyUID := "test-uid"
	for i := range podList.Items {
		podList.Items[i].SetLabels(map[string]string{"tracey-uid": traceyUID})
	}

	mockClient.EXPECT().List(ctx, gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
			*list.(*corev1.PodList) = *podList
			return nil
		},
	).Times(1)
	mockEmitter.EXPECT().LogOperation(ctx, gomock.Any()).Times(3)
	mockEmitter.EXPECT().LogObjectVersion(ctx, gomock.Any()).Times(3)

	err := c.List(ctx, podList)
	assert.NoError(t, err)

	assert.Equal(t, 3, len(podList.Items))
}

func Test_UpdateFail(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockClient(ctrl)
	mockEmitter := mocks.NewMockEmitter(ctrl)
	c := &Client{
		Client:  mockClient,
		emitter: mockEmitter,
		config:  NewConfig(),
		tracker: &ContextTracker{
			rc: &ReconcileContext{
				reconcileID: "reconcileID",
				rootID:      "rootID",
			},
			emitter:    mockEmitter,
			getFrameID: getFrameID,
		},
	}

	ctx := context.TODO()
	pod := &corev1.Pod{}
	origLabels := map[string]string{"key": "value", tag.TraceyRootID: "rootID"}
	pod.SetLabels(origLabels)

	mockClient.EXPECT().Update(ctx, pod).Return(errors.New("API call failed"))
	mockEmitter.EXPECT().LogOperation(ctx, gomock.Any()).Times(0)
	mockEmitter.EXPECT().LogObjectVersion(ctx, gomock.Any()).Times(0)

	err := c.Update(ctx, pod)
	assert.Error(t, err)
	assert.Equal(t, origLabels, pod.GetLabels())
	assert.Equal(t, "API call failed", err.Error())
}

func Test_CreateSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockClient(ctrl)
	mockEmitter := mocks.NewMockEmitter(ctrl)
	c := &Client{
		Client:  mockClient,
		emitter: mockEmitter,
		config:  NewConfig(),
		tracker: &ContextTracker{
			rc: &ReconcileContext{
				reconcileID: "reconcileID",
				rootID:      "rootID",
			},
			emitter:    mockEmitter,
			getFrameID: getFrameID,
		},
	}

	ctx := context.WithValue(context.TODO(), reconcileIDKey{}, "reconcileID")
	pod := &corev1.Pod{}
	origLabels := map[string]string{"key": "value", tag.TraceyRootID: "rootID"}
	pod.SetLabels(origLabels)

	mockClient.EXPECT().Create(ctx, pod).Return(nil)
	mockEmitter.EXPECT().LogOperation(ctx, gomock.Any()).Times(1)
	mockEmitter.EXPECT().LogObjectVersion(ctx, gomock.Any()).Times(1)

	shouldBePresent := []string{
		tag.TraceyCreatorID,
		tag.TraceyReconcileID,
		tag.ChangeID,
		tag.TraceyRootID,
		tag.TraceyObjectID,
	}
	err := c.Create(ctx, pod)
	assert.NoError(t, err)

	for _, label := range shouldBePresent {
		_, exists := pod.GetLabels()[label]
		assert.True(t, exists, "label %s should be present", label)
	}
}

func Test_UpdateSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockClient(ctrl)
	mockEmitter := mocks.NewMockEmitter(ctrl)
	c := &Client{
		Client:  mockClient,
		emitter: mockEmitter,
		config:  NewConfig(),
		tracker: &ContextTracker{
			rc: &ReconcileContext{
				reconcileID: "reconcileID",
				rootID:      "rootID",
			},
			emitter:    mockEmitter,
			getFrameID: getFrameID,
		},
	}

	ctx := context.TODO()
	pod := &corev1.Pod{}
	origLabels := map[string]string{"key": "value", tag.TraceyRootID: "rootID"}
	pod.SetLabels(origLabels)

	mockClient.EXPECT().Update(ctx, pod).Return(nil)
	mockEmitter.EXPECT().LogOperation(ctx, gomock.Any()).Times(1)
	mockEmitter.EXPECT().LogObjectVersion(ctx, gomock.Any()).Times(1)

	err := c.Update(ctx, pod)
	assert.NoError(t, err)

	shouldBePresent := []string{
		tag.TraceyCreatorID,
		tag.TraceyReconcileID,
		tag.ChangeID,
		tag.TraceyRootID,
	}
	for _, label := range shouldBePresent {
		_, exists := pod.GetLabels()[label]
		assert.True(t, exists, "label %s should be present", label)
	}
}

func Test_PatchFail(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockClient(ctrl)
	mockEmitter := mocks.NewMockEmitter(ctrl)
	c := &Client{
		Client:  mockClient,
		emitter: mockEmitter,
		config:  NewConfig(),
		tracker: &ContextTracker{
			rc: &ReconcileContext{
				reconcileID: "reconcileID",
				rootID:      "rootID",
			},
			emitter:    mockEmitter,
			getFrameID: getFrameID,
		},
	}

	ctx := context.TODO()
	pod := &corev1.Pod{}
	origLabels := map[string]string{"key": "value", tag.TraceyRootID: "rootID"}
	pod.SetLabels(origLabels)

	mockClient.EXPECT().Patch(ctx, pod, gomock.Any()).Return(errors.New("API call failed"))

	err := c.Patch(ctx, pod, nil)
	assert.Error(t, err)
	assert.Equal(t, origLabels, pod.GetLabels())
	assert.Equal(t, "API call failed", err.Error())
}

func Test_PatchSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockClient(ctrl)
	mockEmitter := mocks.NewMockEmitter(ctrl)
	c := &Client{
		Client:  mockClient,
		emitter: mockEmitter,
		config:  NewConfig(),
		tracker: &ContextTracker{
			rc: &ReconcileContext{
				reconcileID: "reconcileID",
				rootID:      "rootID",
			},
			emitter:    mockEmitter,
			getFrameID: getFrameID,
		},
	}

	ctx := context.TODO()
	pod := &corev1.Pod{}
	origLabels := map[string]string{"key": "value", tag.TraceyRootID: "rootID"}
	pod.SetLabels(origLabels)

	mockClient.EXPECT().Patch(ctx, pod, gomock.Any()).Return(nil)
	mockEmitter.EXPECT().LogOperation(ctx, gomock.Any()).Times(1)
	mockEmitter.EXPECT().LogObjectVersion(ctx, gomock.Any()).Times(1)

	err := c.Patch(ctx, pod, nil)
	assert.NoError(t, err)

	shouldBePresent := []string{
		tag.TraceyCreatorID,
		tag.TraceyReconcileID,
		tag.ChangeID,
		tag.TraceyRootID,
	}
	for _, label := range shouldBePresent {
		_, exists := pod.GetLabels()[label]
		assert.True(t, exists, "label %s should be present", label)
	}
}
