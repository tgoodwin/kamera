package client

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
)

func Test_createFixedLengthHash(t *testing.T) {
	type args struct {
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Test createFixedLengthHash",
			args: args{},
			want: "f4e3d2c1b0a9",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := createFixedLengthHash(); got != tt.want {
				t.Errorf("createFixedLengthHash() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_Get(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockClient(ctrl)
	c := newClient(mockClient)
	c.id = "test-reconcilerID"

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

	// TODO clean this up. Do we need to call Get multiple times in our implementation?
	mockClient.EXPECT().Get(ctx, key, pod).Return(nil).AnyTimes()

	err := c.Get(ctx, key, pod)
	assert.NoError(t, err)

	assert.Equal(t, c.reconcileContext.reconcileID, "test-reconcileID")
	assert.Equal(t, c.reconcileContext.rootID, "test-uid")
}

func Test_UpdateFail(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockClient(ctrl)
	c := newClient(mockClient)

	ctx := context.TODO()
	pod := &corev1.Pod{}
	origLabels := map[string]string{"key": "value"}
	pod.SetLabels(origLabels)

	mockClient.EXPECT().Update(ctx, pod).Return(errors.New("API call failed"))

	err := c.Update(ctx, pod)
	assert.Error(t, err)
	assert.Equal(t, origLabels, pod.GetLabels())
	assert.Equal(t, "API call failed", err.Error())
}

func Test_CreateSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockClient(ctrl)
	c := newClient(mockClient)

	rc := ReconcileContext{
		reconcileID: "reconcileID",
		rootID:      "rootID",
	}
	c.reconcileContext = &rc

	ctx := context.WithValue(context.TODO(), reconcileIDKey{}, "reconcileID")
	pod := &corev1.Pod{}
	origLabels := map[string]string{"key": "value"}
	pod.SetLabels(origLabels)

	mockClient.EXPECT().Create(ctx, pod).Return(nil)

	shouldBePresent := []string{
		tag.TraceyCreatorID,
		tag.TraceyReconcileID,
		tag.ChangeID,
		tag.TraceyRootID,
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
	c := newClient(mockClient)

	rc := ReconcileContext{
		reconcileID: "reconcileID",
		rootID:      "rootID",
	}
	c.reconcileContext = &rc

	ctx := context.TODO()
	pod := &corev1.Pod{}
	origLabels := map[string]string{"key": "value"}
	pod.SetLabels(origLabels)

	mockClient.EXPECT().Update(ctx, pod).Return(nil)

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
	c := newClient(mockClient)

	ctx := context.TODO()
	pod := &corev1.Pod{}
	origLabels := map[string]string{"key": "value"}
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
	c := newClient(mockClient)

	rc := ReconcileContext{
		reconcileID: "reconcileID",
		rootID:      "rootID",
	}
	c.reconcileContext = &rc

	ctx := context.TODO()
	pod := &corev1.Pod{}
	origLabels := map[string]string{"key": "value"}
	pod.SetLabels(origLabels)

	mockClient.EXPECT().Patch(ctx, pod, gomock.Any()).Return(nil)

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
