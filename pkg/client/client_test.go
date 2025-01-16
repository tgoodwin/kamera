package client

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/tgoodwin/sleeve/mocks"
	corev1 "k8s.io/api/core/v1"
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

func Test_Update(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockClient(ctrl)
	c := newClient(mockClient)

	ctx := context.TODO()
	pod := &corev1.Pod{}

	mockClient.EXPECT().Update(ctx, pod).Return(errors.New("API call failed"))

	err := c.Update(ctx, pod)
	assert.Error(t, err)
	assert.Equal(t, "API call failed", err.Error())
}
