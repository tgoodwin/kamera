package emitter

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/tgoodwin/sleeve/mocks"
)

func TestAsyncEmitterBasicProcessing(t *testing.T) {
	// Create a mock emitter and an async emitter
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := mocks.NewMockEmitter(ctrl)
	async := NewAsyncEmitter(mock, 10)

	// Clean up
	async.Shutdown()
}
