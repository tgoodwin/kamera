package tracegen

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/tgoodwin/sleeve/mocks"
	"github.com/tgoodwin/sleeve/pkg/emitter"
	"github.com/tgoodwin/sleeve/pkg/tag"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Simple test to verify the benchmarks will work properly
func TestGetOperation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockClient(ctrl)
	fileEmitter := emitter.NewFileEmitter("/dev/null")

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "test-pod",
			Labels: map[string]string{
				"tracey-uid": "test-uid",
			},
		},
	}
	key := types.NamespacedName{Namespace: "default", Name: "test-pod"}

	mockClient.EXPECT().Get(gomock.Any(), key, pod).Return(nil)

	instrumentedClient := &Client{
		Client:       mockClient,
		reconcilerID: "test-controller",
		emitter:      fileEmitter,
		config:       NewConfig(),
		tracker: &ContextTracker{
			rc:         &ReconcileContext{},
			getFrameID: func(ctx context.Context) string { return "test-frame-id" },
		},
	}

	ctx := context.WithValue(context.Background(), reconcileIDKey{}, "test-frame-id")
	err := instrumentedClient.Get(ctx, key, pod)

	assert.NoError(t, err)
}

// Benchmark for Get operation
func BenchmarkGet(b *testing.B) {
	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	// Setup
	mockClient := mocks.NewMockClient(ctrl)
	fileEmitter := emitter.NewFileEmitter("/dev/null")

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "test-pod",
			Labels: map[string]string{
				"tracey-uid": "test-uid",
			},
		},
	}
	key := types.NamespacedName{Namespace: "default", Name: "test-pod"}

	mockClient.EXPECT().Get(gomock.Any(), key, gomock.Any()).Return(nil).AnyTimes()

	ctx := context.WithValue(context.Background(), reconcileIDKey{}, "test-frame-id")

	// Baseline benchmark - Plain client
	b.Run("Baseline", func(b *testing.B) {
		// Pre-create all the pod copies we'll need to avoid measuring DeepCopy
		podCopies := make([]*corev1.Pod, b.N)
		for i := 0; i < b.N; i++ {
			podCopies[i] = pod.DeepCopy()
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_ = mockClient.Get(ctx, key, podCopies[i])
		}
	})
	b.Run("NoOpEmitter", func(b *testing.B) {
		instrumentedClient := &Client{
			Client:       mockClient,
			reconcilerID: "test-controller",
			emitter:      &emitter.NoopEmitter{},
			config:       NewConfig(),
			tracker: &ContextTracker{
				rc:         &ReconcileContext{},
				getFrameID: func(ctx context.Context) string { return "test-frame-id" },
			},
		}
		// Pre-create all the pod copies we'll need to avoid measuring DeepCopy
		podCopies := make([]*corev1.Pod, b.N)
		for i := 0; i < b.N; i++ {
			podCopies[i] = pod.DeepCopy()
		}
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = instrumentedClient.Get(ctx, key, podCopies[i])
		}
	})

	// Instrumented client
	b.Run("FileEmitter", func(b *testing.B) {
		instrumentedClient := &Client{
			Client:       mockClient,
			reconcilerID: "test-controller",
			emitter:      fileEmitter,
			config:       NewConfig(),
			tracker: &ContextTracker{
				rc:         &ReconcileContext{},
				getFrameID: func(ctx context.Context) string { return "test-frame-id" },
			},
		}

		// Pre-create all the pod copies we'll need to avoid measuring DeepCopy
		podCopies := make([]*corev1.Pod, b.N)
		for i := 0; i < b.N; i++ {
			podCopies[i] = pod.DeepCopy()
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_ = instrumentedClient.Get(ctx, key, podCopies[i])
		}
	})

	b.Run("InMemoryEmitter", func(b *testing.B) {
		instrumentedClient := &Client{
			Client:       mockClient,
			reconcilerID: "test-controller",
			emitter:      emitter.NewInMemoryEmitter(),
			config:       NewConfig(),
			tracker: &ContextTracker{
				rc:         &ReconcileContext{},
				getFrameID: func(ctx context.Context) string { return "test-frame-id" },
			},
		}

		// Pre-create all the pod copies we'll need to avoid measuring DeepCopy
		podCopies := make([]*corev1.Pod, b.N)
		for i := 0; i < b.N; i++ {
			podCopies[i] = pod.DeepCopy()
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_ = instrumentedClient.Get(ctx, key, podCopies[i])
		}
	})
}

// Benchmark for Update operation
func BenchmarkUpdate(b *testing.B) {
	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	// Setup
	mockClient := mocks.NewMockClient(ctrl)
	fileEmitter := emitter.NewFileEmitter("/dev/null")

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "test-pod",
			Labels: map[string]string{
				tag.TraceyRootID: "root-id",
			},
		},
	}

	mockClient.EXPECT().Update(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, obj client.Object, _ ...client.UpdateOption) error {
			// Just return nil, we don't need to do anything with the object
			return nil
		}).AnyTimes()

	ctx := context.Background()

	// Baseline benchmark - Plain client
	b.Run("Baseline", func(b *testing.B) {
		// Pre-create all the pod copies we'll need to avoid measuring DeepCopy
		podCopies := make([]*corev1.Pod, b.N)
		for i := 0; i < b.N; i++ {
			podCopies[i] = pod.DeepCopy()
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_ = mockClient.Update(ctx, podCopies[i])
		}
	})

	// Instrumented client
	b.Run("Instrumented", func(b *testing.B) {
		instrumentedClient := &Client{
			Client:       mockClient,
			reconcilerID: "test-controller",
			emitter:      fileEmitter,
			config:       NewConfig(),
			tracker: &ContextTracker{
				rc: &ReconcileContext{
					reconcileID: "reconcile-id",
					rootID:      "root-id",
				},
				getFrameID: func(ctx context.Context) string { return "test-frame-id" },
			},
		}

		// Pre-create all the pod copies we'll need to avoid measuring DeepCopy
		podCopies := make([]*corev1.Pod, b.N)
		for i := 0; i < b.N; i++ {
			podCopies[i] = pod.DeepCopy()
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_ = instrumentedClient.Update(ctx, podCopies[i])
		}
	})
}

// Benchmark for Create operation
func BenchmarkCreate(b *testing.B) {
	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	// Setup
	mockClient := mocks.NewMockClient(ctrl)
	fileEmitter := emitter.NewFileEmitter("/dev/null")

	templatePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "test-pod",
			Labels: map[string]string{
				tag.TraceyRootID: "root-id",
			},
		},
	}

	mockClient.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, obj client.Object, _ ...client.CreateOption) error {
			// Just return nil, we don't need to do anything with the object
			return nil
		}).AnyTimes()

	ctx := context.Background()

	// Baseline benchmark - Plain client
	b.Run("Baseline", func(b *testing.B) {
		// Pre-create all the pod copies we'll need to avoid measuring DeepCopy
		podCopies := make([]*corev1.Pod, b.N)
		for i := 0; i < b.N; i++ {
			podCopies[i] = templatePod.DeepCopy()
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_ = mockClient.Create(ctx, podCopies[i])
		}
	})

	// Instrumented client
	b.Run("Instrumented", func(b *testing.B) {
		instrumentedClient := &Client{
			Client:       mockClient,
			reconcilerID: "test-controller",
			emitter:      fileEmitter,
			config:       NewConfig(),
			tracker: &ContextTracker{
				rc: &ReconcileContext{
					reconcileID: "reconcile-id",
					rootID:      "root-id",
				},
				getFrameID: func(ctx context.Context) string { return "test-frame-id" },
			},
		}

		// Pre-create all the pod copies we'll need to avoid measuring DeepCopy
		podCopies := make([]*corev1.Pod, b.N)
		for i := 0; i < b.N; i++ {
			podCopies[i] = templatePod.DeepCopy()
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_ = instrumentedClient.Create(ctx, podCopies[i])
		}
	})
}
