package test

import (
	"context"
	"os"
	"runtime/pprof"

	_ "net/http/pprof"
	"strconv"
	"testing"

	"github.com/tgoodwin/sleeve/pkg/emitter"
	"github.com/tgoodwin/sleeve/pkg/tracegen"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"

	ctrl "sigs.k8s.io/controller-runtime"
	client "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// // RealisticReconciler does some Gets, Lists, Updates, Creates
type RealisticReconciler struct {
	client client.Client
}

func (r *RealisticReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var pod corev1.Pod
	if err := r.client.Get(ctx, req.NamespacedName, &pod); err != nil {
		// Ignore error for benchmark simplicity
	}

	var pods corev1.PodList
	if err := r.client.List(ctx, &pods); err != nil {
		// Ignore error for benchmark simplicity
	}

	// Simulate Update
	pod.Labels = map[string]string{"bench": "true"}
	_ = r.client.Update(ctx, &pod)

	// Simulate Create
	newPod := corev1.Pod{
		ObjectMeta: pod.ObjectMeta,
	}
	newPod.Name = pod.Name + "-clone"
	_ = r.client.Create(ctx, &newPod)

	return ctrl.Result{}, nil
}

// // SetupFakeClient builds a fake client with some objects preloaded
func SetupFakeClient() client.Client {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	var objs []runtime.Object
	// Prepopulate with some Pods
	for i := 0; i < 100; i++ {
		objs = append(objs, &corev1.Pod{
			ObjectMeta: ctrl.ObjectMeta{
				Name:      "pod-" + strconv.Itoa(i),
				Namespace: "default",
				Labels: map[string]string{
					"tracey-uid": "test-uid-" + string(rune('a'+i)),
				},
			},
		})
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
}

// ------------------------------------------
// Benchmarks
// run with go test -bench=BenchmarkReconcile_ -benchmem ./...
// ------------------------------------------
func BenchmarkReconcile_Baseline(b *testing.B) {
	withProfiles(b, "cpu_baseline.pprof", "heap_baseline.pprof", func(b *testing.B) {
		b.ReportAllocs()
		ctx := context.Background()

		baseClient := SetupFakeClient()
		reconciler := &RealisticReconciler{client: baseClient}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Namespace: "default", Name: "pod-1"},
			})
		}
	})
}

func BenchmarkReconcile_InlineEmitter(b *testing.B) {
	withProfiles(b, "cpu_inline.pprof", "heap_inline.pprof", func(b *testing.B) {
		b.ReportAllocs()
		ctx := context.Background()

		emitter := emitter.NewFileEmitter("/dev/null")
		defer emitter.Close()

		baseClient := SetupFakeClient()
		frameExtractor := func(ctx context.Context) string { return "test-frame" }
		instrumented := tracegen.New(
			baseClient,
			"test-reconciler",
			emitter,
			tracegen.NewContextTracker(
				"test-reconciler",
				emitter,
				frameExtractor,
			),
		)

		reconciler := &RealisticReconciler{client: instrumented}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Namespace: "default", Name: "pod-1"},
			})
		}
	})
}

func BenchmarkReconcile_AsyncEmitter(b *testing.B) {
	withProfiles(b, "cpu_async.pprof", "heap_async.pprof", func(b *testing.B) {
		b.ReportAllocs()
		ctx := context.Background()

		baseClient := SetupFakeClient()
		fileEmitter := emitter.NewFileEmitter("/dev/null")
		// defer fileEmitter.Close()

		asyncEmitter := emitter.NewAsyncEmitter(
			fileEmitter,
			1000,
		)

		frameExtractor := func(ctx context.Context) string { return "test-frame" }
		instrumented := tracegen.New(
			baseClient,
			"test-reconciler",
			asyncEmitter,
			tracegen.NewContextTracker(
				"test-reconciler",
				asyncEmitter,
				frameExtractor,
			),
		)

		reconciler := &RealisticReconciler{client: instrumented}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Namespace: "default", Name: "pod-1"},
			})
		}
	})
}

func withProfiles(b *testing.B, cpuProfileFilename, heapProfileFilename string, fn func(b *testing.B)) {
	// Create CPU profile file
	cpuFile, err := os.Create(cpuProfileFilename)
	if err != nil {
		b.Fatalf("could not create CPU profile: %v", err)
	}
	defer cpuFile.Close()

	// Start CPU profiling
	if err := pprof.StartCPUProfile(cpuFile); err != nil {
		b.Fatalf("could not start CPU profile: %v", err)
	}

	// Run the actual benchmark
	fn(b)

	// Stop CPU profiling
	pprof.StopCPUProfile()

	// Create Heap profile file
	heapFile, err := os.Create(heapProfileFilename)
	if err != nil {
		b.Fatalf("could not create heap profile: %v", err)
	}
	defer heapFile.Close()

	// Write current heap profile
	if err := pprof.WriteHeapProfile(heapFile); err != nil {
		b.Fatalf("could not write heap profile: %v", err)
	}
}
