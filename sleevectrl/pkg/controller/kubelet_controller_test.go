package controller

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestPodLifecycleSequentialTransitions(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("adding corev1 to scheme: %v", err)
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-pod",
			Namespace: "default",
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(pod).
		Build()

	factory := &DeterministicPodStateMachineFactory{
		Steps: []PodStatusStep{
			{
				Phase: corev1.PodPending,
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
				},
			},
			{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{
					{Type: corev1.ContainersReady, Status: corev1.ConditionFalse},
					{Type: corev1.PodReady, Status: corev1.ConditionFalse},
				},
			},
			{
				Phase:  corev1.PodRunning,
				PodIP:  "10.1.0.5",
				HostIP: "192.168.1.10",
				Conditions: []corev1.PodCondition{
					{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		},
		// CrashProbabilities defaults to nil (no crashes)
	}

	reconciler := NewPodLifecycleReconciler(client, scheme, factory, 0)

	ctx := context.Background()
	key := types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}
	req := ctrl.Request{NamespacedName: key}

	// First transition: Pending
	res, err := reconciler.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("reconcile 1: %v", err)
	}
	if !res.Requeue {
		t.Fatalf("expected requeue after first transition")
	}
	if err := client.Get(ctx, key, pod); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if pod.Status.Phase != corev1.PodPending {
		t.Fatalf("expected phase Pending, got %s", pod.Status.Phase)
	}

	// Second transition: Running but not ready
	res, err = reconciler.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	if !res.Requeue {
		t.Fatalf("expected requeue after second transition")
	}
	if err := client.Get(ctx, key, pod); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if pod.Status.Phase != corev1.PodRunning {
		t.Fatalf("expected phase Running, got %s", pod.Status.Phase)
	}

	// Third transition: Running and ready
	res, err = reconciler.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("reconcile 3: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Fatalf("expected no requeue after terminal transition")
	}
	if err := client.Get(ctx, key, pod); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if pod.Status.Phase != corev1.PodRunning {
		t.Fatalf("expected phase Running, got %s", pod.Status.Phase)
	}

	ready := false
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			ready = true
		}
	}
	if !ready {
		t.Fatalf("expected PodReady condition to be true")
	}

	// Fourth reconcile should be a no-op.
	res, err = reconciler.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("reconcile 4: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Fatalf("expected no requeue after lifecycle completion")
	}
	if err := client.Get(ctx, key, pod); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if pod.Status.Phase != corev1.PodRunning {
		t.Fatalf("expected phase Running, got %s", pod.Status.Phase)
	}
}

func TestApplyStatusStepPopulatesRunningContainerStatuses(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{Containers: []corev1.Container{
			{Name: "cassandra", Image: "example/cassandra:1"},
			{Name: "sidecar", Image: "example/sidecar:1"},
		}},
	}

	changed := applyStatusStep(pod, PodStatusStep{
		Phase: corev1.PodRunning,
		Conditions: []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionFalse},
		},
	})
	if !changed {
		t.Fatal("expected running status step to change pod status")
	}
	if len(pod.Status.ContainerStatuses) != 2 {
		t.Fatalf("expected two container statuses, got %d", len(pod.Status.ContainerStatuses))
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.State.Running == nil {
			t.Fatalf("expected container %q to be running", status.Name)
		}
		if status.Ready {
			t.Fatalf("expected container %q not to be ready", status.Name)
		}
	}

	startedAt := pod.Status.ContainerStatuses[0].State.Running.StartedAt
	containersReady := false
	applyStatusStep(pod, PodStatusStep{
		Phase:           corev1.PodRunning,
		ContainersReady: &containersReady,
		Conditions: []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		},
	})
	for _, status := range pod.Status.ContainerStatuses {
		if status.Ready {
			t.Fatalf("expected container %q readiness override", status.Name)
		}
		if !status.State.Running.StartedAt.Equal(&startedAt) {
			t.Fatalf("expected container %q start time to be preserved", status.Name)
		}
	}
}

type failingFactory struct {
	err error
}

func (f *failingFactory) NewStateMachine(_ *corev1.Pod) PodStateMachine {
	return &failingStateMachine{err: f.err}
}

type failingStateMachine struct {
	err error
}

func (f *failingStateMachine) Advance(context.Context, *corev1.Pod) (PodStatusStep, bool, error) {
	return PodStatusStep{}, false, f.err
}

func TestPodLifecycleErrorPath(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("adding corev1 to scheme: %v", err)
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "err-pod",
			Namespace: "default",
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(pod).
		Build()

	expectedErr := fmt.Errorf("state machine failure")
	reconciler := NewPodLifecycleReconciler(client, scheme, &failingFactory{err: expectedErr}, 0)

	failureCalled := false
	reconciler.OnFailure = func(_ context.Context, _ *corev1.Pod, err error) {
		if err == expectedErr {
			failureCalled = true
		}
	}

	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}}
	if _, err := reconciler.Reconcile(ctx, req); err == nil {
		t.Fatalf("expected reconcile error")
	}
	if !failureCalled {
		t.Fatalf("expected OnFailure callback to be invoked")
	}
}
