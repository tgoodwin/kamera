package controller

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// PodStateMachine describes a stateful mechanism to advance a Pod through a lifecycle.
type PodStateMachine interface {
	// Advance returns the next desired status for the Pod. The done flag indicates
	// that the machine has reached a terminal state. Implementations must be thread-safe.
	Advance(ctx context.Context, pod *corev1.Pod) (PodStatusStep, bool, error)
}

// PodStatusStep captures a desired PodPhase and optional condition set.
type PodStatusStep struct {
	Phase      corev1.PodPhase
	Conditions []corev1.PodCondition
	PodIP      string
	HostIP     string
}

// PodStateMachineFactory constructs a PodStateMachine for a given Pod.
type PodStateMachineFactory interface {
	NewStateMachine(pod *corev1.Pod) PodStateMachine
}

// SequentialPodStateMachineFactory produces sequential machines with a fixed set of steps.
type SequentialPodStateMachineFactory struct {
	Steps []PodStatusStep
}

// DefaultPodLifecycle returns the standard Pending -> Running -> Ready sequence.
func DefaultPodLifecycle() []PodStatusStep {
	return []PodStatusStep{
		{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
			},
		},
		{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
				{Type: corev1.ContainersReady, Status: corev1.ConditionFalse},
				{Type: corev1.PodReady, Status: corev1.ConditionFalse},
			},
		},
		{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
				{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
			PodIP:  "10.0.0.1",
			HostIP: "192.168.0.1",
		},
	}
}

// NewDefaultPodLifecycleFactory returns a factory that emits the default lifecycle.
func NewDefaultPodLifecycleFactory() PodStateMachineFactory {
	return &SequentialPodStateMachineFactory{
		Steps: DefaultPodLifecycle(),
	}
}

// NewStateMachine constructs a fresh sequential state machine for the provided Pod.
func (f *SequentialPodStateMachineFactory) NewStateMachine(_ *corev1.Pod) PodStateMachine {
	steps := make([]PodStatusStep, len(f.Steps))
	copy(steps, f.Steps)
	return &sequentialPodStateMachine{
		steps: steps,
	}
}

type sequentialPodStateMachine struct {
	mu    sync.Mutex
	index int
	steps []PodStatusStep
}

func (s *sequentialPodStateMachine) Advance(_ context.Context, _ *corev1.Pod) (PodStatusStep, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.index >= len(s.steps) {
		return PodStatusStep{}, true, nil
	}
	step := s.steps[s.index]
	s.index++
	done := s.index >= len(s.steps)
	return step, done, nil
}

// PodLifecycleReconciler simulates kubelet behaviour, updating Pod status over several reconciles.
type PodLifecycleReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	Factory PodStateMachineFactory

	mu        sync.Mutex
	machines  map[types.NamespacedName]PodStateMachine
	requeue   time.Duration
	OnFailure func(context.Context, *corev1.Pod, error)
}

type finishedStateMachine struct{}

func (finishedStateMachine) Advance(context.Context, *corev1.Pod) (PodStatusStep, bool, error) {
	return PodStatusStep{}, true, nil
}

var finishedMachine PodStateMachine = finishedStateMachine{}

// NewPodLifecycleReconciler returns a reconciler with an optional requeue delay.
func NewPodLifecycleReconciler(c client.Client, scheme *runtime.Scheme, factory PodStateMachineFactory, requeue time.Duration) *PodLifecycleReconciler {
	if factory == nil {
		panic("pod lifecycle reconciler requires a state machine factory")
	}
	return &PodLifecycleReconciler{
		Client:   c,
		Scheme:   scheme,
		Factory:  factory,
		machines: make(map[types.NamespacedName]PodStateMachine),
		requeue:  requeue,
	}
}

// Reconcile drives the Pod through its configured lifecycle.
func (r *PodLifecycleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	pod := &corev1.Pod{}
	if err := r.Get(ctx, req.NamespacedName, pod); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !pod.DeletionTimestamp.IsZero() {
		r.deleteMachine(req.NamespacedName)
		return ctrl.Result{}, nil
	}

	machine := r.machineForPod(req.NamespacedName, pod)
	step, done, err := machine.Advance(ctx, pod.DeepCopy())
	if err != nil {
		logger.Error(err, "state machine failed", "pod", req.NamespacedName)
		if r.OnFailure != nil {
			r.OnFailure(ctx, pod.DeepCopy(), err)
		}
		return ctrl.Result{}, fmt.Errorf("advance pod lifecycle: %w", err)
	}

	updated := pod.DeepCopy()
	if applyStatusStep(updated, step) {
		if err := r.Status().Update(ctx, updated); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating pod status: %w", err)
		}
		logger.Info("updated pod status", "phase", updated.Status.Phase, "pod", req.NamespacedName)
	}

	if done || isTerminalPhase(updated.Status.Phase) {
		r.markFinished(req.NamespacedName)
		return ctrl.Result{}, nil
	}

	result := ctrl.Result{}
	if r.requeue > 0 {
		result.RequeueAfter = r.requeue
	} else {
		result.Requeue = true
	}
	return result, nil
}

func (r *PodLifecycleReconciler) machineForPod(key types.NamespacedName, pod *corev1.Pod) PodStateMachine {
	r.mu.Lock()
	defer r.mu.Unlock()

	if machine, ok := r.machines[key]; ok {
		return machine
	}

	machine := r.Factory.NewStateMachine(pod.DeepCopy())
	r.machines[key] = machine
	return machine
}

func (r *PodLifecycleReconciler) markFinished(key types.NamespacedName) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.machines[key] = finishedMachine
}

func (r *PodLifecycleReconciler) deleteMachine(key types.NamespacedName) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.machines, key)
}

func applyStatusStep(pod *corev1.Pod, step PodStatusStep) bool {
	if step.Phase == "" && len(step.Conditions) == 0 && step.PodIP == "" && step.HostIP == "" {
		return false
	}

	original := pod.Status.DeepCopy()

	if step.Phase != "" {
		pod.Status.Phase = step.Phase
	}

	if step.PodIP != "" {
		pod.Status.PodIP = step.PodIP
		if len(pod.Status.PodIPs) == 0 {
			pod.Status.PodIPs = []corev1.PodIP{{IP: step.PodIP}}
		} else {
			pod.Status.PodIPs[0] = corev1.PodIP{IP: step.PodIP}
		}
	}

	if step.HostIP != "" {
		pod.Status.HostIP = step.HostIP
	}

	if len(step.Conditions) > 0 {
		now := metav1.Now()
		condMap := make(map[corev1.PodConditionType]corev1.PodCondition)
		for _, cond := range pod.Status.Conditions {
			condMap[cond.Type] = cond
		}
		for _, cond := range step.Conditions {
			existing, found := condMap[cond.Type]
			if !found || existing.Status != cond.Status {
				cond.LastTransitionTime = now
			} else {
				cond.LastTransitionTime = existing.LastTransitionTime
			}
			condMap[cond.Type] = cond
		}
		pod.Status.Conditions = make([]corev1.PodCondition, 0, len(condMap))
		for _, cond := range condMap {
			pod.Status.Conditions = append(pod.Status.Conditions, cond)
		}
		sort.Slice(pod.Status.Conditions, func(i, j int) bool {
			return pod.Status.Conditions[i].Type < pod.Status.Conditions[j].Type
		})
	}

	return !equality.Semantic.DeepEqual(*original, pod.Status)
}

func isTerminalPhase(phase corev1.PodPhase) bool {
	return phase == corev1.PodFailed || phase == corev1.PodSucceeded
}

// SetupWithManager wires the reconciler into the controller manager.
func (r *PodLifecycleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Pod{}).
		Complete(r)
}
