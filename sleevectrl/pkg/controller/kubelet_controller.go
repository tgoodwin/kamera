package controller

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/tgoodwin/kamera/pkg/simclock"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// PodLifecycleStage represents a named stage in the Pod lifecycle.
// Used to configure crash probabilities without magic numbers.
type PodLifecycleStage int

const (
	// StageScheduled is after the Pod is scheduled (Pending + PodScheduled=True)
	StageScheduled PodLifecycleStage = 0
	// StageRunning is after the Pod starts running but before containers are ready
	StageRunning PodLifecycleStage = 1
	// StageReady is after the Pod is fully ready (Running + Ready=True + IPs assigned)
	StageReady PodLifecycleStage = 2
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

// DeterministicPodStateMachineFactory produces state machines that determine the next
// step based on the current Pod state, not an internal counter. This ensures deterministic
// behavior when the same Pod state is encountered across different exploration branches.
type DeterministicPodStateMachineFactory struct {
	Steps []PodStatusStep

	// CrashProbabilities maps lifecycle stage to crash probability (0.0 to 1.0).
	// After completing a stage, if a probability is set for that stage, there's
	// a random chance the Pod will crash instead of advancing to the next stage.
	// This is used for intentional nondeterminism in exploration testing.
	CrashProbabilities map[PodLifecycleStage]float64
}

// DefaultPodLifecycle returns the standard Pending -> Running -> Ready sequence.
// Stage indices:
//   - 0: Pending + PodScheduled
//   - 1: Running + ContainersReady=False + PodReady=False
//   - 2: Running + ContainersReady=True + PodReady=True + IPs assigned
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
// The state machine is deterministic: given the same Pod state, it always returns
// the same next step, regardless of internal state or exploration branch.
func NewDefaultPodLifecycleFactory() PodStateMachineFactory {
	return &DeterministicPodStateMachineFactory{
		Steps: DefaultPodLifecycle(),
	}
}

// NewStateMachine constructs a deterministic state machine for the provided Pod.
func (f *DeterministicPodStateMachineFactory) NewStateMachine(_ *corev1.Pod) PodStateMachine {
	steps := make([]PodStatusStep, len(f.Steps))
	copy(steps, f.Steps)
	return &deterministicPodStateMachine{
		steps:              steps,
		crashProbabilities: f.CrashProbabilities,
	}
}

func (s PodLifecycleStage) String() string {
	switch s {
	case StageScheduled:
		return "Scheduled"
	case StageRunning:
		return "Running"
	case StageReady:
		return "Ready"
	default:
		return fmt.Sprintf("Stage(%d)", int(s))
	}
}

// deterministicPodStateMachine determines the next step by examining the current Pod state,
// not by maintaining an internal index. This ensures the same Pod state always produces
// the same result, making the reconciler safe for exploration across branches.
type deterministicPodStateMachine struct {
	steps              []PodStatusStep
	crashProbabilities map[PodLifecycleStage]float64
}

func (s *deterministicPodStateMachine) Advance(_ context.Context, pod *corev1.Pod) (PodStatusStep, bool, error) {
	if len(s.steps) == 0 {
		return PodStatusStep{}, true, nil
	}

	// Determine current lifecycle stage based on Pod state
	currentStage := s.determineCurrentStage(pod)

	// If we're at or past the final stage, we're done
	if currentStage >= len(s.steps) {
		return PodStatusStep{}, true, nil
	}

	// Check if we should randomly crash the Pod at this stage
	if prob, ok := s.crashProbabilities[PodLifecycleStage(currentStage)]; ok && prob > 0 {
		if rand.Float64() < prob {
			return PodStatusStep{
				Phase: corev1.PodFailed,
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionFalse, Reason: "SimulatedCrash"},
					{Type: corev1.ContainersReady, Status: corev1.ConditionFalse, Reason: "SimulatedCrash"},
				},
			}, true, nil
		}
	}

	// Return the step for the current stage (the Pod will be advanced to this state)
	step := s.steps[currentStage]
	done := currentStage >= len(s.steps)-1
	return step, done, nil
}

// determineCurrentStage examines the Pod's current status and returns which lifecycle
// stage it should transition to next. This is deterministic based on Pod state.
//
// The logic finds the LAST step whose requirements are satisfied by the current Pod state,
// then returns the NEXT step (or len(steps) if we're at/past the final step).
func (s *deterministicPodStateMachine) determineCurrentStage(pod *corev1.Pod) int {
	// Build a map of current conditions for easy lookup
	condMap := make(map[corev1.PodConditionType]corev1.ConditionStatus)
	for _, c := range pod.Status.Conditions {
		condMap[c.Type] = c.Status
	}

	// Find the highest step index whose requirements are satisfied.
	// This handles the case where a Pod has advanced past intermediate steps.
	highestSatisfied := -1
	for i, step := range s.steps {
		if s.stepSatisfied(pod, step, condMap) {
			highestSatisfied = i
		}
	}

	// Return the next step after the highest satisfied one
	return highestSatisfied + 1
}

// stepSatisfied checks if the Pod's current state satisfies (matches or exceeds) the step's requirements.
func (s *deterministicPodStateMachine) stepSatisfied(pod *corev1.Pod, step PodStatusStep, condMap map[corev1.PodConditionType]corev1.ConditionStatus) bool {
	// Check phase progression
	if step.Phase != "" {
		if !phaseAtOrBeyond(pod.Status.Phase, step.Phase) {
			return false
		}
	}

	// Check all conditions in this step
	for _, stepCond := range step.Conditions {
		currentStatus, exists := condMap[stepCond.Type]
		if !exists {
			return false
		}
		// For condition checks, we need exact match or "better" state
		if !conditionAtOrBeyond(currentStatus, stepCond.Status) {
			return false
		}
	}

	// Check IPs if specified (IPs are additive, once set they should remain)
	if step.PodIP != "" && pod.Status.PodIP != step.PodIP {
		return false
	}
	if step.HostIP != "" && pod.Status.HostIP != step.HostIP {
		return false
	}

	return true
}

// phaseAtOrBeyond returns true if currentPhase is at or beyond targetPhase in the lifecycle.
func phaseAtOrBeyond(current, target corev1.PodPhase) bool {
	// Define phase ordering: Pending < Running < Succeeded/Failed
	phaseOrder := map[corev1.PodPhase]int{
		"":                  0,
		corev1.PodPending:   1,
		corev1.PodRunning:   2,
		corev1.PodSucceeded: 3,
		corev1.PodFailed:    3,
	}
	return phaseOrder[current] >= phaseOrder[target]
}

// conditionAtOrBeyond returns true if the current condition status is "at or beyond" the target.
// For pod lifecycle: Unknown < False < True (for positive conditions like Ready)
func conditionAtOrBeyond(current, target corev1.ConditionStatus) bool {
	// For most lifecycle conditions, True is the "goal" state
	// If target is True, current must be True
	// If target is False, current can be False or True (we've moved past it)
	// If target is Unknown, any status is fine
	if target == corev1.ConditionTrue {
		return current == corev1.ConditionTrue
	}
	if target == corev1.ConditionFalse {
		return current == corev1.ConditionFalse || current == corev1.ConditionTrue
	}
	// target is Unknown, any status satisfies it
	return true
}

// PodLifecycleReconciler simulates kubelet behaviour, updating Pod status over several reconciles.
// The reconciler uses a deterministic state machine factory that determines the next step based
// on the current Pod state, not internal mutable state. This makes the reconciler safe for use
// in exploration scenarios where the same Pod may be reconciled across different branches.
type PodLifecycleReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	Factory   PodStateMachineFactory
	requeue   time.Duration
	OnFailure func(context.Context, *corev1.Pod, error)
}

// NewPodLifecycleReconciler returns a reconciler with an optional requeue delay.
func NewPodLifecycleReconciler(c client.Client, scheme *runtime.Scheme, factory PodStateMachineFactory, requeue time.Duration) *PodLifecycleReconciler {
	if factory == nil {
		panic("pod lifecycle reconciler requires a state machine factory")
	}
	return &PodLifecycleReconciler{
		Client:  c,
		Scheme:  scheme,
		Factory: factory,
		requeue: requeue,
	}
}

// Reconcile drives the Pod through its configured lifecycle.
// The state machine is deterministic: given the same Pod state, it always returns
// the same next step, regardless of which exploration branch is running.
func (r *PodLifecycleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	pod := &corev1.Pod{}
	if err := r.Get(ctx, req.NamespacedName, pod); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	pod.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Pod"))

	if !pod.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// Create a fresh state machine for this reconcile - the machine is stateless
	// and determines the next step based on the current Pod state.
	machine := r.Factory.NewStateMachine(pod)
	step, done, err := machine.Advance(ctx, pod.DeepCopy())
	if err != nil {
		logger.Error(err, "state machine failed", "pod", req.NamespacedName)
		if r.OnFailure != nil {
			r.OnFailure(ctx, pod.DeepCopy(), err)
		}
		return ctrl.Result{}, fmt.Errorf("advance pod lifecycle: %w", err)
	}

	updated := pod.DeepCopy()
	updated.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Pod"))
	if applyStatusStep(updated, step) {
		if err := r.Status().Update(ctx, updated); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating pod status: %w", err)
		}
		logger.Info("updated pod status", "phase", updated.Status.Phase, "pod", req.NamespacedName)
	}

	if done || isTerminalPhase(updated.Status.Phase) {
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
		now := metav1.NewTime(simclock.Now())
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
