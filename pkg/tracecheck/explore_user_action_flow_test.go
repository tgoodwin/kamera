package tracecheck

import (
	"context"
	"strconv"
	"testing"

	"github.com/tgoodwin/kamera/pkg/event"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type noopReconciler struct{}

func (noopReconciler) Reconcile(context.Context, reconcile.Request) (reconcile.Result, error) {
	return reconcile.Result{}, nil
}

func coreScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	return scheme
}

func buildStartStateOrFatal(t *testing.T, b *ExplorerBuilder, objects ...ctrlclient.Object) StateNode {
	t.Helper()
	state, err := b.BuildStartStateFromObjects(objects, nil)
	if err != nil {
		t.Fatalf("build start state: %v", err)
	}
	return state
}

func TestExplore_UserActionTriggersReconcileFanout(t *testing.T) {
	scheme := coreScheme(t)
	builder := NewExplorerBuilder(scheme)
	builder.WithMaxDepth(6)
	builder.WithReconciler("ConfigMapController", func(ctrlclient.Client) Reconciler {
		return &noopReconciler{}
	}).For("ConfigMap").Watches("ConfigMap", EnqueueRequestForObject())
	builder.WithUserActions([]UserAction{
		{
			ID:     "create-cm",
			OpType: event.CREATE,
			Payload: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "created-by-user-action",
				},
				Data: map[string]string{"k": "v1"},
			},
		},
	})

	explorer, err := builder.Build("standalone")
	if err != nil {
		t.Fatalf("build explorer: %v", err)
	}

	start := buildStartStateOrFatal(t, builder, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "seed-pod",
		},
	})

	result := explorer.Explore(context.Background(), start)
	if len(result.ConvergedStates) != 1 {
		t.Fatalf("expected 1 converged state, got %d", len(result.ConvergedStates))
	}
	path := result.ConvergedStates[0].Paths[0]
	if len(path) < 2 {
		t.Fatalf("expected at least 2 steps (user action + triggered reconcile), got %d", len(path))
	}

	userStep := path[0]
	if userStep.ControllerID != UserControllerID {
		t.Fatalf("expected first step controller %q, got %q", UserControllerID, userStep.ControllerID)
	}
	if got := userStep.StepMetadata[UserActionIDMetadataKey]; got != "create-cm" {
		t.Fatalf("expected user action id metadata create-cm, got %q", got)
	}
	if len(userStep.PendingReconciles) == 0 {
		t.Fatalf("expected user action step to trigger pending reconciles")
	}
	firstPending := userStep.PendingReconciles[0]
	if firstPending.ReconcilerID != "ConfigMapController" {
		t.Fatalf("expected triggered reconciler ConfigMapController, got %q", firstPending.ReconcilerID)
	}
	if firstPending.Request.Name != "created-by-user-action" || firstPending.Request.Namespace != "default" {
		t.Fatalf("expected triggered request default/created-by-user-action, got %s/%s", firstPending.Request.Namespace, firstPending.Request.Name)
	}
	if path[1].ControllerID != "ConfigMapController" {
		t.Fatalf("expected second step from ConfigMapController, got %q", path[1].ControllerID)
	}
}

func TestExplore_UserActionIndexProgressesInOrder(t *testing.T) {
	scheme := coreScheme(t)
	builder := NewExplorerBuilder(scheme)
	builder.WithMaxDepth(8)
	builder.WithReconciler("PodController", func(ctrlclient.Client) Reconciler {
		return &noopReconciler{}
	}).For("Pod")
	builder.WithUserActions([]UserAction{
		{
			ID:     "create-cm",
			OpType: event.CREATE,
			Payload: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "workflow-cm",
				},
				Data: map[string]string{"k": "v1"},
			},
		},
		{
			ID:     "update-cm",
			OpType: event.UPDATE,
			Payload: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "workflow-cm",
				},
				Data: map[string]string{"k": "v2"},
			},
		},
	})

	explorer, err := builder.Build("standalone")
	if err != nil {
		t.Fatalf("build explorer: %v", err)
	}

	start := buildStartStateOrFatal(t, builder, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "seed-pod",
		},
	})

	result := explorer.Explore(context.Background(), start)
	if len(result.ConvergedStates) != 1 {
		t.Fatalf("expected 1 converged state, got %d", len(result.ConvergedStates))
	}
	path := result.ConvergedStates[0].Paths[0]

	userSteps := make([]*ReconcileResult, 0, 2)
	for _, step := range path {
		if step != nil && step.ControllerID == UserControllerID {
			userSteps = append(userSteps, step)
		}
	}
	if len(userSteps) != 2 {
		t.Fatalf("expected exactly 2 user steps, got %d", len(userSteps))
	}

	for expectedIdx, step := range userSteps {
		gotIdx := step.StepMetadata[UserActionIndexMetadataKey]
		if gotIdx != strconv.Itoa(expectedIdx) {
			t.Fatalf("expected user action index %d, got %q", expectedIdx, gotIdx)
		}
	}
	if userSteps[0].StepMetadata[UserActionIDMetadataKey] != "create-cm" {
		t.Fatalf("expected first user step id create-cm, got %q", userSteps[0].StepMetadata[UserActionIDMetadataKey])
	}
	if userSteps[1].StepMetadata[UserActionIDMetadataKey] != "update-cm" {
		t.Fatalf("expected second user step id update-cm, got %q", userSteps[1].StepMetadata[UserActionIDMetadataKey])
	}
}

func TestExplore_NonMutatingUserActionIsAllowed(t *testing.T) {
	scheme := coreScheme(t)
	builder := NewExplorerBuilder(scheme)
	builder.WithMaxDepth(6)
	builder.WithReconciler("PodController", func(ctrlclient.Client) Reconciler {
		return &noopReconciler{}
	}).For("Pod")
	builder.WithUserActions([]UserAction{
		{
			ID:     "noop-update",
			OpType: event.UPDATE,
			Payload: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "existing-cm",
				},
				Data: map[string]string{"k": "same"},
			},
		},
	})

	explorer, err := builder.Build("standalone")
	if err != nil {
		t.Fatalf("build explorer: %v", err)
	}

	start := buildStartStateOrFatal(t, builder,
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "seed-pod",
			},
		},
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "existing-cm",
			},
			Data: map[string]string{"k": "same"},
		},
	)

	result := explorer.Explore(context.Background(), start)
	if len(result.ConvergedStates) != 1 {
		t.Fatalf("expected 1 converged state for no-op user action, got %d", len(result.ConvergedStates))
	}
	if len(result.AbortedStates) != 0 {
		t.Fatalf("expected no aborted states for no-op user action, got %d", len(result.AbortedStates))
	}
	path := result.ConvergedStates[0].Paths[0]
	if len(path) == 0 || path[0] == nil {
		t.Fatalf("expected converged path to include user step")
	}
	if path[0].ControllerID != UserControllerID {
		t.Fatalf("expected first step controller %q, got %q", UserControllerID, path[0].ControllerID)
	}
	if path[0].Error != "" {
		t.Fatalf("expected no-op user step to be non-error, got %q", path[0].Error)
	}
}
