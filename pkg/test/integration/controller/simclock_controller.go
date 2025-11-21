package controller

import (
	"context"
	"fmt"

	"github.com/tgoodwin/kamera/pkg/simclock"
	webappv1 "github.com/tgoodwin/kamera/pkg/test/integration/api/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// SimClockReconciler updates Foo conditions using deterministic simclock timestamps.
type SimClockReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

var simClockConditionOrder = []string{"ReadyPhaseOne", "ReadyPhaseTwo", "ReadyPhaseThree"}

func (r *SimClockReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var foo webappv1.Foo
	if err := r.Get(ctx, req.NamespacedName, &foo); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	current := len(foo.Status.Conditions)
	if current >= len(simClockConditionOrder) {
		return ctrl.Result{}, nil
	}

	condType := simClockConditionOrder[current]
	nextCondition := metav1.Condition{
		Type:               condType,
		Status:             metav1.ConditionTrue,
		Reason:             fmt.Sprintf("phase-%d", current+1),
		LastTransitionTime: metav1.NewTime(simclock.Now()),
	}

	apimeta.SetStatusCondition(&foo.Status.Conditions, nextCondition)
	if err := r.Status().Update(ctx, &foo); err != nil {
		return ctrl.Result{}, err
	}

	// Requeue until all conditions have been written so Explore can advance depth.
	shouldRequeue := current+1 < len(simClockConditionOrder)
	logger.V(2).Info("updated deterministic condition", "type", condType, "depth", current)

	return ctrl.Result{Requeue: shouldRequeue}, nil
}

func (r *SimClockReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&webappv1.Foo{}).
		Complete(r)
}
