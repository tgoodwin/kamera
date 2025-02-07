package controller

import (
	"context"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	webappv1 "github.com/tgoodwin/sleeve/pkg/test/integration/api/v1"
)

// BarReconciler reconciles a Foo object
type BarReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// Reconcile allows the BarController to modify Foo to introduce variation in execution paths.
func (r *BarReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var foo webappv1.Foo
	if err := r.Get(ctx, req.NamespacedName, &foo); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil // Object deleted
		}
		return ctrl.Result{}, err
	}

	updated := false

	// Flip mode at most once
	if _, exists := foo.Annotations["mode-flip-done"]; !exists {
		// if foo.Status.State == "A-2" {
		// 	panic("ok")
		// }
		if foo.Status.State == "A-1" {
			foo.Spec.Mode = "B"
			if foo.Annotations == nil {
				foo.Annotations = make(map[string]string)
			}
			foo.Annotations["mode-flip-done"] = "true"
			updated = true
		} else if foo.Status.State == "B-1" {
			foo.Spec.Mode = "A"
			if foo.Annotations == nil {
				foo.Annotations = make(map[string]string)
			}
			updated = true
			foo.Annotations["mode-flip-done"] = "true"
		}
		if updated {
			if err := r.Update(ctx, &foo); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// if foo.Status.State == "A-1" {
	// 	panic("ok")
	// }

	// Some duplicated logic to introduce variation
	// among paths to convergence
	if foo.Spec.Mode == "A" && foo.Status.State == "A-1" {
		foo.Status.State = "A-2"
		updated = true
		// panic("HERE")
	} else if foo.Spec.Mode == "B" && foo.Status.State == "B-2" {
		foo.Status.State = "B-Final"
		updated = true
	}

	if updated {
		if err := r.Status().Update(ctx, &foo); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *BarReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&webappv1.Foo{}).
		Complete(r)
}
