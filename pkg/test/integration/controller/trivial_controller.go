/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/go-logr/logr"
	webappv1 "github.com/tgoodwin/kamera/pkg/test/integration/api/v1"
)

var logger logr.Logger

// FooReconciler reconciles a Foo object
type TestReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=webapp.discrete.events,resources=foos,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=webapp.discrete.events,resources=foos/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=webapp.discrete.events,resources=foos/finalizers,verbs=update

func (r *TestReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger = log.FromContext(ctx)

	var foo webappv1.Foo
	if err := r.Get(ctx, req.NamespacedName, &foo); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	updated := false

	if foo.Status.State == "" {
		logger.V(2).Info("Setting state to A-1")
		foo.Status.State = "A-1"
		updated = true
	} else if foo.Status.State == "A-1" {
		logger.V(2).Info("Setting state to A-Final")
		foo.Status.State = "A-Final"
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
func (r *TestReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&webappv1.Foo{}).
		Complete(r)
}
