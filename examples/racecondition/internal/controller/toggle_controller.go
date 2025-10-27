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

	rcv1 "github.com/tgoodwin/kamera/examples/racecondition/api/v1"
)

type ToggleController struct {
	client.Client
	Scheme       *runtime.Scheme
	ControllerID string
}

func (r *ToggleController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	toggle := &rcv1.Toggle{}
	if err := r.Get(ctx, req.NamespacedName, toggle); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if toggle.Status.Owner == r.ControllerID {
		return ctrl.Result{}, nil
	}

	// If nobody owns it yet, or the desired owner matches us, claim it.
	if toggle.Status.Owner == "" || toggle.Spec.Desired == r.ControllerID {
		toggle.Status.Owner = r.ControllerID
		if err := r.Status().Update(ctx, toggle); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *ToggleController) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&rcv1.Toggle{}).
		Complete(r)
}
