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
	"math/rand/v2"
	"reflect"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	appsv1 "github.com/tgoodwin/sleeve/examples/chaos/api/v1"
)

func allServicesHealthy(states []appsv1.ServiceState) bool {
	for _, state := range states {
		if state.Health != "healthy" {
			return false
		}
	}
	return true
}

// OrchestrationReconciler reconciles a Orchestration object
type OrchestrationReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=apps.my.domain,resources=orchestrations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps.my.domain,resources=orchestrations/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps.my.domain,resources=orchestrations/finalizers,verbs=update
func (r *OrchestrationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = log.FromContext(ctx)

	var orch appsv1.Orchestration
	if err := r.Get(ctx, req.NamespacedName, &orch); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	orig := orch.DeepCopy()

	// Initialize ServiceStates if this is first time seeing the resource
	if len(orch.Status.ServiceStates) == 0 && len(orch.Spec.Services) > 0 {
		orch.Status.Phase = "preparing"
		orch.Status.Progress = 0
		orch.Status.ServiceStates = make([]appsv1.ServiceState, len(orch.Spec.Services))
		for i, svc := range orch.Spec.Services {
			orch.Status.ServiceStates[i] = appsv1.ServiceState{
				Name:     svc,
				Version:  "v1", // Could be derived from elsewhere
				Replicas: 0,
				Health:   "pending",
			}
		}
		if err := r.Status().Update(ctx, &orch); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil // Exit after initialization
	}

	// Each controller might make different decisions about:
	// - Which service to deploy/scale next
	// - How many replicas to adjust based on current state
	// - Whether to mark services as healthy/degraded
	// - Whether to progress to next phase

	for i, state := range orch.Status.ServiceStates {
		if state.Replicas < 3 {
			state.Replicas++
			if state.Replicas == 3 {
				state.Health = "healthy"
			}
			orch.Status.ServiceStates[i] = state
			orch.Status.Progress += 10
			orch.Status.Phase = "in-progress"
			break // Only handle one service per reconcile
		}
	}

	if allServicesHealthy(orch.Status.ServiceStates) {
		orch.Status.Phase = "completed"
	}

	if !reflect.DeepEqual(orig, &orch) {
		if err := r.Status().Update(ctx, &orch); err != nil {
			return ctrl.Result{}, err
		}
	}

	// TODO(user): your logic here

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *OrchestrationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1.Orchestration{}).
		Complete(r)
}

// Handles health checking and rollback decisions
type HealthReconciler struct {
	client.Client
	Name   string
	Scheme *runtime.Scheme
}

func (r *HealthReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var orch appsv1.Orchestration
	if err := r.Get(ctx, req.NamespacedName, &orch); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	original := orch.DeepCopy()

	if orch.Status.HealthCheckAttempts == nil {
		count := 0
		orch.Status.HealthCheckAttempts = &count
	}

	// May decide to mark services as degraded/failed
	// Could trigger rollbacks which affect other controllers
	if *orch.Status.HealthCheckAttempts < 3 {
		for i, state := range orch.Status.ServiceStates {
			if state.Replicas > 0 && rand.Float32() < 0.2 { // Simulate occasional health check failures
				state.Health = "degraded"
				orch.Status.ServiceStates[i] = state
				break
			}
		}
		*orch.Status.HealthCheckAttempts++
	}
	if !allServicesHealthy(orch.Status.ServiceStates) {
		orch.Status.Phase = "failed"
	}

	if !reflect.DeepEqual(original, &orch) {
		if err := r.Status().Update(ctx, &orch); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *HealthReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1.Orchestration{}).
		Complete(r)
}
