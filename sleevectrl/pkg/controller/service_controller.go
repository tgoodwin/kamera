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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// ServiceReconciler reconciles a Service object
type ServiceReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=my.domain,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=my.domain,resources=services/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=my.domain,resources=services/finalizers,verbs=update

// For Service controller:
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services/status,verbs=get;update;patch
// Reconcile is part of the main kubernetes reconciliation loop.
func (r *ServiceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// Fetch the Service instance
	service := &corev1.Service{}
	if err := r.Get(ctx, req.NamespacedName, service); err != nil {
		if client.IgnoreNotFound(err) != nil {
			log.Error(err, "unable to fetch Service")
			return ctrl.Result{}, err
		}
		// Service not found, likely deleted, return
		return ctrl.Result{}, nil
	}
	service.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Service"))

	// Check if the Service is being deleted
	if !service.DeletionTimestamp.IsZero() {
		log.Info("Service is being deleted", "name", service.Name)
		return ctrl.Result{}, nil
	}

	// For LoadBalancer services, update status with external IP (simulated here)
	if service.Spec.Type == corev1.ServiceTypeLoadBalancer && len(service.Status.LoadBalancer.Ingress) == 0 {
		service.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{
			{
				IP: "10.0.0.1", // This would be dynamically assigned in a real controller
			},
		}
		if err := r.Status().Update(ctx, service); err != nil {
			log.Error(err, "failed to update service status")
			return ctrl.Result{}, err
		}
		log.Info("Updated Service status with LoadBalancer IP", "name", service.Name)
	}

	return ctrl.Result{}, nil
}

// isPodReady is shared by several reconcilers that need to filter pods.
func isPodReady(pod *corev1.Pod) bool {
	if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// SetupWithManager sets up the controller with the Manager.
func (r *ServiceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		// Uncomment the following line adding a pointer to an instance of the controlled resource as an argument
		For(&corev1.Service{}).
		Complete(r)
}
