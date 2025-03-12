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
	"reflect"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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
// +kubebuilder:rbac:groups="",resources=endpoints,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Service object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.18.4/pkg/reconcile
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

	// Check if the Service is being deleted
	if !service.DeletionTimestamp.IsZero() {
		log.Info("Service is being deleted", "name", service.Name)
		return ctrl.Result{}, nil
	}

	// Get endpoints or create if they don't exist
	endpoints := &corev1.Endpoints{}
	err := r.Get(ctx, types.NamespacedName{Name: service.Name, Namespace: service.Namespace}, endpoints)
	if client.IgnoreNotFound(err) != nil {
		log.Error(err, "failed to get endpoints")
		return ctrl.Result{}, err
	}

	// If endpoints don't exist and this is not a headless service or ExternalName service, create them
	if errors.IsNotFound(err) && service.Spec.ClusterIP != "None" && service.Spec.Type != corev1.ServiceTypeExternalName {
		endpoints = &corev1.Endpoints{
			ObjectMeta: metav1.ObjectMeta{
				Name:      service.Name,
				Namespace: service.Namespace,
				OwnerReferences: []metav1.OwnerReference{
					*metav1.NewControllerRef(service, corev1.SchemeGroupVersion.WithKind("Service")),
				},
			},
		}
		if err := r.Create(ctx, endpoints); err != nil {
			log.Error(err, "failed to create empty endpoints")
			return ctrl.Result{}, err
		}
		log.Info("Created new Endpoints", "name", endpoints.Name)
	}

	// Skip headless and ExternalName services for endpoint management
	if service.Spec.ClusterIP == "None" || service.Spec.Type == corev1.ServiceTypeExternalName {
		return ctrl.Result{}, nil
	}

	// Get pods matching service selector
	if len(service.Spec.Selector) == 0 {
		// Service doesn't select any pods, keep endpoints empty
		if len(endpoints.Subsets) > 0 {
			endpoints.Subsets = nil
			if err := r.Update(ctx, endpoints); err != nil {
				log.Error(err, "failed to clear endpoints")
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// List pods matching the service selector
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList,
		client.InNamespace(service.Namespace),
		client.MatchingLabels(service.Spec.Selector)); err != nil {
		log.Error(err, "failed to list pods")
		return ctrl.Result{}, err
	}

	// Build new endpoint subsets from matching pods
	subsets := buildSubsets(service, podList.Items)

	// Update endpoints if necessary
	if !reflect.DeepEqual(endpoints.Subsets, subsets) {
		endpoints.Subsets = subsets
		if err := r.Update(ctx, endpoints); err != nil {
			log.Error(err, "failed to update endpoints")
			return ctrl.Result{}, err
		}
		log.Info("Updated Endpoints", "name", endpoints.Name)
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

// Helper function to build Endpoints subsets from pods
func buildSubsets(service *corev1.Service, pods []corev1.Pod) []corev1.EndpointSubset {
	if len(pods) == 0 {
		return nil
	}

	// Create address slice from ready pods
	addresses := []corev1.EndpointAddress{}
	for _, pod := range pods {
		if !isPodReady(&pod) || pod.DeletionTimestamp != nil || pod.Status.PodIP == "" {
			continue
		}

		address := corev1.EndpointAddress{
			IP: pod.Status.PodIP,
			TargetRef: &corev1.ObjectReference{
				Kind:            "Pod",
				Namespace:       pod.Namespace,
				Name:            pod.Name,
				UID:             pod.UID,
				ResourceVersion: pod.ResourceVersion,
			},
		}
		addresses = append(addresses, address)
	}

	if len(addresses) == 0 {
		return nil
	}

	// Create ports from service spec
	ports := []corev1.EndpointPort{}
	for _, servicePort := range service.Spec.Ports {
		endpointPort := corev1.EndpointPort{
			Name:     servicePort.Name,
			Port:     servicePort.Port,
			Protocol: servicePort.Protocol,
		}
		ports = append(ports, endpointPort)
	}

	return []corev1.EndpointSubset{
		{
			Addresses: addresses,
			Ports:     ports,
		},
	}
}

// isPodReady checks if a pod is in the Ready state
func isPodReady(pod *corev1.Pod) bool {
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
		// For().
		Complete(r)
}
