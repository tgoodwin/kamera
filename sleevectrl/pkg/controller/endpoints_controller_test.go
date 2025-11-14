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
	"testing"

	"github.com/go-logr/logr/testr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func TestBuildEndpointSubsetsReadyPod(t *testing.T) {
	t.Parallel()

	reconciler := &EndpointsReconciler{}
	logger := testr.NewWithOptions(t, testr.Options{Verbosity: 1})

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc",
			Namespace: "default",
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "demo"},
			Ports: []corev1.ServicePort{
				{
					Name:       "http",
					Port:       80,
					TargetPort: intstr.FromInt(8080),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}

	pods := []corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod-1",
				Namespace: "default",
				Labels:    map[string]string{"app": "demo"},
			},
			Spec: corev1.PodSpec{
				NodeName: "node-a",
				Containers: []corev1.Container{
					{
						Name: "app",
						Ports: []corev1.ContainerPort{
							{
								Name:          "http",
								ContainerPort: 8080,
								Protocol:      corev1.ProtocolTCP,
							},
						},
					},
				},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				PodIP: "10.0.0.1",
				Conditions: []corev1.PodCondition{
					{
						Type:   corev1.PodReady,
						Status: corev1.ConditionTrue,
					},
				},
			},
		},
	}

	subsets := reconciler.buildEndpointSubsets(context.Background(), svc, pods, logger)

	if len(subsets) != 1 {
		t.Fatalf("expected 1 subset, got %d", len(subsets))
	}
	subset := subsets[0]
	if len(subset.Addresses) != 1 {
		t.Fatalf("expected 1 ready address, got %d", len(subset.Addresses))
	}
	if subset.Addresses[0].IP != "10.0.0.1" {
		t.Fatalf("expected address IP 10.0.0.1, got %s", subset.Addresses[0].IP)
	}
	if len(subset.Ports) != 1 {
		t.Fatalf("expected 1 port, got %d", len(subset.Ports))
	}
	if subset.Ports[0].Port != 8080 {
		t.Fatalf("expected resolved port 8080, got %d", subset.Ports[0].Port)
	}
}

func TestBuildEndpointSubsetsPublishNotReady(t *testing.T) {
	t.Parallel()

	reconciler := &EndpointsReconciler{}
	logger := testr.NewWithOptions(t, testr.Options{Verbosity: 1})

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc",
			Namespace: "default",
		},
		Spec: corev1.ServiceSpec{
			Selector:                 map[string]string{"app": "demo"},
			PublishNotReadyAddresses: true,
			Ports: []corev1.ServicePort{
				{
					Name:       "grpc",
					Port:       9090,
					TargetPort: intstr.FromString("grpc"),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}

	pods := []corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod-1",
				Namespace: "default",
				Labels:    map[string]string{"app": "demo"},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name: "app",
						Ports: []corev1.ContainerPort{
							{
								Name:          "grpc",
								ContainerPort: 50051,
								Protocol:      corev1.ProtocolTCP,
							},
						},
					},
				},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				PodIP: "10.0.0.2",
				Conditions: []corev1.PodCondition{
					{
						Type:   corev1.PodReady,
						Status: corev1.ConditionFalse,
					},
				},
			},
		},
	}

	subsets := reconciler.buildEndpointSubsets(context.Background(), svc, pods, logger)

	if len(subsets) != 1 {
		t.Fatalf("expected 1 subset, got %d", len(subsets))
	}
	subset := subsets[0]
	if len(subset.Addresses) != 0 {
		t.Fatalf("expected 0 ready addresses, got %d", len(subset.Addresses))
	}
	if len(subset.NotReadyAddresses) != 1 {
		t.Fatalf("expected 1 not-ready address, got %d", len(subset.NotReadyAddresses))
	}
	if subset.NotReadyAddresses[0].IP != "10.0.0.2" {
		t.Fatalf("expected address IP 10.0.0.2, got %s", subset.NotReadyAddresses[0].IP)
	}
	if subset.Ports[0].Port != 50051 {
		t.Fatalf("expected resolved port 50051, got %d", subset.Ports[0].Port)
	}
}

func TestBuildEndpointSubsetsSyntheticIP(t *testing.T) {
	t.Parallel()

	reconciler := &EndpointsReconciler{}
	logger := testr.NewWithOptions(t, testr.Options{Verbosity: 1})

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc",
			Namespace: "default",
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "demo"},
		},
	}

	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-1",
			Namespace: "default",
			Labels:    map[string]string{"app": "demo"},
			UID:       "12345",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}

	subsets := reconciler.buildEndpointSubsets(context.Background(), svc, []corev1.Pod{pod}, logger)
	if len(subsets) != 1 {
		t.Fatalf("expected 1 subset, got %d", len(subsets))
	}
	if len(subsets[0].Addresses) != 1 {
		t.Fatalf("expected 1 ready address, got %d", len(subsets[0].Addresses))
	}

	expectedIP := syntheticPodIPv4(&pod)
	if subsets[0].Addresses[0].IP != expectedIP {
		t.Fatalf("expected synthetic IP %s, got %s", expectedIP, subsets[0].Addresses[0].IP)
	}
}
