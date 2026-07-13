package main

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	api "github.com/tgoodwin/kamera/examples/cass-operator/cassandra/v1beta1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSimulatedManagementAPITransport(t *testing.T) {
	tests := []struct {
		path string
		body string
	}{
		{path: "/api/v0/lifecycle/start", body: "{}"},
		{path: "/api/v0/metadata/endpoints", body: `{"entity":[]}`},
	}

	for _, test := range tests {
		t.Run(strings.TrimPrefix(test.path, "/"), func(t *testing.T) {
			request, err := http.NewRequest(http.MethodGet, "http://10.0.0.1:8080"+test.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			response, err := (simulatedManagementAPITransport{}).RoundTrip(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != http.StatusOK || string(body) != test.body {
				t.Fatalf("got status %d body %q", response.StatusCode, body)
			}
		})
	}
}

func TestCassPodLifecycleWaitsForOperatorStart(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{api.CassNodeState: "Ready-to-Start"}},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
				{Type: corev1.ContainersReady, Status: corev1.ConditionFalse},
				{Type: corev1.PodReady, Status: corev1.ConditionFalse},
			},
		},
	}

	step, _, err := (cassPodLifecycleFactory{}).NewStateMachine(pod).Advance(context.Background(), pod)
	if err != nil {
		t.Fatal(err)
	}
	assertConditionStatus(t, step.Conditions, corev1.ContainersReady, corev1.ConditionFalse)

	pod.Labels[api.CassNodeState] = "Starting"
	step, _, err = (cassPodLifecycleFactory{}).NewStateMachine(pod).Advance(context.Background(), pod)
	if err != nil {
		t.Fatal(err)
	}
	assertConditionStatus(t, step.Conditions, corev1.ContainersReady, corev1.ConditionTrue)
}

func assertConditionStatus(t *testing.T, conditions []corev1.PodCondition, kind corev1.PodConditionType, want corev1.ConditionStatus) {
	t.Helper()
	for _, condition := range conditions {
		if condition.Type == kind {
			if condition.Status != want {
				t.Fatalf("condition %s: got %s, want %s", kind, condition.Status, want)
			}
			return
		}
	}
	t.Fatalf("condition %s not found", kind)
}
