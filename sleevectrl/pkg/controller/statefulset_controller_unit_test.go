package controller

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestCreatePodForStatefulSetUsesTemplateLabels(t *testing.T) {
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "database",
			Namespace: "default",
			Labels:    map[string]string{"statefulset-only": "not-inherited"},
		},
		Spec: appsv1.StatefulSetSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"node-state": "Ready-to-Start"},
				},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "database"}}},
			},
		},
	}

	pod, err := createPodForStatefulSet(statefulSet, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := pod.Labels["node-state"]; got != "Ready-to-Start" {
		t.Fatalf("expected template label on Pod, got %q", got)
	}
	if _, found := pod.Labels["statefulset-only"]; found {
		t.Fatal("StatefulSet object label must not be inherited by Pod")
	}
}
