package main

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes/scheme"

	_ "sigs.k8s.io/karpenter/pkg/apis/v1"       // register karpenter.sh/v1
	_ "sigs.k8s.io/karpenter/pkg/test/v1alpha1" // register karpenter.test.sh/v1alpha1
)

func newScheme() *runtime.Scheme {
	// Use the global scheme so Karpenter’s init() registrations are picked up.
	s := scheme.Scheme
	utilruntime.Must(corev1.AddToScheme(s))
	return s
}
