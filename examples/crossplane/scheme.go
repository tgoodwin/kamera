package main

import (
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"

	corev1 "k8s.io/api/core/v1"

	v1 "github.com/crossplane/crossplane/v2/apis/apiextensions/v1"
	pkgv1 "github.com/crossplane/crossplane/v2/apis/pkg/v1"
)

func newScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(v1.AddToScheme(scheme))
	utilruntime.Must(pkgv1.AddToScheme(scheme))
	return scheme
}
