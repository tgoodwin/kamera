package scheme

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/runtime"

	cachingv1alpha1 "knative.dev/caching/pkg/apis/caching/v1alpha1"
	networkingv1alpha1 "knative.dev/networking/pkg/apis/networking/v1alpha1"
	autoscalingv1alpha1 "knative.dev/serving/pkg/apis/autoscaling/v1alpha1"
	servingv1 "knative.dev/serving/pkg/apis/serving/v1"
)

// Default holds the object types used by the kamera harness utilities.
var Default = runtime.NewScheme()

func init() {
	mustAddToScheme(corev1.AddToScheme)
	mustAddToScheme(appsv1.AddToScheme)
	mustAddToScheme(servingv1.AddToScheme)
	mustAddToScheme(autoscalingv1alpha1.AddToScheme)
	mustAddToScheme(cachingv1alpha1.AddToScheme)
	mustAddToScheme(networkingv1.AddToScheme)
	mustAddToScheme(networkingv1alpha1.AddToScheme)
}

func mustAddToScheme(fn func(*runtime.Scheme) error) {
	if err := fn(Default); err != nil {
		panic(fmt.Sprintf("registering scheme: %v", err))
	}
}
