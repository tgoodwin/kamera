package main

import (
	"context"
	"fmt"
	"time"

	knativeharness "github.com/tgoodwin/kamera/examples/knative-serving/knative"
	"github.com/tgoodwin/kamera/pkg/replay"
	"github.com/tgoodwin/kamera/pkg/simclock"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/serving/pkg/apis/autoscaling"
	"knative.dev/serving/pkg/apis/serving"
	v1 "knative.dev/serving/pkg/apis/serving/v1"
	kpareconciler "knative.dev/serving/pkg/reconciler/autoscaling/kpa"
	"knative.dev/serving/pkg/reconciler/configuration"
	revisionreconciler "knative.dev/serving/pkg/reconciler/revision"
	routecontroller "knative.dev/serving/pkg/reconciler/route"
	serverlessservicecontroller "knative.dev/serving/pkg/reconciler/serverlessservice"
	servicecontroller "knative.dev/serving/pkg/reconciler/service"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const defaultMaxDepth = 100

func newKnativeExplorerAndState(maxDepth int, perfStats bool) (*tracecheck.Explorer, tracecheck.StateNode, error) {
	depth := maxDepth
	if depth <= 0 {
		depth = defaultMaxDepth
	}

	// Configure simclock to use 2s steps instead of 1s to speed up scale-to-zero simulation
	// (60s stable window + 30s grace period = 90s total, which is 45 steps at 2s/step)
	// Note: 2s matches the KPA ticker interval (tickInterval = 2s), so tickers work correctly
	simclock.Configure(time.Unix(0, 0), 2*time.Second)

	builder := tracecheck.NewExplorerBuilder(scheme)
	configureKnativeExplorer(builder)
	builder.WithMaxDepth(depth)
	if perfStats {
		builder.WithPerfStats()
	}

	explorer, err := builder.Build("standalone")
	if err != nil {
		return nil, tracecheck.StateNode{}, fmt.Errorf("build explorer: %w", err)
	}

	initialState := buildInitialKnativeState(builder)
	return explorer, initialState, nil
}

func buildInitialKnativeState(builder *tracecheck.ExplorerBuilder) tracecheck.StateNode {
	stateBuilder := builder.NewStateEventBuilder()
	svc := buildBaselineService()
	tag.AddSleeveObjectID(svc)
	serviceState := stateBuilder.AddTopLevelObject(svc, "ServiceReconciler")
	return mergeStateNodes(serviceState)
}

func buildBaselineService() *v1.Service {
	return &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "default",
		},
		Spec: v1.ServiceSpec{
			ConfigurationSpec: v1.ConfigurationSpec{
				Template: v1.RevisionTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Name: "kamera-test",
						Annotations: map[string]string{
							// Ensure KPA class annotation is present so the KPA reconciler processes the PA.
							autoscaling.ClassAnnotationKey: autoscaling.KPA,
							// Set InitialScale=1 to allow activation
							autoscaling.InitialScaleAnnotationKey: "1",
							// Set MinScale=0 to allow scale-to-zero
							autoscaling.MinScaleAnnotationKey: "0",
						},
					},
					Spec: v1.RevisionSpec{
						PodSpec: corev1.PodSpec{
							Containers: []corev1.Container{{
								Image: "dev.local/test", // this bypasses digest resolution
							}},
						},
					},
				},
			},
		},
	}
}

func configureKnativeExplorer(builder *tracecheck.ExplorerBuilder) {
	builder.WithCustomStrategy("RevisionReconciler", func(r replay.EffectRecorder) tracecheck.Strategy {
		factory := func(ctx context.Context, cmw configmap.Watcher) *controller.Impl {
			impl := revisionreconciler.NewController(ctx, cmw)
			overrideRevisionResolver(impl)
			return impl
		}
		strategy, err := knativeharness.NewKnativeStrategy(factory, r)
		if err != nil {
			panic(err)
		}
		strategy.SetLogger(logf.Log.WithName("RevisionReconciler"))
		return strategy
	}).For("serving.knative.dev/Revision")

	builder.WithCustomStrategy("KPA", func(r replay.EffectRecorder) tracecheck.Strategy {
		factory := func(ctx context.Context, cmw configmap.Watcher) *controller.Impl {
			// Create MultiScaler and wrap it to capture enqueues.
			// The wrapper uses the global async enqueue collector directly.
			baseMultiScaler := knativeharness.NewFakeMultiScaler(ctx.Done(), logging.FromContext(ctx))
			multiScaler := knativeharness.NewEnqueueCapturingDeciders(baseMultiScaler, "KPA")

			impl := kpareconciler.NewController(ctx, cmw, multiScaler)
			return impl
		}
		strategy, err := knativeharness.NewKnativeStrategy(factory, r, serving.RevisionUID)
		if err != nil {
			panic(fmt.Sprintf("NewKnativeStrategy() error = %v", err))
		}
		strategy.SetLogger(logf.Log.WithName("KPAReconciler"))
		return strategy
	}).For("autoscaling.internal.knative.dev/PodAutoscaler")

	builder.WithCustomStrategy("ServiceReconciler", func(r replay.EffectRecorder) tracecheck.Strategy {
		strategy, err := knativeharness.NewKnativeStrategy(servicecontroller.NewController, r)
		if err != nil {
			panic(fmt.Sprintf("NewKnativeStrategy() error = %v", err))
		}
		strategy.SetLogger(logf.Log.WithName("ServiceReconciler"))
		return strategy
	}).For("serving.knative.dev/Service")

	builder.WithCustomStrategy("RouteReconciler", func(r replay.EffectRecorder) tracecheck.Strategy {
		strategy, err := knativeharness.NewKnativeStrategy(routecontroller.NewController, r)
		if err != nil {
			panic(fmt.Sprintf("NewKnativeStrategy() error = %v", err))
		}
		strategy.SetLogger(logf.Log.WithName("RouteReconciler"))
		return strategy
	}).For("serving.knative.dev/Route").Watches("serving.knative.dev/Configuration", func(u *unstructured.Unstructured) []reconcile.Request {
		labels := u.GetLabels()
		svcName := labels[serving.ServiceLabelKey]
		if svcName == "" {
			return nil
		}

		return []reconcile.Request{
			{NamespacedName: client.ObjectKey{Namespace: u.GetNamespace(), Name: svcName}},
		}
	})

	builder.WithCustomStrategy("ServerlessServiceReconciler", func(r replay.EffectRecorder) tracecheck.Strategy {
		strategy, err := knativeharness.NewKnativeStrategy(serverlessservicecontroller.NewController, r)
		if err != nil {
			panic(fmt.Sprintf("NewKnativeStrategy() error = %v", err))
		}
		strategy.SetLogger(logf.Log.WithName("ServerlessServiceReconciler"))
		return strategy
	}).For("networking.internal.knative.dev/ServerlessService")

	builder.WithResourceDep("networking.internal.knative.dev/ServerlessService", "ServerlessServiceReconciler", "KPA")

	builder.WithCustomStrategy("ConfigurationReconciler", func(r replay.EffectRecorder) tracecheck.Strategy {
		strategy, err := knativeharness.NewKnativeStrategy(configuration.NewController, r)
		if err != nil {
			panic(err)
		}
		strategy.SetLogger(logf.Log.WithName("ConfigurationReconciler"))
		return strategy
	}).For("serving.knative.dev/Configuration")

	builder.WithResourceDep("Configuration", "ConfigurationReconciler", "RevisionReconciler")

	builder.WithReconciler("RevisionDigestStub", func(c client.Client) tracecheck.Reconciler {
		return &revisionDigestStub{Client: c}
	}).For("serving.knative.dev/Revision")

	builder.WithReconciler("IngressStatusStub", func(c client.Client) tracecheck.Reconciler {
		return &knativeharness.IngressStatusStub{Client: c}
	}).For("networking.internal.knative.dev/Ingress")

	// builder.WithResourceDep("serving.knative.dev/Revision", "RevisionDigestStub", "RevisionReconciler", "KPA", "ServiceReconciler")
	// builder.WithResourceDep("autoscaling.internal.knative.dev/PodAutoscaler", "KPA", "ServerlessServiceReconciler")
	// builder.WithResourceDep("autoscaling.internal.knative.dev/PodAutoscaler", "RevisionReconciler")
	// builder.WithResourceDep("serving.knative.dev/Service", "ServiceReconciler")
	// builder.WithResourceDep("serving.knative.dev/Configuration", "ServiceReconciler", "RevisionReconciler", "RouteReconciler")
	// builder.WithResourceDep("serving.knative.dev/Route", "RouteReconciler", "ServiceReconciler")
	// builder.WithResourceDep("networking.internal.knative.dev/Ingress", "IngressStatusStub", "RouteReconciler", "ServerlessServiceReconciler")
	// builder.WithResourceDep("apps/Deployment", "RevisionReconciler")
}
