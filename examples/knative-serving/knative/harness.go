package kamera

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"go.uber.org/zap"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/rest"
	testing "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	filteredinformerfactory "knative.dev/pkg/client/injection/kube/informers/factory/filtered"
	"knative.dev/pkg/injection"
	"knative.dev/pkg/injection/clients/dynamicclient"
	"knative.dev/pkg/reconciler"
	reconcilertesting "knative.dev/pkg/reconciler/testing"

	kamerascheme "github.com/tgoodwin/kamera/examples/knative-serving/knative/scheme"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/replay"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tgoodwin/kamera/pkg/tag"
	cachingv1alpha1 "knative.dev/caching/pkg/apis/caching/v1alpha1"
	fakecachingclient "knative.dev/caching/pkg/client/injection/client/fake"
	netv1alpha1 "knative.dev/networking/pkg/apis/networking/v1alpha1"
	fakenetworkingclient "knative.dev/networking/pkg/client/injection/client/fake"
	fakekubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	v1 "knative.dev/serving/pkg/apis/serving/v1"
	autoscalercfg "knative.dev/serving/pkg/autoscaler/config"
	"knative.dev/serving/pkg/autoscaler/scaling"
	fakeservingclient "knative.dev/serving/pkg/client/injection/client/fake"
	"knative.dev/serving/pkg/gc"
	"knative.dev/serving/pkg/reconciler/route/config"

	netcfg "knative.dev/networking/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/client"
	log "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	// Knative pkg imports
	// Knative controller plumbing
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/system"

	sksinformers "knative.dev/networking/pkg/client/injection/informers/networking/v1alpha1/serverlessservice"
	deploymentinformers "knative.dev/pkg/client/injection/kube/informers/apps/v1/deployment"
	endpointsinformers "knative.dev/pkg/client/injection/kube/informers/core/v1/endpoints"
	serviceinformers "knative.dev/pkg/client/injection/kube/informers/core/v1/service"
	autoscalingv1alpha1 "knative.dev/serving/pkg/apis/autoscaling/v1alpha1"
	cfgmap "knative.dev/serving/pkg/apis/config"
	podscalableinformer "knative.dev/serving/pkg/client/injection/ducks/autoscaling/v1alpha1/podscalable"
	painformers "knative.dev/serving/pkg/client/injection/informers/autoscaling/v1alpha1/podautoscaler"
)

// Ensure KnativeStrategy implements the Strategy interface
var _ tracecheck.Strategy = (*KnativeStrategy)(nil)

// ControllerFactory is a function that creates a new controller.
type ControllerFactory func(ctx context.Context, cmw configmap.Watcher) *controller.Impl

// KnativeStrategy implements the Strategy interface for Knative controllers.
type KnativeStrategy struct {
	factory   ControllerFactory
	recorder  replay.EffectRecorder
	selectors []string
	logger    logr.Logger
}

type fakeUniScaler struct {
	mu       sync.RWMutex
	desired  int32
	excessBC int32
}

func newFakeUniScaler(decider *scaling.Decider) *fakeUniScaler {
	desired := decider.Spec.InitialScale
	return &fakeUniScaler{
		desired:  desired,
		excessBC: 0,
	}
}

func (f *fakeUniScaler) Scale(_ *zap.SugaredLogger, _ time.Time) scaling.ScaleResult {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return scaling.ScaleResult{
		DesiredPodCount:     f.desired,
		ExcessBurstCapacity: f.excessBC,
		ScaleValid:          true,
	}
}

func (f *fakeUniScaler) Update(spec *scaling.DeciderSpec) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.desired = spec.InitialScale
}

// NewFakeMultiScaler constructs a MultiScaler suitable for offline simulations.
func NewFakeMultiScaler(stopCh <-chan struct{}, logger *zap.SugaredLogger) *scaling.MultiScaler {
	return scaling.NewMultiScaler(stopCh, func(decider *scaling.Decider) (scaling.UniScaler, error) {
		return newFakeUniScaler(decider), nil
	}, logger)
}

// NewKnativeStrategy creates a new KnativeStrategy for a given controller factory.
func NewKnativeStrategy(factory ControllerFactory, recorder replay.EffectRecorder, selectors ...string) (*KnativeStrategy, error) {
	if factory == nil {
		return nil, fmt.Errorf("controller factory cannot be nil")
	}

	cmw := configmap.NewStaticWatcher(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cfgmap.FeaturesConfigName,
			Namespace: system.Namespace(),
		},
		Data: map[string]string{},
	}, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cfgmap.DefaultsConfigName,
			Namespace: system.Namespace(),
		},
		Data: map[string]string{},
	}, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      autoscalercfg.ConfigName,
			Namespace: system.Namespace(),
		},
		Data: map[string]string{},
	},
		// added the following for route reconciler
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      config.DomainConfigName,
				Namespace: system.Namespace(),
			},
			Data: map[string]string{
				"test-domain.dev": "",
				"prod-domain.com": "selector:\n  app: prod",
			},
		}, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      netcfg.ConfigMapName,
				Namespace: system.Namespace(),
			},
			Data: map[string]string{},
		}, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      gc.ConfigName,
				Namespace: system.Namespace(),
			},
			Data: map[string]string{},
		}, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      cfgmap.FeaturesConfigName,
				Namespace: system.Namespace(),
			},
			Data: map[string]string{},
		},
		// added for revision informer
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "config-observability",
				Namespace: system.Namespace(),
			},
			Data: map[string]string{},
		},
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "config-deployment",
				Namespace: system.Namespace(),
			},
			Data: map[string]string{
				"queue-sidecar-image": "gcr.io/knative-releases/knative.dev/serving/cmd/queue@sha256:abc123",
			},
		},
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "config-logging",
				Namespace: system.Namespace(),
			},
			Data: map[string]string{},
		},

		// added for certificate reconciler
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "config-certmanager",
				Namespace: system.Namespace(),
			},
			Data: map[string]string{},
		},
	)

	partial := func(ctx context.Context, _ configmap.Watcher) *controller.Impl {
		return factory(ctx, cmw)
	}

	return &KnativeStrategy{
		factory:   partial,
		selectors: selectors,
		recorder:  recorder,
		logger:    log.Log.WithName("knative-strategy"),
	}, nil
}

// SetLogger overrides the default logger used by the strategy. Call this before PrepareState.
func (ks *KnativeStrategy) SetLogger(logger logr.Logger) {
	if logger.GetSink() == nil {
		return
	}
	ks.logger = logger
}

// PrepareState sets up the fake clients and informers for the reconciler under test.
func (ks *KnativeStrategy) PrepareState(ctx context.Context, state []runtime.Object) (context.Context, func(), error) {
	ctx = log.IntoContext(ctx, ks.logger)
	ctx, cancel, err := setupClientState(ctx, state, ks.selectors...)
	if err != nil {
		return nil, cancel, err
	}
	ctx = log.IntoContext(ctx, ks.logger)
	return ctx, cancel, nil
}

// newReactor creates a new reactor function that intercepts client actions,
// records them as effects, and uses the provided trackers to fetch object states.
func newReactor(ctx context.Context, recorder replay.EffectRecorder, trackers ...testing.ObjectTracker) testing.ReactionFunc {
	baseLogger := log.FromContext(ctx).WithName("fake-reactor")

	return func(action testing.Action) (handled bool, ret runtime.Object, err error) {
		var obj runtime.Object
		var op event.OperationType
		resource := action.GetResource().Resource
		logger := baseLogger.WithValues(
			"verb", action.GetVerb(),
			"resource", resource,
			"namespace", action.GetNamespace(),
		)

		// lookup iterates through all provided trackers to find the object.
		lookup := func(res schema.GroupVersionResource, ns, name string) (runtime.Object, error) {
			for _, tracker := range trackers {
				obj, err := tracker.Get(res, ns, name)
				if err == nil {
					return obj, nil
				}
			}
			// If we didn't find it in any tracker, return a generic error.
			return nil, fmt.Errorf("object %s/%s not found in any tracker for resource %v", ns, name, res)
		}

		switch action.GetVerb() {
		case "get":
			a := action.(testing.GetAction)
			obj, err = lookup(a.GetResource(), a.GetNamespace(), a.GetName())
			op = event.GET
		case "list":
			a := action.(testing.ListAction)
			gvr := a.GetResource()
			gvk := schema.GroupVersionKind{
				Group:   gvr.Group,
				Version: gvr.Version,
				Kind:    listKindForResource(gvr.Resource),
			}
			ul := &unstructured.Unstructured{}
			ul.SetGroupVersionKind(gvk)
			obj = ul
			op = event.LIST
			if resource == "serverlessservices" {
				logger.Info("listing serverlessservices", "namespace", a.GetNamespace())
			}
		case "create":
			a := action.(testing.CreateAction)
			obj = a.GetObject()
			op = event.CREATE
			if resource == "podautoscalers" {
				fmt.Printf("[reactor] CREATE podautoscaler %s/%s\n", action.GetNamespace(), a.GetObject().(client.Object).GetName())
			}
		case "update":
			a := action.(testing.UpdateAction)
			obj = a.GetObject()
			op = event.UPDATE
		case "delete":
			a := action.(testing.DeleteAction)
			obj, err = lookup(a.GetResource(), a.GetNamespace(), a.GetName())
			op = event.MARK_FOR_DELETION
		case "patch":
			a := action.(testing.PatchAction)
			obj, err = lookup(a.GetResource(), a.GetNamespace(), a.GetName())
			op = event.PATCH
		default:
			logger.V(1).Info("unhandled action type")
			return false, nil, nil
		}

		if err == nil && obj != nil {
			if co, ok := obj.(client.Object); ok {
				if _, isEvent := co.(*corev1.Event); isEvent {
					logger.V(1).Info("skipping event recording", "operation", op, "name", co.GetName())
					return false, nil, err
				}
				ensureGVK(co)
				if tag.GetSleeveObjectID(co) == "" {
					tag.AddSleeveObjectID(co)
				}
				// TODO determine if we need this
				// syncInformerCache(ctx, resource, co, op)
				if op == event.CREATE && resource == "serverlessservices" {
					logger.Info("observed serverlessservice create", "name", co.GetName())
					fmt.Printf("[reactor] CREATE serverlessservice %s/%s\n", action.GetNamespace(), co.GetName())
				}
				logger.V(1).Info("recording effect",
					"operation", op,
					"name", co.GetName(),
					"kind", co.GetObjectKind().GroupVersionKind().Kind,
				)
				recorder.RecordEffect(ctx, co, op, nil)
			} else {
				logger.V(1).Info("object does not implement client.Object", "operation", op, "type", fmt.Sprintf("%T", obj))
			}
		} else if err != nil {
			logger.V(1).Info("failed to resolve object for action", "error", err)
		}

		// Return false to let the default reactor handle the action.
		return false, nil, err
	}
}

func syncInformerCache(ctx context.Context, resource string, obj client.Object, op event.OperationType) {
	var (
		indexer cache.Indexer
	)

	switch resource {
	case "deployments":
		indexer = deploymentinformers.Get(ctx).Informer().GetIndexer()
	case "podautoscalers":
		indexer = painformers.Get(ctx).Informer().GetIndexer()
	case "endpoints":
		indexer = endpointsinformers.Get(ctx).Informer().GetIndexer()
	case "services":
		indexer = serviceinformers.Get(ctx).Informer().GetIndexer()
	case "serverlessservices":
		indexer = sksinformers.Get(ctx).Informer().GetIndexer()
	default:
		return
	}

	copy := obj.DeepCopyObject().(client.Object)
	logger := log.FromContext(ctx).WithName("cache-sync").WithValues(
		"resource", resource,
		"name", copy.GetName(),
		"namespace", copy.GetNamespace(),
	)
	if anns := copy.GetAnnotations(); len(anns) > 0 {
		logger = logger.WithValues("annotations", anns)
	}

	var err error
	switch op {
	case event.CREATE:
		err = indexer.Add(copy)
		logger.Info("added object to informer cache")
	case event.UPDATE:
		err = indexer.Update(copy)
		logger.Info("updated object in informer cache")
	case event.MARK_FOR_DELETION:
		err = indexer.Delete(copy)
		logger.Info("deleted object from informer cache")
	default:
		return
	}

	syncDynamicClient(ctx, resource, copy, op, logger)
	switch resource {
	case "deployments":
		if dep, ok := copy.(*appsv1.Deployment); ok {
			syncPodScalableInformer(ctx, dep, op, logger)
		}
	case "services":
		fmt.Printf("[syncInformerCache] service op=%s type=%T name=%s/%s\n", op, copy, copy.GetNamespace(), copy.GetName())
	case "serverlessservices":
		fmt.Printf("[syncInformerCache] serverlessservice op=%s name=%s/%s\n", op, copy.GetNamespace(), copy.GetName())
	}

	if err != nil {
		logger.Error(err, "failed to sync informer cache")
	}
}

func syncDynamicClient(ctx context.Context, resource string, obj client.Object, op event.OperationType, logger logr.Logger) {
	dc := dynamicclient.Get(ctx)
	gvr, ok := resourceToDynamicGVR[resource]
	if !ok {
		return
	}

	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj.DeepCopyObject())
	if err != nil {
		logger.WithValues("stage", "toUnstructured").Error(err, "failed to convert object for dynamic client")
		return
	}

	unstr := &unstructured.Unstructured{Object: content}
	unstr.SetGroupVersionKind(obj.GetObjectKind().GroupVersionKind())
	res := dc.Resource(gvr).Namespace(obj.GetNamespace())

	switch op {
	case event.CREATE:
		loggerWithStage := logger.WithValues("stage", "dynamic-create")
		if _, err = res.Create(ctx, unstr, metav1.CreateOptions{}); err != nil {
			if !apierrs.IsAlreadyExists(err) {
				loggerWithStage.Error(err, "failed to create object in dynamic client")
			}
		} else {
			loggerWithStage.Info("created object in dynamic client")
		}
	case event.UPDATE:
		loggerWithStage := logger.WithValues("stage", "dynamic-update")
		if _, err = res.Update(ctx, unstr, metav1.UpdateOptions{}); err != nil {
			loggerWithStage.Error(err, "failed to update object in dynamic client")
		} else {
			loggerWithStage.Info("updated object in dynamic client")
		}
	case event.MARK_FOR_DELETION:
		loggerWithStage := logger.WithValues("stage", "dynamic-delete")
		if err = res.Delete(ctx, obj.GetName(), metav1.DeleteOptions{}); err != nil {
			if !apierrs.IsNotFound(err) {
				loggerWithStage.Error(err, "failed to delete object from dynamic client")
			}
		} else {
			loggerWithStage.Info("deleted object from dynamic client")
		}
	}
}

func syncPodScalableInformer(ctx context.Context, dep *appsv1.Deployment, op event.OperationType, logger logr.Logger) {
	factory := podscalableinformer.Get(ctx)
	gvr := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}

	inf, _, err := factory.Get(ctx, gvr)
	if err != nil {
		logger.WithValues("stage", "podscalable-get").Error(err, "failed to get podscalable informer")
		return
	}

	ps := makePodScalableFromDeployment(dep)
	indexer := inf.GetIndexer()

	switch op {
	case event.CREATE:
		if err := indexer.Add(ps); err != nil && !apierrs.IsAlreadyExists(err) {
			logger.WithValues("stage", "podscalable-add").Error(err, "failed to add podscalable to informer")
		} else {
			logger.WithValues("stage", "podscalable-add", "keys", indexer.ListKeys()).Info("added podscalable to informer")
		}
	case event.UPDATE:
		if err := indexer.Update(ps); err != nil {
			logger.WithValues("stage", "podscalable-update").Error(err, "failed to update podscalable in informer")
		} else {
			logger.WithValues("stage", "podscalable-update", "keys", indexer.ListKeys()).Info("updated podscalable in informer")
		}
	case event.MARK_FOR_DELETION:
		if err := indexer.Delete(ps); err != nil && !apierrs.IsNotFound(err) {
			logger.WithValues("stage", "podscalable-delete").Error(err, "failed to delete podscalable from informer")
		} else {
			logger.WithValues("stage", "podscalable-delete", "keys", indexer.ListKeys()).Info("deleted podscalable from informer")
		}
	}

	_, lister, err := factory.Get(ctx, gvr)
	if err == nil {
		if obj, getErr := lister.ByNamespace(dep.Namespace).Get(dep.Name); getErr == nil {
			logger.WithValues("stage", "podscalable-lister").Info("podscalable visible to lister", "object", obj)
		} else {
			logger.WithValues("stage", "podscalable-lister").Error(getErr, "podscalable not visible to lister")
		}
	} else {
		logger.WithValues("stage", "podscalable-lister").Error(err, "failed to retrieve lister")
	}
}

func makePodScalableFromDeployment(dep *appsv1.Deployment) *autoscalingv1alpha1.PodScalable {
	ps := &autoscalingv1alpha1.PodScalable{
		TypeMeta: metav1.TypeMeta{
			APIVersion: autoscalingv1alpha1.SchemeGroupVersion.String(),
			Kind:       "PodScalable",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        dep.Name,
			Namespace:   dep.Namespace,
			Labels:      dep.Labels,
			Annotations: dep.Annotations,
		},
		Status: autoscalingv1alpha1.PodScalableStatus{
			Replicas: dep.Status.Replicas,
		},
	}

	if dep.Spec.Replicas != nil {
		rep := *dep.Spec.Replicas
		ps.Spec.Replicas = &rep
	}
	if dep.Spec.Selector != nil {
		ps.Spec.Selector = dep.Spec.Selector.DeepCopy()
	}
	ps.Spec.Template = *dep.Spec.Template.DeepCopy()
	return ps
}

// ReconcileAtState invokes the reconciler for a given state.
func (ks *KnativeStrategy) ReconcileAtState(ctx context.Context, nsName types.NamespacedName) (reconcile.Result, error) {
	servingClient := fakeservingclient.Get(ctx)
	kubeClient := fakekubeclient.Get(ctx)
	cachingClient := fakecachingclient.Get(ctx)
	networkingClient := fakenetworkingclient.Get(ctx)

	logger := log.FromContext(ctx).WithName("reconcile").WithValues("key", nsName.String())

	// Create a reactor and attach it to both clients to intercept and record actions.
	reactor := newReactor(ctx, ks.recorder,
		servingClient.Tracker(),
		kubeClient.Tracker(),
		cachingClient.Tracker(),
		networkingClient.Tracker())

	// Add the reactor to both clients.
	servingClient.PrependReactor("*", "*", reactor)
	kubeClient.PrependReactor("*", "*", reactor)
	cachingClient.PrependReactor("*", "*", reactor)
	networkingClient.PrependReactor("*", "*", reactor)

	// must re-initialize the controller each time to reset its informer state
	ctrl := ks.factory(ctx, nil)
	logger = logger.WithValues("reconciler", ctrl.Name)
	if la, ok := ctrl.Reconciler.(reconciler.LeaderAware); ok {
		la.Promote(reconciler.UniversalBucket(), func(reconciler.Bucket, types.NamespacedName) {})
	} else {
		logger.Error(fmt.Errorf("not leader-aware"), "reconcile aborted")
		return reconcile.Result{}, fmt.Errorf("Reconciler is not leader-aware")
	}

	key := nsName.String()
	err := ctrl.Reconciler.Reconcile(ctx, key)

	if err != nil {
		errMsg := err.Error()
		fmt.Printf("[ReconcileAtState] error type=%T err=%v\n", err, errMsg)
		if apierrs.IsNotFound(err) || strings.Contains(errMsg, " not found") {
			logger.Info("transient not found; requeueing", "error", err)
			return reconcile.Result{Requeue: true}, nil
		}

		requeue, requeueAfter := controller.IsRequeueKey(err)
		if !requeue {
			logger.Error(err, "reconcile failed")
			return reconcile.Result{}, err // Return actual error if it's not a requeue request
		}

		logger.Info("reconcile completed", "requeue", true, "requeueAfter", requeueAfter)
		return reconcile.Result{Requeue: true, RequeueAfter: requeueAfter}, nil
	}

	logger.Info("reconcile completed", "requeue", false, "requeueAfter", 0)
	return reconcile.Result{}, nil
}

func setupClientState(ctx context.Context, state []runtime.Object, selectors ...string) (context.Context, func(), error) {
	ctx, cancel := context.WithCancel(ctx)
	ctx = filteredinformerfactory.WithSelectors(ctx, selectors...)
	ctx = injection.WithConfig(ctx, &rest.Config{})
	ctx, informers := injection.Fake.SetupInformers(ctx, &rest.Config{})

	logger := log.FromContext(ctx).WithName("setup")
	type informerMeta struct {
		typeName string
		informer controller.Informer
	}
	metas := make([]informerMeta, len(informers))

	for idx, informer := range informers {
		typeName := fmt.Sprintf("%T", informer)
		logger.Info("registered informer", "index", idx, "type", typeName)
		metas[idx] = informerMeta{typeName: typeName, informer: informer}
	}

	if err := insertObjects(ctx, state); err != nil {
		return nil, cancel, err
	}

	if err := ensureSystemResources(ctx); err != nil {
		return nil, cancel, err
	}

	waitInformers, err := reconcilertesting.RunAndSyncInformers(ctx, informers...)
	if err != nil {
		logger.Error(err, "RunAndSyncInformers failed")
		for idx, meta := range metas {
			logger.Error(err, "informer sync status", "index", idx, "type", meta.typeName, "synced", meta.informer.HasSynced())
		}
		cancel()
		return nil, nil, fmt.Errorf("failed to sync informers: %w", err)
	}

	return ctx, func() {
		cancel()
		waitInformers()
	}, nil
}

func insertObjects(ctx context.Context, objs []runtime.Object) error {
	servingclient := fakeservingclient.Get(ctx)
	kubeclient := fakekubeclient.Get(ctx)
	cachingclient := fakecachingclient.Get(ctx)
	networkingclient := fakenetworkingclient.Get(ctx)

	// i am sorry for the following code
	for _, obj := range objs {
		if u, ok := obj.(*unstructured.Unstructured); ok {
			typed, err := convertUnstructured(u)
			if err != nil {
				return fmt.Errorf("failed to convert unstructured %s: %w", u.GroupVersionKind().String(), err)
			}
			obj = typed
		}

		// if o, ok := obj.(client.Object); ok {
		// 	fmt.Printf("[insertObjects] type=%T name=%s/%s\n", obj, o.GetNamespace(), o.GetName())
		// }

		switch o := obj.(type) {
		case *v1.Service:
			if _, err := servingclient.ServingV1().Services(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create service: %w", err)
			}
		case *v1.Route:
			if _, err := servingclient.ServingV1().Routes(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create route: %w", err)
			}
		case *v1.Configuration:
			if _, err := servingclient.ServingV1().Configurations(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create configuration: %w", err)
			}
		case *v1.Revision:
			if _, err := servingclient.ServingV1().Revisions(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create revision: %w", err)
			}
		case *autoscalingv1alpha1.PodAutoscaler:
			if _, err := servingclient.AutoscalingV1alpha1().PodAutoscalers(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create podautoscaler: %w", err)
			}
		case *autoscalingv1alpha1.Metric:
			if _, err := servingclient.AutoscalingV1alpha1().Metrics(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create metric: %w", err)
			}
		case *netv1alpha1.ServerlessService:
			if _, err := networkingclient.NetworkingV1alpha1().ServerlessServices(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create serverlessservice: %w", err)
			}
		case *netv1alpha1.Ingress:
			if _, err := networkingclient.NetworkingV1alpha1().Ingresses(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create knative ingress: %w", err)
			}
		case *corev1.ConfigMap:
			if _, err := kubeclient.CoreV1().ConfigMaps(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create configmap: %w", err)
			}
		case *corev1.Secret:
			if _, err := kubeclient.CoreV1().Secrets(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create secret: %w", err)
			}
		case *corev1.ServiceAccount:
			if _, err := kubeclient.CoreV1().ServiceAccounts(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create serviceaccount: %w", err)
			}
		case *corev1.Pod:
			if _, err := kubeclient.CoreV1().Pods(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create pod: %w", err)
			}
		case *corev1.Endpoints:
			if _, err := kubeclient.CoreV1().Endpoints(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create endpointss: %w", err)
			}
		case *corev1.Service:
			if _, err := kubeclient.CoreV1().Services(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create service: %w", err)
			}
		case *appsv1.Deployment:
			if _, err := kubeclient.AppsV1().Deployments(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create deployment: %w", err)
			}
			ensureGVK(o)
			logger := log.FromContext(ctx).WithName("seed").WithValues("resource", "deployments", "namespace", o.Namespace, "name", o.Name)
			syncDynamicClient(ctx, "deployments", o, event.CREATE, logger)
			syncPodScalableInformer(ctx, o, event.CREATE, logger)
		case *appsv1.ReplicaSet:
			if _, err := kubeclient.AppsV1().ReplicaSets(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create replicaset: %w", err)
			}
		case *cachingv1alpha1.Image:
			if _, err := cachingclient.CachingV1alpha1().Images(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create image: %w", err)
			}
		case *networkingv1.Ingress:
			if _, err := kubeclient.NetworkingV1().Ingresses(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create ingress: %w", err)
			}
			ensureGVK(o)
			logger := log.FromContext(ctx).WithName("seed").WithValues("resource", "ingresses", "namespace", o.Namespace, "name", o.Name)
			syncDynamicClient(ctx, "ingresses", o, event.CREATE, logger)
		default:
			return fmt.Errorf("unsupported type %T", o)
		}
	}
	return nil

}

func ensureSystemResources(ctx context.Context) error {
	kubeclient := fakekubeclient.Get(ctx)
	ns := system.Namespace()

	activatorSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "activator-service",
			Namespace: ns,
			Labels: map[string]string{
				"app": "activator",
			},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"app": "activator",
			},
			Ports: []corev1.ServicePort{
				{Name: "http", Port: 80, TargetPort: intstr.FromInt(8012)},
				{Name: "http2", Port: 81, TargetPort: intstr.FromInt(8013)},
				{Name: "https", Port: 443, TargetPort: intstr.FromInt(8112)},
				{Name: "http-metrics", Port: 9090, TargetPort: intstr.FromInt(9090)},
				{Name: "http-profiling", Port: 8008, TargetPort: intstr.FromInt(8008)},
			},
		},
	}
	ensureGVK(activatorSvc)
	if _, err := kubeclient.CoreV1().Services(ns).Create(ctx, activatorSvc, metav1.CreateOptions{}); err != nil && !apierrs.IsAlreadyExists(err) {
		return fmt.Errorf("failed to seed activator service: %w", err)
	}

	activatorEndpoints := &corev1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "activator-service",
			Namespace: ns,
		},
		Subsets: []corev1.EndpointSubset{{
			Addresses: []corev1.EndpointAddress{{
				IP:       "10.0.0.1",
				Hostname: "activator-0",
			}},
			Ports: []corev1.EndpointPort{
				{Name: "http", Port: 8012, Protocol: corev1.ProtocolTCP},
				{Name: "http2", Port: 8013, Protocol: corev1.ProtocolTCP},
				{Name: "https", Port: 8112, Protocol: corev1.ProtocolTCP},
				{Name: "http-metrics", Port: 9090, Protocol: corev1.ProtocolTCP},
				{Name: "http-profiling", Port: 8008, Protocol: corev1.ProtocolTCP},
			},
		}},
	}
	ensureGVK(activatorEndpoints)
	if _, err := kubeclient.CoreV1().Endpoints(ns).Create(ctx, activatorEndpoints, metav1.CreateOptions{}); err != nil && !apierrs.IsAlreadyExists(err) {
		return fmt.Errorf("failed to seed activator endpoints: %w", err)
	}

	return nil
}

var kindToGVK = map[string]schema.GroupVersionKind{
	"Deployment":        appsv1.SchemeGroupVersion.WithKind("Deployment"),
	"ReplicaSet":        appsv1.SchemeGroupVersion.WithKind("ReplicaSet"),
	"Image":             cachingv1alpha1.SchemeGroupVersion.WithKind("Image"),
	"PodAutoscaler":     autoscalingv1alpha1.SchemeGroupVersion.WithKind("PodAutoscaler"),
	"Metric":            autoscalingv1alpha1.SchemeGroupVersion.WithKind("Metric"),
	"Configuration":     v1.SchemeGroupVersion.WithKind("Configuration"),
	"Revision":          v1.SchemeGroupVersion.WithKind("Revision"),
	"Route":             v1.SchemeGroupVersion.WithKind("Route"),
	"Service":           v1.SchemeGroupVersion.WithKind("Service"),
	"ServerlessService": netv1alpha1.SchemeGroupVersion.WithKind("ServerlessService"),
	"Ingress":           networkingv1.SchemeGroupVersion.WithKind("Ingress"),
}

var resourceToListKind = map[string]string{
	"deployments":        "DeploymentList",
	"replicasets":        "ReplicaSetList",
	"images":             "ImageList",
	"podautoscalers":     "PodAutoscalerList",
	"metrics":            "MetricList",
	"configurations":     "ConfigurationList",
	"revisions":          "RevisionList",
	"routes":             "RouteList",
	"services":           "ServiceList",
	"serverlessservices": "ServerlessServiceList",
	"pods":               "PodList",
	"endpoints":          "EndpointsList",
	"configmaps":         "ConfigMapList",
	"secrets":            "SecretList",
	"serviceaccounts":    "ServiceAccountList",
	"ingresses":          "IngressList",
}

var resourceToDynamicGVR = map[string]schema.GroupVersionResource{
	"deployments":        {Group: "apps", Version: "v1", Resource: "deployments"},
	"services":           {Group: "", Version: "v1", Resource: "services"},
	"serverlessservices": {Group: "networking.internal.knative.dev", Version: "v1alpha1", Resource: "serverlessservices"},
	"ingresses":          {Group: "networking.k8s.io", Version: "v1", Resource: "ingresses"},
}

func ensureGVK(obj client.Object) {
	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Kind != "" && gvk.Version != "" {
		return
	}
	if gvks, _, err := kamerascheme.Default.ObjectKinds(obj); err == nil && len(gvks) > 0 {
		for _, candidate := range gvks {
			if candidate.Kind != "" && candidate.Version != "" {
				obj.GetObjectKind().SetGroupVersionKind(candidate)
				return
			}
		}
	}
	switch o := obj.(type) {
	case *corev1.ConfigMap:
		o.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("ConfigMap"))
	case *corev1.Secret:
		o.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Secret"))
	case *corev1.ServiceAccount:
		o.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("ServiceAccount"))
	case *corev1.Pod:
		o.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Pod"))
	case *corev1.Endpoints:
		o.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Endpoints"))
	case *corev1.Service:
		o.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Service"))
	case *networkingv1.Ingress:
		o.SetGroupVersionKind(networkingv1.SchemeGroupVersion.WithKind("Ingress"))
	case *netv1alpha1.Ingress:
		o.SetGroupVersionKind(netv1alpha1.SchemeGroupVersion.WithKind("Ingress"))
	case *appsv1.Deployment:
		o.SetGroupVersionKind(appsv1.SchemeGroupVersion.WithKind("Deployment"))
	case *appsv1.ReplicaSet:
		o.SetGroupVersionKind(appsv1.SchemeGroupVersion.WithKind("ReplicaSet"))
	case *v1.Service:
		o.SetGroupVersionKind(v1.SchemeGroupVersion.WithKind("Service"))
	case *v1.Route:
		o.SetGroupVersionKind(v1.SchemeGroupVersion.WithKind("Route"))
	case *v1.Configuration:
		o.SetGroupVersionKind(v1.SchemeGroupVersion.WithKind("Configuration"))
	case *v1.Revision:
		o.SetGroupVersionKind(v1.SchemeGroupVersion.WithKind("Revision"))
	case *autoscalingv1alpha1.PodAutoscaler:
		o.SetGroupVersionKind(autoscalingv1alpha1.SchemeGroupVersion.WithKind("PodAutoscaler"))
	case *autoscalingv1alpha1.Metric:
		o.SetGroupVersionKind(autoscalingv1alpha1.SchemeGroupVersion.WithKind("Metric"))
	case *cachingv1alpha1.Image:
		o.SetGroupVersionKind(cachingv1alpha1.SchemeGroupVersion.WithKind("Image"))
	}
}

func convertUnstructured(u *unstructured.Unstructured) (runtime.Object, error) {
	gvk := u.GroupVersionKind()
	if gvk.Empty() {
		if kind := u.GetKind(); kind != "" {
			if apiVersion := u.GetAPIVersion(); apiVersion != "" {
				if gv, err := schema.ParseGroupVersion(apiVersion); err == nil {
					gvk = gv.WithKind(kind)
				}
			}
		}
	}
	if (gvk.Group == "" || gvk.Kind == "") && u.GetKind() != "" {
		if mapped, ok := kindToGVK[u.GetKind()]; ok {
			gvk = mapped
		}
	}
	if gvk.Empty() {
		return nil, fmt.Errorf("object has no GroupVersionKind")
	}

	obj, err := kamerascheme.Default.New(gvk)
	if err != nil {
		return nil, fmt.Errorf("creating typed object for %s: %w", gvk.String(), err)
	}

	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, obj); err != nil {
		return nil, fmt.Errorf("converting from unstructured: %w", err)
	}

	if accessor, err := meta.Accessor(obj); err == nil {
		accessor.SetNamespace(u.GetNamespace())
		accessor.SetName(u.GetName())
		accessor.SetResourceVersion(u.GetResourceVersion())
	}
	obj.GetObjectKind().SetGroupVersionKind(gvk)

	return obj, nil
}

func listKindForResource(resource string) string {
	if kind, ok := resourceToListKind[resource]; ok {
		return kind
	}
	if resource == "" {
		return "List"
	}
	return strings.ToUpper(resource[:1]) + resource[1:] + "List"
}
