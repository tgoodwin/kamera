package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"
	"unsafe"

	"github.com/google/go-containerregistry/pkg/authn/k8schain"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/tgoodwin/kamera/examples/knative-explore/knative"
	kamerascheme "github.com/tgoodwin/kamera/examples/knative-explore/knative/scheme"
	"github.com/tgoodwin/kamera/pkg/interactive"
	"github.com/tgoodwin/kamera/pkg/replay"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
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
	ctrlzap "sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var scheme = kamerascheme.Default

type digestBypassResolver struct{}

func (digestBypassResolver) Resolve(_ *zap.SugaredLogger, rev *v1.Revision, _ k8schain.Options, registriesToSkip sets.Set[string], _ time.Duration) ([]v1.ContainerStatus, []v1.ContainerStatus, error) {
	initStatuses := make([]v1.ContainerStatus, len(rev.Spec.InitContainers))
	for i, container := range rev.Spec.InitContainers {
		initStatuses[i] = v1.ContainerStatus{
			Name:        container.Name,
			ImageDigest: fakeDigest(container.Image, registriesToSkip),
		}
	}

	statuses := make([]v1.ContainerStatus, len(rev.Spec.Containers))
	for i, container := range rev.Spec.Containers {
		statuses[i] = v1.ContainerStatus{
			Name:        container.Name,
			ImageDigest: fakeDigest(container.Image, registriesToSkip),
		}
	}
	return initStatuses, statuses, nil
}

func (digestBypassResolver) Clear(types.NamespacedName) {}

func (digestBypassResolver) Forget(types.NamespacedName) {}

func fakeDigest(image string, skipRegistries sets.Set[string]) string {
	if image == "" {
		return ""
	}

	if _, err := name.NewDigest(image, name.WeakValidation); err == nil {
		return image
	}

	tag, err := name.NewTag(image, name.WeakValidation)
	if err != nil {
		return ""
	}

	if skipRegistries != nil && skipRegistries.Has(tag.Registry.RegistryStr()) {
		return ""
	}

	repo := tag.Repository.String()
	sum := sha256.Sum256([]byte(image))
	return fmt.Sprintf("%s@sha256:%s", repo, hex.EncodeToString(sum[:]))
}

func overrideRevisionResolver(impl *controller.Impl) {
	reconcilerValue := reflect.ValueOf(impl.Reconciler)
	if reconcilerValue.Kind() != reflect.Ptr {
		return
	}

	reconcilerElem := reconcilerValue.Elem()
	internalReconciler := reconcilerElem.FieldByName("reconciler")
	if !internalReconciler.IsValid() {
		return
	}

	if !internalReconciler.CanInterface() {
		internalReconciler = reflect.NewAt(internalReconciler.Type(), unsafe.Pointer(internalReconciler.UnsafeAddr())).Elem()
	}

	rec, ok := internalReconciler.Interface().(*revisionreconciler.Reconciler)
	if !ok || rec == nil {
		return
	}

	resolverField := reflect.ValueOf(rec).Elem().FieldByName("resolver")
	if !resolverField.IsValid() {
		return
	}

	if !resolverField.CanSet() {
		resolverField = reflect.NewAt(resolverField.Type(), unsafe.Pointer(resolverField.UnsafeAddr())).Elem()
	}
	resolverField.Set(reflect.ValueOf(digestBypassResolver{}))
}

type revisionDigestStub struct {
	client.Client
}

func (r *revisionDigestStub) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	rev := &v1.Revision{}
	if err := r.Get(ctx, req.NamespacedName, rev); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}

	totalContainers := len(rev.Spec.Containers)
	totalInits := len(rev.Spec.InitContainers)
	if len(rev.Status.ContainerStatuses) == totalContainers && len(rev.Status.InitContainerStatuses) == totalInits {
		return reconcile.Result{}, nil
	}

	containerStatuses := make([]v1.ContainerStatus, totalContainers)
	for i, c := range rev.Spec.Containers {
		containerStatuses[i] = v1.ContainerStatus{Name: c.Name}
	}

	initStatuses := make([]v1.ContainerStatus, totalInits)
	for i, c := range rev.Spec.InitContainers {
		initStatuses[i] = v1.ContainerStatus{Name: c.Name}
	}

	rev.Status.ContainerStatuses = containerStatuses
	rev.Status.InitContainerStatuses = initStatuses
	if err := r.Status().Update(ctx, rev); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

func main() {
	logLevel := flag.String("log-level", "info", "logging level (debug, info, warn, error)")
	interactiveFlag := flag.Bool("interactive", true, "launch interactive trace inspector")
	flag.Parse()

	level, err := parseLogLevel(*logLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid log level %q: %v\n", *logLevel, err)
		os.Exit(2)
	}

	logf.SetLogger(ctrlzap.New(
		ctrlzap.UseDevMode(level <= zapcore.DebugLevel),
		ctrlzap.Level(level),
	))

	tracecheck.SetLogger(logf.Log.WithName("tracecheck"))

	eb := tracecheck.NewExplorerBuilder(scheme)
	// instantiate reconcilers
	eb.WithCustomStrategy("RevisionReconciler", func(r replay.EffectRecorder) tracecheck.Strategy {
		factory := func(ctx context.Context, cmw configmap.Watcher) *controller.Impl {
			impl := revisionreconciler.NewController(ctx, cmw)
			overrideRevisionResolver(impl)
			return impl
		}
		strategy, err := kamera.NewKnativeStrategy(factory, r)
		if err != nil {
			panic(err)
		}
		strategy.SetLogger(logf.Log.WithName("RevisionReconciler"))
		return strategy
	})
	eb.WithCustomStrategy("KPA", func(r replay.EffectRecorder) tracecheck.Strategy {
		factory := func(ctx context.Context, cmw configmap.Watcher) *controller.Impl {
			multiScaler := kamera.NewFakeMultiScaler(ctx.Done(), logging.FromContext(ctx))
			return kpareconciler.NewController(ctx, cmw, multiScaler)
		}
		strategy, err := kamera.NewKnativeStrategy(factory, r, serving.RevisionUID)
		if err != nil {
			panic(fmt.Sprintf("NewKnativeStrategy() error = %v", err))
		}
		strategy.SetLogger(logf.Log.WithName("KPAReconciler"))
		return strategy
	})
	eb.WithCustomStrategy("ServiceReconciler", func(r replay.EffectRecorder) tracecheck.Strategy {
		strategy, err := kamera.NewKnativeStrategy(servicecontroller.NewController, r)
		if err != nil {
			panic(fmt.Sprintf("NewKnativeStrategy() error = %v", err))
		}
		strategy.SetLogger(logf.Log.WithName("ServiceReconciler"))
		return strategy
	})
	eb.WithCustomStrategy("RouteReconciler", func(r replay.EffectRecorder) tracecheck.Strategy {
		strategy, err := kamera.NewKnativeStrategy(routecontroller.NewController, r)
		if err != nil {
			panic(fmt.Sprintf("NewKnativeStrategy() error = %v", err))
		}
		strategy.SetLogger(logf.Log.WithName("RouteReconciler"))
		return strategy
	})

	eb.WithCustomStrategy("ServerlessServiceReconciler", func(r replay.EffectRecorder) tracecheck.Strategy {
		strategy, err := kamera.NewKnativeStrategy(serverlessservicecontroller.NewController, r)
		if err != nil {
			panic(fmt.Sprintf("NewKnativeStrategy() error = %v", err))
		}
		strategy.SetLogger(logf.Log.WithName("ServerlessServiceReconciler"))
		return strategy
	})
	eb.AssignReconcilerToKind("ServerlessServiceReconciler", "networking.internal.knative.dev/ServerlessService")
	eb.WithResourceDep("networking.internal.knative.dev/ServerlessService", "ServerlessServiceReconciler", "KPA")

	eb.WithCustomStrategy("ConfigurationReconciler", func(r replay.EffectRecorder) tracecheck.Strategy {
		strategy, err := kamera.NewKnativeStrategy(configuration.NewController, r)
		if err != nil {
			panic(err)
		}
		strategy.SetLogger(logf.Log.WithName("ConfigurationReconciler"))
		return strategy
	})
	eb.AssignReconcilerToKind("ConfigurationReconciler", "serving.knative.dev/Configuration")
	eb.WithResourceDep("Configuration", "ConfigurationReconciler", "RevisionReconciler")

	eb.AssignReconcilerToKind("RevisionReconciler", "serving.knative.dev/Revision")
	eb.AssignReconcilerToKind("KPA", "autoscaling.internal.knative.dev/PodAutoscaler")
	eb.AssignReconcilerToKind("ServiceReconciler", "serving.knative.dev/Service")
	eb.AssignReconcilerToKind("RouteReconciler", "serving.knative.dev/Route")

	eb.WithReconciler("RevisionDigestStub", func(c tracecheck.Client) tracecheck.Reconciler {
		return &revisionDigestStub{Client: c}
	})
	eb.AssignReconcilerToKind("RevisionDigestStub", "serving.knative.dev/Revision")

	eb.WithReconciler("IngressStatusStub", func(c tracecheck.Client) tracecheck.Reconciler {
		return &kamera.IngressStatusStub{Client: c}
	})
	eb.AssignReconcilerToKind("IngressStatusStub", "networking.internal.knative.dev/Ingress")

	eb.WithResourceDep("serving.knative.dev/Revision", "RevisionDigestStub", "RevisionReconciler", "KPA", "ServiceReconciler")
	eb.WithResourceDep("autoscaling.internal.knative.dev/PodAutoscaler", "KPA", "ServerlessServiceReconciler")
	eb.WithResourceDep("serving.knative.dev/Service", "ServiceReconciler")
	eb.WithResourceDep("serving.knative.dev/Configuration", "ServiceReconciler", "RevisionReconciler")
	eb.WithResourceDep("serving.knative.dev/Route", "RouteReconciler", "ServiceReconciler")
	eb.WithResourceDep("networking.internal.knative.dev/Ingress", "RouteReconciler", "ServerlessServiceReconciler")

	eb.WithMaxDepth(100)

	explorer, err := eb.Build("standalone")
	if err != nil {
		panic(fmt.Sprintf("Build() error = %v", err))
	}
	stateBuilder := eb.NewStateEventBuilder()
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "default",
		},
		Spec: v1.ServiceSpec{
			ConfigurationSpec: v1.ConfigurationSpec{
				Template: v1.RevisionTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Name: "kamera-test",
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

	tag.AddSleeveObjectID(svc)
	serviceState := stateBuilder.AddTopLevelObject(svc, "ServiceReconciler")
	initialState := mergeStateNodes(serviceState)
	fmt.Printf("[main] initial pending: %v\n", initialState.PendingReconciles)

	res := explorer.Explore(context.Background(), initialState)

	fmt.Println("converged states:", len(res.ConvergedStates))
	fmt.Println("aborted states:", len(res.AbortedStates))

	states := append([]tracecheck.ResultState{}, res.ConvergedStates...)
	states = append(states, res.AbortedStates...)
	if len(states) == 0 {
		fmt.Println("no states returned from exploration")
		return
	}

	if !*interactiveFlag {
		fmt.Printf("interactive inspector disabled; states available: %d (converged=%d, aborted=%d)\n",
			len(states), len(res.ConvergedStates), len(res.AbortedStates))
		return
	}

	interactive.RunStateInspectorTUIView(states, true)
}

// mergeStateNodes is a helper that merges multiple StateNode instances into one
func mergeStateNodes(primary tracecheck.StateNode, others ...tracecheck.StateNode) tracecheck.StateNode {
	merged := primary

	pendingSeen := make(map[string]struct{})
	pending := make([]tracecheck.PendingReconcile, 0, len(merged.PendingReconciles))
	appendPending := func(items []tracecheck.PendingReconcile) {
		for _, pr := range items {
			key := pr.String()
			if _, exists := pendingSeen[key]; exists {
				continue
			}
			pendingSeen[key] = struct{}{}
			pending = append(pending, pr)
		}
	}

	appendPending(merged.PendingReconciles)

	objects := merged.Objects()
	if objects == nil {
		objects = make(tracecheck.ObjectVersions)
	}

	allNodes := append([]tracecheck.StateNode{merged}, others...)

	for _, node := range others {
		for key, value := range node.Objects() {
			objects[key] = value
		}
		appendPending(node.PendingReconciles)
	}

	kindSeq := make(tracecheck.KindSequences)
	for key := range objects {
		canonicalKind := key.IdentityKey.CanonicalGroupKind()
		var (
			seq   int64
			found bool
		)
		for _, node := range allNodes {
			if node.Contents.KindSequences == nil {
				continue
			}
			if val, ok := node.Contents.KindSequences[canonicalKind]; ok {
				seq = val
				found = true
				break
			}
		}
		if !found {
			seq = 1
		}
		kindSeq[canonicalKind] = seq
	}

	merged.PendingReconciles = pending
	merged.Contents.KindSequences = kindSeq
	return merged
}

func parseLogLevel(level string) (zapcore.Level, error) {
	switch strings.ToLower(level) {
	case "debug":
		return zapcore.DebugLevel, nil
	case "info":
		return zapcore.InfoLevel, nil
	case "warn", "warning":
		return zapcore.WarnLevel, nil
	case "error":
		return zapcore.ErrorLevel, nil
	case "dpanic":
		return zapcore.DPanicLevel, nil
	case "panic":
		return zapcore.PanicLevel, nil
	case "fatal":
		return zapcore.FatalLevel, nil
	default:
		return zapcore.InfoLevel, fmt.Errorf("supported values: debug, info, warn, error")
	}
}
