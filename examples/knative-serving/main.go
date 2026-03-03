package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"
	"unsafe"

	"github.com/google/go-containerregistry/pkg/authn/k8schain"
	"github.com/google/go-containerregistry/pkg/name"
	knativescheme "github.com/tgoodwin/kamera/examples/knative-serving/knative/scheme"
	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/explore"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"knative.dev/pkg/controller"
	v1 "knative.dev/serving/pkg/apis/serving/v1"
	revisionreconciler "knative.dev/serving/pkg/reconciler/revision"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var scheme = knativescheme.Default

var parallelFlag = flag.Bool("parallel", false, "run parallel scenario mode; if --inputs is omitted, load default knative inputs from file")

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
	flag.Parse()

	ctx := context.Background()
	builder := newKnativeExplorerBuilder()
	if cfgPath := explore.ConfigPath(); cfgPath != "" {
		loadedCfg, err := explore.LoadExploreConfigFromFile(cfgPath, builder.Config())
		if err != nil {
			fmt.Fprintf(os.Stderr, "load explore config: %v\n", err)
			os.Exit(1)
		}
		builder.SetConfig(loadedCfg)
	}
	inputs, batchMode, err := batchInputsForRun(*parallelFlag, explore.InputsPath())
	if err != nil {
		fmt.Fprintf(os.Stderr, "load inputs: %v\n", err)
		os.Exit(1)
	}
	if batchMode {
		if explore.InteractiveEnabled() {
			fmt.Fprintln(os.Stderr, "interactive ignored in batch mode")
		}
		runner, err := explore.NewParallelRunner(builder)
		if err != nil {
			fmt.Fprintf(os.Stderr, "runner setup error: %v\n", err)
			os.Exit(1)
		}
		opts := explore.ParallelOptions{DumpDir: explore.DumpPath()}

		if explore.PerturbEnabled() {
			fmt.Fprintln(os.Stderr, "closed-loop scaffold: running per-input reference->rerun pipelines (core planner)")
		} else {
			fmt.Fprintln(os.Stderr, "closed-loop scaffold: running per-input reference-only simulation (--perturb=false)")
		}

		scenarios, err := scenariosFromInputs(builder, inputs)
		if err != nil {
			fmt.Fprintf(os.Stderr, "build scenarios: %v\n", err)
			os.Exit(1)
		}
		if _, err := runner.RunAll(ctx, scenarios, opts); err != nil {
			fmt.Fprintf(os.Stderr, "batch run error: %v\n", err)
			os.Exit(1)
		}
		return
	}
	initialState := buildInitialKnativeState(builder)
	runner, err := explore.NewRunner(builder)
	if err != nil {
		fmt.Fprintf(os.Stderr, "runner setup error: %v\n", err)
		os.Exit(1)
	}
	if err := runner.Run(ctx, explore.RunInput{EnvironmentState: initialState}); err != nil {
		fmt.Fprintf(os.Stderr, "session error: %v\n", err)
		os.Exit(1)
	}
}

func batchInputsForRun(parallel bool, inputsPath string) ([]coverage.Input, bool, error) {
	if inputsPath != "" {
		inputs, err := coverage.LoadInputs(inputsPath)
		return inputs, true, err
	}
	if !parallel {
		return nil, false, nil
	}
	inputs, err := loadDefaultKnativeInputs()
	if err != nil {
		return nil, false, err
	}
	return inputs, true, nil
}

var defaultKnativeInputsSearchPaths = []string{
	"inputs-example.json",
	filepath.Join("examples", "knative-serving", "inputs-example.json"),
}

func loadDefaultKnativeInputs() ([]coverage.Input, error) {
	for _, path := range defaultKnativeInputsSearchPaths {
		inputs, err := coverage.LoadInputs(path)
		if err == nil {
			return inputs, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("load inputs from %s: %w", path, err)
		}
	}
	return nil, fmt.Errorf("inputs file not found in search paths %v: %w", defaultKnativeInputsSearchPaths, os.ErrNotExist)
}
