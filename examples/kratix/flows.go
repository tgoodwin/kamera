package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-logr/logr"
	kratix "github.com/syntasso/kratix/api/v1alpha1"
	"github.com/syntasso/kratix/internal/controller"
	"github.com/syntasso/kratix/internal/controller/controllerfakes"
	"github.com/syntasso/kratix/lib/compression"
	"github.com/syntasso/kratix/lib/writers"
	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/explore"
	"github.com/tgoodwin/kamera/pkg/replay"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	fakeclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	controllerconfig "sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

const (
	workControllerID            tracecheck.ReconcilerID = "WorkController"
	workPlacementControllerID   tracecheck.ReconcilerID = "WorkPlacementController"
	promiseControllerID         tracecheck.ReconcilerID = "PromiseController"
	promiseRevisionControllerID tracecheck.ReconcilerID = "PromiseRevisionController"
	healthRecordControllerID    tracecheck.ReconcilerID = "HealthRecordController"
)

const (
	workKind            = "platform.kratix.io/Work"
	workPlacementKind   = "platform.kratix.io/WorkPlacement"
	destinationKind     = "platform.kratix.io/Destination"
	stateStoreKind      = "platform.kratix.io/BucketStateStore"
	resourceBindingKind = "platform.kratix.io/ResourceBinding"
	promiseKind         = "platform.kratix.io/Promise"
	promiseRevisionKind = "platform.kratix.io/PromiseRevision"
	healthRecordKind    = "platform.kratix.io/HealthRecord"
)

type dynamicControllerSpec struct {
	key          string
	controllerID tracecheck.ReconcilerID
	promiseName  string
	gvk          *schema.GroupVersionKind
	crd          *apiextensionsv1.CustomResourceDefinition
	placeholder  *controller.DynamicResourceRequestController
}

type noopScheduler struct{}

func (noopScheduler) ReconcileWork(context.Context, *kratix.Work) ([]string, error) {
	return nil, nil
}

type fakeStateStoreWriter struct {
	updates int
}

func (w *fakeStateStoreWriter) UpdateFiles(string, string, []kratix.Workload, []string) (string, error) {
	w.updates++
	return fmt.Sprintf("fake-%d", w.updates), nil
}

func (*fakeStateStoreWriter) ReadFile(string) ([]byte, error) {
	return nil, writers.ErrFileNotFound
}

func (*fakeStateStoreWriter) ValidatePermissions() error {
	return nil
}

func init() {
	controller.SetStateStoreWriterFactories(
		func(logr.Logger, kratix.BucketStateStoreSpec, string, map[string][]byte) (writers.StateStoreWriter, error) {
			return &fakeStateStoreWriter{}, nil
		},
		func(logr.Logger, kratix.GitStateStoreSpec, string, map[string][]byte) (writers.StateStoreWriter, error) {
			return &fakeStateStoreWriter{}, nil
		},
	)
}

func newKratixExplorerBuilder() *tracecheck.ExplorerBuilder {
	scheme := runtime.NewScheme()
	utilruntime.Must(kratix.AddToScheme(scheme))
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(kratix.AddToScheme(clientgoscheme.Scheme))
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	tracecheck.SetLogger(ctrl.Log.WithName("tracecheck"))
	eb := tracecheck.NewExplorerBuilder(scheme)
	eb.WithMaxDepth(100)
	return eb
}

func configureWorksReconcilers(eb *tracecheck.ExplorerBuilder) {
	eb.WithReconciler(workControllerID, func(c ctrlclient.Client) tracecheck.Reconciler {
		return &controller.WorkReconciler{
			Client:        c,
			Log:           ctrl.Log.WithName("work"),
			Scheduler:     noopScheduler{},
			EventRecorder: record.NewFakeRecorder(32),
		}
	}).For(workKind)
	eb.WithReconciler(workPlacementControllerID, func(c ctrlclient.Client) tracecheck.Reconciler {
		return &controller.WorkPlacementReconciler{
			Client:        c,
			Log:           ctrl.Log.WithName("workplacement"),
			VersionCache:  map[string]string{},
			EventRecorder: record.NewFakeRecorder(32),
		}
	}).For(workPlacementKind)
	eb.WithResourceDep(workKind, workControllerID)
	eb.WithResourceDep(workPlacementKind, workPlacementControllerID)
	eb.WithResourceDep(destinationKind, workPlacementControllerID)
	eb.WithResourceDep(stateStoreKind, workPlacementControllerID)
}

func configureHealthRecordReconciler(eb *tracecheck.ExplorerBuilder) {
	eb.WithReconciler(healthRecordControllerID, func(c ctrlclient.Client) tracecheck.Reconciler {
		nsClient := replay.NewDefaultNamespaceClient(c, "default", shouldDefaultNamespace)
		return &controller.HealthRecordReconciler{
			Client:        nsClient,
			Scheme:        c.Scheme(),
			Log:           ctrl.Log.WithName("healthrecord"),
			EventRecorder: record.NewFakeRecorder(32),
		}
	}).For(healthRecordKind)
	eb.WithResourceDep(healthRecordKind, healthRecordControllerID)
}

func configurePromisesReconcilers(
	eb *tracecheck.ExplorerBuilder,
	preStarted map[string]*controller.DynamicResourceRequestController,
) {
	eb.WithReconciler(promiseControllerID, func(c ctrlclient.Client) tracecheck.Reconciler {
		nsClient := replay.NewDefaultNamespaceClient(c, "default", shouldDefaultNamespace)
		manager := &controllerfakes.FakeManager{}
		skipNameValidation := true
		manager.GetControllerOptionsReturns(controllerconfig.Controller{SkipNameValidation: &skipNameValidation})
		manager.GetEventRecorderForReturns(record.NewFakeRecorder(32))
		return &controller.PromiseReconciler{
			Client:                    nsClient,
			ApiextensionsClient:       fakeclientset.NewSimpleClientset().ApiextensionsV1(),
			Log:                       ctrl.Log.WithName("promise"),
			Manager:                   manager,
			EventRecorder:             record.NewFakeRecorder(32),
			PromiseUpgrade:            true,
			NumberOfJobsToKeep:        1,
			ReconciliationInterval:    time.Hour,
			RestartManager:            func() {},
			StartedDynamicControllers: copyDynamicControllers(preStarted),
		}
	}).For(promiseKind)
	eb.WithReconciler(promiseRevisionControllerID, func(c ctrlclient.Client) tracecheck.Reconciler {
		nsClient := replay.NewDefaultNamespaceClient(c, "default", shouldDefaultNamespace)
		return &controller.PromiseRevisionReconciler{
			Client:        nsClient,
			Log:           ctrl.Log.WithName("promise-revision"),
			EventRecorder: record.NewFakeRecorder(32),
		}
	}).For(promiseRevisionKind)
	eb.WithResourceDep(promiseKind, promiseControllerID)
	eb.WithResourceDep(promiseRevisionKind, promiseRevisionControllerID)
}

func buildInputDrivenBuilder(inputs []coverage.Input) (*tracecheck.ExplorerBuilder, error) {
	specs := dynamicControllerSpecsFromInputs(inputs)
	prestarted := preStartedDynamicControllersFromSpecs(specs)

	eb := newKratixExplorerBuilder()
	configureWorksReconcilers(eb)
	configureDynamicRequestReconcilers(eb, specs)
	configurePromisesReconcilers(eb, prestarted)
	configureHealthRecordReconciler(eb)
	return eb, nil
}

func buildWorksFlow() (*tracecheck.ExplorerBuilder, tracecheck.StateNode, error) {
	eb := newKratixExplorerBuilder()
	configureWorksReconcilers(eb)

	work := &kratix.Work{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example-work",
			Namespace: "default",
		},
		Spec: kratix.WorkSpec{
			PromiseName:  "example-promise",
			ResourceName: "example-resource",
		},
	}
	work.SetGroupVersionKind(kratix.GroupVersion.WithKind("Work"))
	tag.AddSleeveObjectID(work)

	compressed, err := compression.CompressContent([]byte("kind: ConfigMap\napiVersion: v1\n"))
	if err != nil {
		return nil, tracecheck.StateNode{}, fmt.Errorf("compress workload: %w", err)
	}

	workPlacement := &kratix.WorkPlacement{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example-workplacement",
			Namespace: "default",
			Labels: map[string]string{
				"kratix.io/work":          work.Name,
				"kratix.io/pipeline-name": "default",
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: kratix.GroupVersion.String(),
					Kind:       "Work",
					Name:       work.Name,
				},
			},
		},
		Spec: kratix.WorkPlacementSpec{
			TargetDestinationName: "example-destination",
			PromiseName:           work.Spec.PromiseName,
			ResourceName:          work.Spec.ResourceName,
			ID:                    "workload-group-1",
			Workloads: []kratix.Workload{
				{
					Filepath: "manifests/configmap.yaml",
					Content:  string(compressed),
				},
			},
		},
	}
	workPlacement.SetGroupVersionKind(kratix.GroupVersion.WithKind("WorkPlacement"))
	tag.AddSleeveObjectID(workPlacement)

	destination := &kratix.Destination{
		ObjectMeta: metav1.ObjectMeta{
			Name: "example-destination",
		},
		Spec: kratix.DestinationSpec{
			Path: "example-path",
			StateStoreRef: &kratix.StateStoreReference{
				Kind: "BucketStateStore",
				Name: "example-statestore",
			},
		},
	}
	destination.SetGroupVersionKind(kratix.GroupVersion.WithKind("Destination"))
	tag.AddSleeveObjectID(destination)

	stateStore := &kratix.BucketStateStore{
		ObjectMeta: metav1.ObjectMeta{
			Name: "example-statestore",
		},
		Spec: kratix.BucketStateStoreSpec{
			BucketName: "example-bucket",
			Endpoint:   "https://example.invalid",
			AuthMethod: kratix.AuthMethodIAM,
		},
	}
	stateStore.SetGroupVersionKind(kratix.GroupVersion.WithKind("BucketStateStore"))
	tag.AddSleeveObjectID(stateStore)

	initialState, err := eb.BuildStartStateFromObjects(
		[]ctrlclient.Object{work, workPlacement, destination, stateStore},
		[]tracecheck.PendingReconcile{
			{
				ReconcilerID: workPlacementControllerID,
				Request: ctrl.Request{
					NamespacedName: ctrlclient.ObjectKeyFromObject(workPlacement),
				},
				Source: tracecheck.SourceStateChange,
			},
			{
				ReconcilerID: workControllerID,
				Request: ctrl.Request{
					NamespacedName: ctrlclient.ObjectKeyFromObject(work),
				},
				Source: tracecheck.SourceStateChange,
			},
		},
	)
	if err != nil {
		return nil, tracecheck.StateNode{}, fmt.Errorf("build start state: %w", err)
	}

	return eb, initialState, nil
}

func buildPromisesFlow() (*tracecheck.ExplorerBuilder, tracecheck.StateNode, error) {
	eb := newKratixExplorerBuilder()
	configurePromisesReconcilers(eb, nil)

	promise := &kratix.Promise{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example-promise",
			Namespace: "default",
			Labels: map[string]string{
				kratix.PromiseVersionLabel: "v1",
			},
		},
	}
	promise.SetGroupVersionKind(kratix.GroupVersion.WithKind("Promise"))
	tag.AddSleeveObjectID(promise)

	initialState, err := eb.BuildStartStateFromObjects(
		[]ctrlclient.Object{promise},
		[]tracecheck.PendingReconcile{
			{
				ReconcilerID: promiseControllerID,
				Request: ctrl.Request{
					NamespacedName: ctrlclient.ObjectKeyFromObject(promise),
				},
				Source: tracecheck.SourceStateChange,
			},
		},
	)
	if err != nil {
		return nil, tracecheck.StateNode{}, fmt.Errorf("build start state: %w", err)
	}

	return eb, initialState, nil
}

func scenariosFromInputs(builder *tracecheck.ExplorerBuilder, inputs []coverage.Input) ([]explore.Scenario, error) {
	return explore.CompileInputScenarios(builder, inputs, explore.ScenarioCompileOptions{
		BuildState: buildStateFromCoverageInput,
	})
}

func buildStateFromCoverageInput(
	builder *tracecheck.ExplorerBuilder,
	input coverage.Input,
) (tracecheck.StateNode, []ctrlclient.Object, error) {
	objects := make([]ctrlclient.Object, 0, len(input.EnvironmentState.Objects))
	for _, obj := range input.EnvironmentState.Objects {
		if obj == nil {
			continue
		}
		objects = append(objects, obj.DeepCopy())
	}
	if len(objects) == 0 {
		return tracecheck.StateNode{}, nil, fmt.Errorf("input has no environment objects")
	}

	pending := initialPendingReconciles(objects)
	state, err := builder.BuildStartStateFromObjects(objects, pending)
	if err != nil {
		return tracecheck.StateNode{}, nil, err
	}
	return state, objects, nil
}

func initialPendingReconciles(objects []ctrlclient.Object) []tracecheck.PendingReconcile {
	pending := make([]tracecheck.PendingReconcile, 0)
	for _, obj := range objects {
		if obj == nil {
			continue
		}
		controllerID, ok := controllerForObject(obj)
		if !ok {
			continue
		}
		pending = append(pending, tracecheck.PendingReconcile{
			ReconcilerID: controllerID,
			Request: ctrl.Request{
				NamespacedName: ctrlclient.ObjectKeyFromObject(obj),
			},
			Source: tracecheck.SourceStateChange,
		})
	}
	return pending
}

func controllerForObject(obj ctrlclient.Object) (tracecheck.ReconcilerID, bool) {
	gvk := obj.GetObjectKind().GroupVersionKind()
	switch gvk.Group + "/" + gvk.Kind {
	case "platform.kratix.io/Promise":
		return promiseControllerID, true
	case "platform.kratix.io/PromiseRevision":
		return promiseRevisionControllerID, true
	case "platform.kratix.io/Work":
		return workControllerID, true
	case "platform.kratix.io/WorkPlacement", "platform.kratix.io/Destination", "platform.kratix.io/BucketStateStore":
		return workPlacementControllerID, true
	case "platform.kratix.io/HealthRecord":
		return healthRecordControllerID, true
	default:
		return "", false
	}
}

func dynamicControllerSpecsFromInputs(inputs []coverage.Input) []dynamicControllerSpec {
	out := make([]dynamicControllerSpec, 0)
	seen := make(map[string]struct{})
	add := func(obj *unstructured.Unstructured) {
		spec, ok := dynamicControllerSpecForPromise(obj)
		if !ok {
			return
		}
		if _, exists := seen[spec.key]; exists {
			return
		}
		seen[spec.key] = struct{}{}
		out = append(out, spec)
	}

	for _, input := range inputs {
		for _, obj := range input.EnvironmentState.Objects {
			add(obj)
		}
		for _, userInput := range input.ExternalInputs {
			add(userInput.Object)
		}
	}
	return out
}

func dynamicControllerSpecForPromise(obj *unstructured.Unstructured) (dynamicControllerSpec, bool) {
	if obj == nil {
		return dynamicControllerSpec{}, false
	}
	if obj.GetKind() != "Promise" || !strings.HasPrefix(obj.GetAPIVersion(), "platform.kratix.io/") {
		return dynamicControllerSpec{}, false
	}

	promise := &kratix.Promise{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, promise); err != nil {
		return dynamicControllerSpec{}, false
	}
	if !promise.ContainsAPI() {
		return dynamicControllerSpec{}, false
	}

	gvk, crd, err := promise.GetAPI()
	if err != nil || gvk == nil || crd == nil {
		return dynamicControllerSpec{}, false
	}

	controllerID := tracecheck.ReconcilerID(fmt.Sprintf("DynamicResourceRequestController/%s", promise.GetName()))
	return dynamicControllerSpec{
		key:          promise.GetDynamicControllerName(ctrl.Log.WithName("promise-prestart")),
		controllerID: controllerID,
		promiseName:  promise.GetName(),
		gvk:          gvk,
		crd:          crd,
		placeholder:  &controller.DynamicResourceRequestController{},
	}, true
}

func preStartedDynamicControllersFromSpecs(specs []dynamicControllerSpec) map[string]*controller.DynamicResourceRequestController {
	if len(specs) == 0 {
		return nil
	}
	out := make(map[string]*controller.DynamicResourceRequestController, len(specs))
	for _, spec := range specs {
		out[spec.key] = spec.placeholder
	}
	return out
}

func configureDynamicRequestReconcilers(eb *tracecheck.ExplorerBuilder, specs []dynamicControllerSpec) {
	for _, spec := range specs {
		spec := spec
		rrKind := spec.gvk.Group + "/" + spec.gvk.Kind

		eb.WithReconciler(spec.controllerID, func(c ctrlclient.Client) tracecheck.Reconciler {
			nsClient := replay.NewDefaultNamespaceClient(c, "default", shouldDefaultNamespace)
			enabled := true
			canCreateResources := true

			ctrl := &controller.DynamicResourceRequestController{
				Client:                      nsClient,
				GVK:                         spec.gvk,
				Scheme:                      c.Scheme(),
				PromiseIdentifier:           spec.promiseName,
				Log:                         ctrl.Log.WithName("dynamic-resource-request").WithName(spec.promiseName),
				UID:                         "00000",
				Enabled:                     &enabled,
				CRD:                         spec.crd,
				PromiseDestinationSelectors: nil,
				CanCreateResources:          &canCreateResources,
				NumberOfJobsToKeep:          1,
				ReconciliationInterval:      time.Hour,
				EventRecorder:               record.NewFakeRecorder(32),
				PromiseUpgrade:              true,
			}
			// Update the shared placeholder so the Promise controller can find
			// this controller via StartedDynamicControllers. Each fork gets a
			// fresh controller with the correct replay client.
			*spec.placeholder = *ctrl
			return ctrl
		}).For(rrKind)

		eb.WithResourceDep(rrKind, spec.controllerID)
		eb.WithResourceDep(workKind, spec.controllerID)
		eb.WithResourceDep(resourceBindingKind, spec.controllerID)
	}
}

func copyDynamicControllers(
	in map[string]*controller.DynamicResourceRequestController,
) map[string]*controller.DynamicResourceRequestController {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]*controller.DynamicResourceRequestController, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
