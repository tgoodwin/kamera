package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"time"

	knativeharness "github.com/tgoodwin/kamera/examples/knative-serving/knative"
	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/explore"
	"github.com/tgoodwin/kamera/pkg/replay"
	"github.com/tgoodwin/kamera/pkg/simclock"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
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

var errInvalidKnativeParams = errors.New("invalid knative service params")

func newKnativeExplorerBuilder() *tracecheck.ExplorerBuilder {
	// Configure simclock to use 2s steps instead of 1s to speed up scale-to-zero simulation
	// (60s stable window + 30s grace period = 90s total, which is 45 steps at 2s/step)
	// Note: 2s matches the KPA ticker interval (tickInterval = 2s), so tickers work correctly
	simclock.Configure(time.Unix(0, 0), 2*time.Second)

	builder := tracecheck.NewExplorerBuilder(scheme)
	configureKnativeExplorer(builder)

	builder.WithoutOptimizations()

	return builder
}

func buildInitialKnativeState(builder *tracecheck.ExplorerBuilder) tracecheck.StateNode {
	stateBuilder := builder.NewStateEventBuilder()
	svc := buildBaselineService()
	tag.AddSleeveObjectID(svc)
	serviceState := stateBuilder.AddTopLevelObject(svc, "ServiceReconciler")
	return tracecheck.MergeStateNodes(serviceState)
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
	}).For("serving.knative.dev/Route").Watches(
		"serving.knative.dev/Configuration",
		func(u *unstructured.Unstructured) []reconcile.Request {
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
	}).For("networking.internal.knative.dev/ServerlessService").Watches(
		// In real Knative, SSReconciler watches Endpoints via two handlers:
		// 1. Endpoints with SKS label → map to SSS name
		// 2. Activator endpoints → global resync
		// The private endpoints (e.g. demo-00001-private) don't have the SKS label,
		// but they're critical for SSReconciler to complete. We match both labeled
		// endpoints AND private endpoints (name ending in "-private" → strip suffix).
		"Endpoints",
		func(u *unstructured.Unstructured) []reconcile.Request {
			labels := u.GetLabels()
			// Labeled endpoints: direct SSS name from label
			if sksName := labels["networking.internal.knative.dev/serverlessservice"]; sksName != "" {
				return []reconcile.Request{
					{NamespacedName: client.ObjectKey{Namespace: u.GetNamespace(), Name: sksName}},
				}
			}
			// Private endpoints: name convention is "<sss-name>-private"
			name := u.GetName()
			if strings.HasSuffix(name, "-private") {
				sksName := strings.TrimSuffix(name, "-private")
				return []reconcile.Request{
					{NamespacedName: client.ObjectKey{Namespace: u.GetNamespace(), Name: sksName}},
				}
			}
			return nil
		}).Watches(
		// SSReconciler also watches Services it owns (via ownerRef).
		"Service",
		func(u *unstructured.Unstructured) []reconcile.Request {
			for _, ref := range u.GetOwnerReferences() {
				if ref.Kind == "ServerlessService" {
					return []reconcile.Request{
						{NamespacedName: client.ObjectKey{Namespace: u.GetNamespace(), Name: ref.Name}},
					}
				}
			}
			return nil
		})

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

func scenariosFromInputs(builder *tracecheck.ExplorerBuilder, inputs []coverage.Input) ([]explore.Scenario, error) {
	if builder == nil {
		return nil, fmt.Errorf("builder is nil")
	}
	if len(inputs) == 0 {
		return nil, fmt.Errorf("no inputs supplied")
	}

	baseCfg := builder.Config()
	scenarios := make([]explore.Scenario, 0, len(inputs))
	for idx, input := range inputs {
		state, seededObjects, err := buildStateFromCoverageInput(builder, input)
		if err != nil {
			return nil, fmt.Errorf("build start state for input %d (%s): %w", idx, input.Name, err)
		}
		userInputs, err := buildUserActionsFromCoverageInput(input, seededObjects)
		if err != nil {
			return nil, fmt.Errorf("build user actions for input %d (%s): %w", idx, input.Name, err)
		}

		cfg, err := explore.ApplyInputTuning(baseCfg, input.Tuning)
		if err != nil {
			return nil, fmt.Errorf("apply tuning for input %d (%s): %w", idx, input.Name, err)
		}
		scenarios = append(scenarios, explore.Scenario{
			Name:             input.Name,
			EnvironmentState: state,
			ExternalInputs:       userInputs,
			Config:           cfg,
			Context:          scenarioContextForInput(input),
		})
	}

	if len(scenarios) == 0 {
		return nil, fmt.Errorf("no scenarios produced")
	}
	return scenarios, nil
}

func scenarioContextForInput(input coverage.Input) explore.ScenarioContext {
	workflow := "batch-input"
	attributes := map[string]string{}

	inputRef := ""
	inputsPath := strings.TrimSpace(explore.InputsPath())
	if inputsPath != "" {
		name := strings.TrimSpace(input.Name)
		if name == "" {
			inputRef = inputsPath
		} else {
			inputRef = fmt.Sprintf("%s#%s", inputsPath, name)
		}
	}

	if len(attributes) == 0 {
		attributes = nil
	}
	return explore.ScenarioContext{
		Workflow:   workflow,
		InputRef:   inputRef,
		Attributes: attributes,
	}
}

type knativeParamSpec struct {
	name    string
	options []knativeParamOption
}

type knativeParamOption struct {
	label      string
	isBaseline bool
	apply      func(*v1.Service)
}

func expandKnativeParameterizedInput(input coverage.Input, fuzzCases int, fuzzSeed int64) ([]coverage.Input, error) {
	baseName := strings.TrimSpace(input.Name)
	if baseName == "" {
		baseName = "scenario"
	}

	base := cloneCoverageInput(input)
	base.Name = baseName + "/base"
	serviceIdx := findKnativeServiceInUserInputs(base.ExternalInputs)
	if serviceIdx < 0 {
		return []coverage.Input{base}, nil
	}

	templateSvc, err := unstructuredToService(base.ExternalInputs[serviceIdx].Object)
	if err != nil {
		return nil, err
	}

	variants := []coverage.Input{base}
	singleVariants, err := expandKnativeSingleParamVariants(input, baseName, serviceIdx, templateSvc)
	if err != nil {
		return nil, err
	}
	variants = append(variants, singleVariants...)

	sampledVariants, err := expandKnativeSampledParamVariants(input, baseName, serviceIdx, templateSvc, fuzzCases, fuzzSeed)
	if err != nil {
		return nil, err
	}
	variants = append(variants, sampledVariants...)

	return variants, nil
}

func knativeParamCatalog() []knativeParamSpec {
	return []knativeParamSpec{
		{
			name: "image",
			options: []knativeParamOption{
				{label: "baseline", isBaseline: true, apply: func(*v1.Service) {}},
				{label: "v2", apply: func(svc *v1.Service) { ensurePrimaryContainer(svc).Image = "dev.local/test:v2" }},
				{label: "v3", apply: func(svc *v1.Service) { ensurePrimaryContainer(svc).Image = "dev.local/test:v3" }},
			},
		},
		{
			name: "min-scale",
			options: []knativeParamOption{
				{label: "baseline", isBaseline: true, apply: func(*v1.Service) {}},
				{label: "1", apply: func(svc *v1.Service) { ensureTemplateAnnotations(svc)[autoscaling.MinScaleAnnotationKey] = "1" }},
				{label: "2", apply: func(svc *v1.Service) { ensureTemplateAnnotations(svc)[autoscaling.MinScaleAnnotationKey] = "2" }},
			},
		},
		{
			name: "max-scale",
			options: []knativeParamOption{
				{label: "baseline", isBaseline: true, apply: func(*v1.Service) {}},
				{label: "1", apply: func(svc *v1.Service) { ensureTemplateAnnotations(svc)[autoscaling.MaxScaleAnnotationKey] = "1" }},
				{label: "5", apply: func(svc *v1.Service) { ensureTemplateAnnotations(svc)[autoscaling.MaxScaleAnnotationKey] = "5" }},
				{label: "10", apply: func(svc *v1.Service) { ensureTemplateAnnotations(svc)[autoscaling.MaxScaleAnnotationKey] = "10" }},
			},
		},
		{
			name: "concurrency",
			options: []knativeParamOption{
				{label: "baseline", isBaseline: true, apply: func(*v1.Service) {}},
				{label: "0", apply: func(svc *v1.Service) { zero := int64(0); svc.Spec.Template.Spec.ContainerConcurrency = &zero }},
				{label: "1", apply: func(svc *v1.Service) { one := int64(1); svc.Spec.Template.Spec.ContainerConcurrency = &one }},
				{label: "10", apply: func(svc *v1.Service) { ten := int64(10); svc.Spec.Template.Spec.ContainerConcurrency = &ten }},
				{label: "100", apply: func(svc *v1.Service) { hundred := int64(100); svc.Spec.Template.Spec.ContainerConcurrency = &hundred }},
			},
		},
	}
}

func expandKnativeSingleParamVariants(
	input coverage.Input,
	baseName string,
	serviceIdx int,
	templateSvc *v1.Service,
) ([]coverage.Input, error) {
	variants := make([]coverage.Input, 0)
	for _, spec := range knativeParamCatalog() {
		for _, option := range spec.options {
			if option.isBaseline {
				continue
			}
			name := fmt.Sprintf("%s/single/%s-%s", baseName, spec.name, option.label)
			updated, err := buildKnativeVariantInput(input, serviceIdx, templateSvc, name, []knativeParamOption{option})
			if err != nil {
				return nil, err
			}
			variants = append(variants, updated)
		}
	}
	return variants, nil
}

func expandKnativeSampledParamVariants(
	input coverage.Input,
	baseName string,
	serviceIdx int,
	templateSvc *v1.Service,
	cases int,
	seed int64,
) ([]coverage.Input, error) {
	if cases <= 0 {
		return nil, nil
	}

	specs := knativeParamCatalog()
	rng := rand.New(rand.NewSource(seed))
	seenAssignments := map[string]struct{}{}
	variants := make([]coverage.Input, 0, cases)

	maxAttempts := cases * 20
	for attempts := 0; attempts < maxAttempts && len(variants) < cases; attempts++ {
		selection := sampleKnativeParamSelection(rng, specs)
		key := knativeSelectionKey(selection)
		if key == "" {
			continue
		}
		if _, exists := seenAssignments[key]; exists {
			continue
		}
		seenAssignments[key] = struct{}{}

		choiceOptions := make([]knativeParamOption, 0, len(selection))
		choiceNames := make([]string, 0, len(selection))
		for specIdx, optionIdx := range selection {
			spec := specs[specIdx]
			option := spec.options[optionIdx]
			choiceOptions = append(choiceOptions, option)
			choiceNames = append(choiceNames, fmt.Sprintf("%s-%s", spec.name, option.label))
		}
		sort.Strings(choiceNames)
		name := fmt.Sprintf("%s/sampled-%03d/%s", baseName, len(variants)+1, strings.Join(choiceNames, "+"))

		updated, err := buildKnativeVariantInput(input, serviceIdx, templateSvc, name, choiceOptions)
		if err != nil {
			if errors.Is(err, errInvalidKnativeParams) {
				continue
			}
			return nil, err
		}
		variants = append(variants, updated)
	}

	return variants, nil
}

func buildKnativeVariantInput(
	input coverage.Input,
	serviceIdx int,
	templateSvc *v1.Service,
	name string,
	options []knativeParamOption,
) (coverage.Input, error) {
	updated := cloneCoverageInput(input)
	updated.Name = name

	svc := templateSvc.DeepCopy()
	for _, option := range options {
		option.apply(svc)
	}

	if err := validateKnativeServiceParams(svc); err != nil {
		return coverage.Input{}, fmt.Errorf("%w for %q: %v", errInvalidKnativeParams, name, err)
	}

	serviceObj, err := serviceToUnstructured(svc)
	if err != nil {
		return coverage.Input{}, fmt.Errorf("convert parameterized service for %q: %w", name, err)
	}
	if serviceIdx >= len(updated.ExternalInputs) || updated.ExternalInputs[serviceIdx].Object == nil {
		return coverage.Input{}, fmt.Errorf("service user input missing for %q", name)
	}
	updated.ExternalInputs[serviceIdx].Object = serviceObj
	return updated, nil
}

func sampleKnativeParamSelection(rng *rand.Rand, specs []knativeParamSpec) map[int]int {
	selection := map[int]int{}
	if len(specs) == 0 {
		return selection
	}

	primaryIdx := rng.Intn(len(specs))
	selection[primaryIdx] = randomKnativeNonBaselineOptionIndex(rng, specs[primaryIdx])

	secondaryProb := rng.Intn(100)
	if secondaryProb < 30 {
		if secondaryIdx, ok := randomKnativeSpecIndexExcluding(rng, len(specs), selection); ok {
			selection[secondaryIdx] = randomKnativeNonBaselineOptionIndex(rng, specs[secondaryIdx])
		}
	}

	thirdProb := rng.Intn(100)
	if thirdProb < 5 {
		if thirdIdx, ok := randomKnativeSpecIndexExcluding(rng, len(specs), selection); ok {
			selection[thirdIdx] = randomKnativeNonBaselineOptionIndex(rng, specs[thirdIdx])
		}
	}

	return selection
}

func randomKnativeSpecIndexExcluding(rng *rand.Rand, total int, excluded map[int]int) (int, bool) {
	candidates := make([]int, 0, total-len(excluded))
	for i := 0; i < total; i++ {
		if _, blocked := excluded[i]; blocked {
			continue
		}
		candidates = append(candidates, i)
	}
	if len(candidates) == 0 {
		return 0, false
	}
	return candidates[rng.Intn(len(candidates))], true
}

func randomKnativeNonBaselineOptionIndex(rng *rand.Rand, spec knativeParamSpec) int {
	choices := make([]int, 0, len(spec.options))
	for idx, option := range spec.options {
		if option.isBaseline {
			continue
		}
		choices = append(choices, idx)
	}
	if len(choices) == 0 {
		return 0
	}
	return choices[rng.Intn(len(choices))]
}

func knativeSelectionKey(selection map[int]int) string {
	if len(selection) == 0 {
		return ""
	}
	keys := make([]int, 0, len(selection))
	for k := range selection {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%d=%d", k, selection[k]))
	}
	return strings.Join(parts, ",")
}

func validateKnativeServiceParams(svc *v1.Service) error {
	if svc == nil {
		return fmt.Errorf("service is nil")
	}
	if len(svc.Spec.Template.Spec.Containers) == 0 {
		return fmt.Errorf("service template has no containers")
	}

	minScale, hasMin, err := parseScaleAnnotation(svc, autoscaling.MinScaleAnnotationKey)
	if err != nil {
		return err
	}
	maxScale, hasMax, err := parseScaleAnnotation(svc, autoscaling.MaxScaleAnnotationKey)
	if err != nil {
		return err
	}
	if hasMin && hasMax && minScale > maxScale {
		return fmt.Errorf("min scale %d exceeds max scale %d", minScale, maxScale)
	}
	if svc.Spec.Template.Spec.ContainerConcurrency != nil && *svc.Spec.Template.Spec.ContainerConcurrency < 0 {
		return fmt.Errorf("containerConcurrency must be >= 0")
	}
	return nil
}

func parseScaleAnnotation(svc *v1.Service, key string) (int64, bool, error) {
	anns := svc.Spec.Template.ObjectMeta.Annotations
	if len(anns) == 0 {
		return 0, false, nil
	}
	raw := strings.TrimSpace(anns[key])
	if raw == "" {
		return 0, false, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, true, fmt.Errorf("parse %s=%q: %w", key, raw, err)
	}
	if value < 0 {
		return 0, true, fmt.Errorf("%s must be >= 0", key)
	}
	return value, true, nil
}

func buildStateFromCoverageInput(builder *tracecheck.ExplorerBuilder, input coverage.Input) (tracecheck.StateNode, []client.Object, error) {
	objects := make([]client.Object, 0, len(input.EnvironmentState.Objects))
	for _, obj := range input.EnvironmentState.Objects {
		if obj == nil {
			continue
		}
		clone := obj.DeepCopy()
		objects = append(objects, clone)
	}
	if len(objects) == 0 {
		baseSvc := buildBaselineService()
		objects = append(objects, baseSvc)
	}
	if len(objects) == 0 {
		return tracecheck.StateNode{}, nil, fmt.Errorf("input has no objects")
	}

	pending := make([]tracecheck.PendingReconcile, 0)
	state, err := builder.BuildStartStateFromObjects(objects, pending)
	if err != nil {
		return tracecheck.StateNode{}, nil, err
	}
	return state, objects, nil
}


func cloneCoverageInput(input coverage.Input) coverage.Input {
	objects := make([]*unstructured.Unstructured, 0, len(input.EnvironmentState.Objects))
	for _, obj := range input.EnvironmentState.Objects {
		if obj == nil {
			objects = append(objects, nil)
			continue
		}
		objects = append(objects, obj.DeepCopy())
	}
	userInputs := cloneUserInputs(input.ExternalInputs)

	tuning := coverage.InputTuning{
		MaxDepth:           input.Tuning.MaxDepth,
		PermuteControllers: append([]string(nil), input.Tuning.PermuteControllers...),
		StaleReads:         cloneStringSliceMap(input.Tuning.StaleReads),
		StaleLookback:      cloneIntMap(input.Tuning.StaleLookback),
		Search:             cloneInputSearchTuning(input.Tuning.Search),
	}
	return coverage.Input{
		Name: input.Name,
		EnvironmentState: coverage.EnvironmentState{
			Objects: objects,
		},
		ExternalInputs: userInputs,
		Tuning:     tuning,
	}
}

func cloneInputSearchTuning(search coverage.InputSearchTuning) coverage.InputSearchTuning {
	out := coverage.InputSearchTuning{
		Mode: strings.TrimSpace(search.Mode),
	}
	if search.MonteCarlo.Seed != nil {
		seed := *search.MonteCarlo.Seed
		out.MonteCarlo.Seed = &seed
	}
	if search.MonteCarlo.Trials != nil {
		trials := *search.MonteCarlo.Trials
		out.MonteCarlo.Trials = &trials
	}
	if search.MonteCarlo.TrialIndex != nil {
		trialIdx := *search.MonteCarlo.TrialIndex
		out.MonteCarlo.TrialIndex = &trialIdx
	}
	if search.MonteCarlo.ScenarioGroup != nil {
		group := *search.MonteCarlo.ScenarioGroup
		out.MonteCarlo.ScenarioGroup = &group
	}
	return out
}

func buildUserActionsFromCoverageInput(input coverage.Input, seededObjects []client.Object) ([]tracecheck.UserAction, error) {
	actions := make([]tracecheck.UserAction, 0, len(input.ExternalInputs))
	for idx, action := range input.ExternalInputs {
		if action.Object == nil {
			return nil, fmt.Errorf("input user input %d has nil object", idx)
		}
		id := strings.TrimSpace(action.ID)
		if id == "" {
			id = fmt.Sprintf("user-input-%d", idx)
		}
		opType := action.OpType
		if opType == event.CREATE && isInputObjectSeeded(action.Object, seededObjects) {
			opType = event.UPDATE
		}
		actions = append(actions, tracecheck.UserAction{
			ID:      id,
			OpType:  opType,
			Payload: action.Object.DeepCopy(),
		})
	}
	return actions, nil
}

func isInputObjectSeeded(object client.Object, seededObjects []client.Object) bool {
	if object == nil {
		return false
	}
	for _, seeded := range seededObjects {
		if sameObjectIdentity(seeded, object) {
			return true
		}
	}
	return false
}

func sameObjectIdentity(a, b client.Object) bool {
	if a == nil || b == nil {
		return false
	}
	aGVK := a.GetObjectKind().GroupVersionKind()
	bGVK := b.GetObjectKind().GroupVersionKind()
	if aGVK.Group != bGVK.Group || aGVK.Kind != bGVK.Kind {
		return false
	}
	return a.GetNamespace() == b.GetNamespace() && a.GetName() == b.GetName()
}

func cloneUserInputs(inputs []coverage.ExternalInput) []coverage.ExternalInput {
	if len(inputs) == 0 {
		return nil
	}
	out := make([]coverage.ExternalInput, 0, len(inputs))
	for _, input := range inputs {
		clone := coverage.ExternalInput{
			ID:     input.ID,
			Type:   input.OpType,
			Object: nil,
		}
		if input.Object != nil {
			clone.Object = input.Object.DeepCopy()
		}
		out = append(out, clone)
	}
	return out
}

func cloneStringSliceMap(in map[string][]string) map[string][]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string][]string, len(in))
	for k, v := range in {
		out[k] = append([]string(nil), v...)
	}
	return out
}

func cloneIntMap(in map[string]int) map[string]int {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func findKnativeService(objects []*unstructured.Unstructured) int {
	for idx, obj := range objects {
		if obj == nil {
			continue
		}
		if isKnativeService(obj) {
			return idx
		}
	}
	return -1
}

func findKnativeServiceInUserInputs(userInputs []coverage.ExternalInput) int {
	for idx, input := range userInputs {
		if input.Object == nil {
			continue
		}
		if isKnativeService(input.Object) {
			return idx
		}
	}
	return -1
}

func isKnativeService(obj client.Object) bool {
	if obj == nil {
		return false
	}
	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Kind != "Service" {
		return false
	}
	if gvk.Group == "serving.knative.dev" {
		return true
	}
	if u, ok := obj.(*unstructured.Unstructured); ok {
		return strings.HasPrefix(u.GetAPIVersion(), "serving.knative.dev/")
	}
	return false
}

func ensurePrimaryContainer(svc *v1.Service) *corev1.Container {
	containers := svc.Spec.Template.Spec.Containers
	if len(containers) == 0 {
		svc.Spec.Template.Spec.Containers = []corev1.Container{{Name: "user-container", Image: "dev.local/test"}}
	}
	return &svc.Spec.Template.Spec.Containers[0]
}

func ensureTemplateAnnotations(svc *v1.Service) map[string]string {
	anns := svc.Spec.Template.ObjectMeta.Annotations
	if anns == nil {
		anns = map[string]string{}
		svc.Spec.Template.ObjectMeta.Annotations = anns
	}
	return anns
}

func unstructuredToService(obj *unstructured.Unstructured) (*v1.Service, error) {
	if obj == nil {
		return nil, fmt.Errorf("service object is nil")
	}
	var svc v1.Service
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &svc); err != nil {
		return nil, fmt.Errorf("convert service from unstructured: %w", err)
	}
	svc.SetGroupVersionKind(v1.SchemeGroupVersion.WithKind("Service"))
	return &svc, nil
}

func serviceToUnstructured(svc *v1.Service) (*unstructured.Unstructured, error) {
	if svc == nil {
		return nil, fmt.Errorf("service is nil")
	}
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(svc)
	if err != nil {
		return nil, fmt.Errorf("convert service to unstructured: %w", err)
	}
	u := &unstructured.Unstructured{Object: obj}
	u.SetGroupVersionKind(v1.SchemeGroupVersion.WithKind("Service"))
	return u, nil
}
