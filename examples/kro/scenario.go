package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/explore"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	resourceGraphDefinitionControllerID tracecheck.ReconcilerID = "ResourceGraphDefinitionController"
	applicationControllerID             tracecheck.ReconcilerID = "ApplicationController"

	kroDomainName                 = "kro.run"
	applicationAPIVersion         = "kro.run/v1alpha1"
	applicationKind               = "Application"
	resourceGraphDefinitionKind   = "ResourceGraphDefinition"
	resourceGraphDefinitionName   = "my-application"
	applicationCRDName            = "applications.kro.run"
	defaultApplicationName        = "my-app"
	defaultApplicationInstance    = "my-app-instance"
	defaultApplicationNamespace   = "default"
	managedByLabelKey             = "app.kubernetes.io/managed-by"
	instanceNameLabelKey          = "kro.run/instance-name"
	instanceNamespaceLabelKey     = "kro.run/instance-namespace"
	nodeIDLabelKey                = "kro.run/node-id"
	ownedLabelKey                 = "kro.run/owned"
	reconciliationLabelKey        = "kro.run/reconcile"
	reconciliationDisabledValue   = "disabled"
	resourceGraphDefinitionStatus = "Active"
	applicationStateActive        = "Active"
	applicationStateInProgress    = "InProgress"
)

func newKROExplorerBuilder() *tracecheck.ExplorerBuilder {
	scheme := runtime.NewScheme()
	utilruntime.Must(extv1.AddToScheme(scheme))
	utilruntime.Must(networkingv1.AddToScheme(scheme))

	builder := tracecheck.NewExplorerBuilder(scheme)
	builder.WithMaxDepth(30)

	builder.WithReconciler(resourceGraphDefinitionControllerID, func(c client.Client) tracecheck.Reconciler {
		return &resourceGraphDefinitionController{Client: c}
	}).For(kroDomainName + "/" + resourceGraphDefinitionKind)

	builder.WithReconciler(applicationControllerID, func(c client.Client) tracecheck.Reconciler {
		return &applicationController{Client: c}
	}).For(kroDomainName+"/"+applicationKind).
		Watches("apps/Deployment", enqueueApplicationFromManagedResource).
		Watches("Service", enqueueApplicationFromManagedResource).
		Watches("networking.k8s.io/Ingress", enqueueApplicationFromManagedResource)

	builder.WithResourceDep(kroDomainName+"/"+resourceGraphDefinitionKind, resourceGraphDefinitionControllerID)
	builder.WithResourceDep(kroDomainName+"/"+applicationKind, applicationControllerID)

	return builder
}

func buildInitialKROState(builder *tracecheck.ExplorerBuilder) tracecheck.StateNode {
	stateBuilder := builder.NewStateEventBuilder()
	rgd := buildQuickstartApplicationRGD()
	return stateBuilder.AddTopLevelObject(rgd, resourceGraphDefinitionControllerID)
}

func defaultInteractiveUserActions() []tracecheck.UserAction {
	return []tracecheck.UserAction{
		{
			ID:      "create-application-instance",
			OpType:  event.CREATE,
			Payload: buildQuickstartApplicationInstance(),
		},
	}
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

		scenarios = append(scenarios, explore.Scenario{
			Name:             input.Name,
			EnvironmentState: state,
			UserInputs:       userInputs,
			Config:           applyInputTuning(baseCfg, input.Tuning),
		})
	}

	return scenarios, nil
}

func buildStateFromCoverageInput(builder *tracecheck.ExplorerBuilder, input coverage.Input) (tracecheck.StateNode, []client.Object, error) {
	if builder == nil {
		return tracecheck.StateNode{}, nil, fmt.Errorf("builder is nil")
	}

	objects := make([]client.Object, 0, len(input.EnvironmentState.Objects))
	for idx, obj := range input.EnvironmentState.Objects {
		if obj == nil {
			return tracecheck.StateNode{}, nil, fmt.Errorf("input environment object %d is nil", idx)
		}
		objects = append(objects, obj.DeepCopy())
	}

	if len(objects) == 0 {
		for _, action := range input.UserInputs {
			if action.Type != event.CREATE || action.Object == nil {
				continue
			}
			objects = append(objects, action.Object.DeepCopy())
		}
	}

	if len(objects) == 0 {
		return tracecheck.StateNode{}, nil, fmt.Errorf("input has no seedable objects")
	}

	stateBuilder := builder.NewStateEventBuilder()
	state := addSeedObject(stateBuilder, objects[0])
	for _, obj := range objects[1:] {
		state = tracecheck.MergeStateNodes(state, addSeedObject(stateBuilder, obj))
	}
	return state, objects, nil
}

func addSeedObject(builder *tracecheck.StateEventBuilder, obj client.Object) tracecheck.StateNode {
	gvk := obj.GetObjectKind().GroupVersionKind()
	switch {
	case gvk.Group == kroDomainName && gvk.Kind == resourceGraphDefinitionKind:
		return builder.AddTopLevelObject(obj, resourceGraphDefinitionControllerID)
	case gvk.Group == kroDomainName && gvk.Kind == applicationKind:
		return builder.AddTopLevelObject(obj, applicationControllerID)
	default:
		return builder.AddTopLevelObject(obj)
	}
}

func buildUserActionsFromCoverageInput(input coverage.Input, seededObjects []client.Object) ([]tracecheck.UserAction, error) {
	actions := make([]tracecheck.UserAction, 0, len(input.UserInputs))
	for idx, action := range input.UserInputs {
		if action.Object == nil {
			return nil, fmt.Errorf("input user input %d has nil object", idx)
		}

		id := strings.TrimSpace(action.ID)
		if id == "" {
			id = fmt.Sprintf("user-input-%d", idx)
		}

		opType := action.Type
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

func applyInputTuning(base tracecheck.ExploreConfig, tuning coverage.InputTuning) tracecheck.ExploreConfig {
	cfg := base.Clone()
	if tuning.MaxDepth > 0 {
		cfg.MaxDepth = tuning.MaxDepth
	}
	if len(tuning.PermuteControllers) > 0 {
		if cfg.Perturbations.PermuteOrder == nil {
			cfg.Perturbations.PermuteOrder = make(map[tracecheck.ReconcilerID]bool)
		}
		for _, controllerID := range tuning.PermuteControllers {
			cfg.Perturbations.PermuteOrder[tracecheck.ReconcilerID(controllerID)] = true
		}
	}
	if len(tuning.StaleReads) > 0 {
		if cfg.Perturbations.Staleness == nil {
			cfg.Perturbations.Staleness = make(map[tracecheck.ReconcilerID]tracecheck.StalenessConfig)
		}
		for controllerID, kinds := range tuning.StaleReads {
			id := tracecheck.ReconcilerID(controllerID)
			st := cfg.Perturbations.Staleness[id]
			if st.StaleReadBounds == nil {
				st.StaleReadBounds = make(tracecheck.LookbackLimits)
			}
			for _, kind := range kinds {
				trimmed := strings.TrimSpace(kind)
				if trimmed == "" {
					continue
				}
				lookback := tuning.StaleLookback[trimmed]
				if lookback <= 0 {
					lookback = 1
				}
				st.StaleReadBounds[trimmed] = tracecheck.LookbackLimit(lookback)
			}
			cfg.Perturbations.Staleness[id] = st
		}
	}
	return cfg
}

type resourceGraphDefinitionController struct {
	client.Client
}

func (c *resourceGraphDefinitionController) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	rgd := &unstructured.Unstructured{}
	rgd.SetAPIVersion(applicationAPIVersion)
	rgd.SetKind(resourceGraphDefinitionKind)
	if err := c.Get(ctx, req.NamespacedName, rgd); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}

	crd := buildApplicationCRD()
	current := &extv1.CustomResourceDefinition{}
	err := c.Get(ctx, client.ObjectKey{Name: crd.Name}, current)
	switch {
	case apierrors.IsNotFound(err):
		if err := c.Create(ctx, crd); err != nil {
			return reconcile.Result{}, err
		}
	case err != nil:
		return reconcile.Result{}, err
	default:
		if !updateCRDIfChanged(current, crd) {
			break
		}
		if err := c.Update(ctx, current); err != nil {
			return reconcile.Result{}, err
		}
	}

	if !setResourceGraphDefinitionStatus(rgd) {
		return reconcile.Result{}, nil
	}
	return reconcile.Result{}, c.Update(ctx, rgd)
}

func setResourceGraphDefinitionStatus(rgd *unstructured.Unstructured) bool {
	original := rgd.DeepCopy()
	setNestedField(rgd.Object, resourceGraphDefinitionStatus, "status", "state")
	setNestedStringSlice(rgd.Object, []string{"deployment", "service", "ingress"}, "status", "topologicalOrder")
	setNestedField(rgd.Object, []any{
		map[string]any{"id": "deployment"},
		map[string]any{"id": "service", "dependencies": []any{map[string]any{"id": "deployment"}}},
		map[string]any{"id": "ingress", "dependencies": []any{map[string]any{"id": "service"}}},
	}, "status", "resources")
	return !apiequality.Semantic.DeepEqual(original.Object, rgd.Object)
}

type applicationController struct {
	client.Client
}

func (c *applicationController) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	app := &unstructured.Unstructured{}
	app.SetAPIVersion(applicationAPIVersion)
	app.SetKind(applicationKind)
	if err := c.Get(ctx, req.NamespacedName, app); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}

	if strings.EqualFold(app.GetLabels()[reconciliationLabelKey], reconciliationDisabledValue) {
		return reconcile.Result{}, nil
	}

	spec := applicationSpecFromObject(app)
	deployment := buildApplicationDeployment(app, spec)
	service := buildApplicationService(app, spec)
	if err := c.reconcileDeployment(ctx, deployment); err != nil {
		return reconcile.Result{}, err
	}
	if err := c.reconcileService(ctx, service); err != nil {
		return reconcile.Result{}, err
	}
	if spec.IngressEnabled {
		if err := c.reconcileIngress(ctx, buildApplicationIngress(app, spec)); err != nil {
			return reconcile.Result{}, err
		}
	} else if err := c.deleteIngressIfPresent(ctx, buildApplicationIngress(app, spec)); err != nil {
		return reconcile.Result{}, err
	}

	if err := c.updateApplicationStatus(ctx, app, deployment.Namespace, deployment.Name, spec.Replicas); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

func (c *applicationController) reconcileDeployment(ctx context.Context, desired *appsv1.Deployment) error {
	current := &appsv1.Deployment{}
	err := c.Get(ctx, client.ObjectKeyFromObject(desired), current)
	if apierrors.IsNotFound(err) {
		return c.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	if apiequality.Semantic.DeepEqual(current.Labels, desired.Labels) &&
		apiequality.Semantic.DeepEqual(current.Annotations, desired.Annotations) &&
		apiequality.Semantic.DeepEqual(current.Spec, desired.Spec) {
		return nil
	}
	current.Labels = desired.Labels
	current.Annotations = desired.Annotations
	current.Spec = desired.Spec
	return c.Update(ctx, current)
}

func (c *applicationController) reconcileService(ctx context.Context, desired *corev1.Service) error {
	current := &corev1.Service{}
	err := c.Get(ctx, client.ObjectKeyFromObject(desired), current)
	if apierrors.IsNotFound(err) {
		return c.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	if apiequality.Semantic.DeepEqual(current.Labels, desired.Labels) &&
		apiequality.Semantic.DeepEqual(current.Annotations, desired.Annotations) &&
		apiequality.Semantic.DeepEqual(current.Spec.Selector, desired.Spec.Selector) &&
		apiequality.Semantic.DeepEqual(current.Spec.Ports, desired.Spec.Ports) &&
		current.Spec.Type == desired.Spec.Type {
		return nil
	}
	current.Labels = desired.Labels
	current.Annotations = desired.Annotations
	current.Spec.Selector = desired.Spec.Selector
	current.Spec.Ports = desired.Spec.Ports
	current.Spec.Type = desired.Spec.Type
	return c.Update(ctx, current)
}

func (c *applicationController) reconcileIngress(ctx context.Context, desired *networkingv1.Ingress) error {
	current := &networkingv1.Ingress{}
	err := c.Get(ctx, client.ObjectKeyFromObject(desired), current)
	if apierrors.IsNotFound(err) {
		return c.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	if apiequality.Semantic.DeepEqual(current.Labels, desired.Labels) &&
		apiequality.Semantic.DeepEqual(current.Annotations, desired.Annotations) &&
		apiequality.Semantic.DeepEqual(current.Spec, desired.Spec) {
		return nil
	}
	current.Labels = desired.Labels
	current.Annotations = desired.Annotations
	current.Spec = desired.Spec
	return c.Update(ctx, current)
}

func (c *applicationController) deleteIngressIfPresent(ctx context.Context, ingress *networkingv1.Ingress) error {
	current := &networkingv1.Ingress{}
	err := c.Get(ctx, client.ObjectKeyFromObject(ingress), current)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return c.Delete(ctx, current)
}

func (c *applicationController) updateApplicationStatus(ctx context.Context, app *unstructured.Unstructured, namespace, name string, desiredReplicas int32) error {
	original := app.DeepCopy()
	// Use float64 for numeric status fields so that the value matches what
	// the store returns after JSON round-tripping (JSON numbers decode as float64
	// in unstructured maps).
	availableReplicas := float64(0)
	state := applicationStateInProgress

	deployment := &appsv1.Deployment{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, deployment); err != nil && !apierrors.IsNotFound(err) {
		return err
	} else if err == nil {
		availableReplicas = float64(deployment.Status.AvailableReplicas)
		if deployment.Status.AvailableReplicas >= desiredReplicas && desiredReplicas > 0 {
			state = applicationStateActive
		}
		if deployment.Status.AvailableReplicas == 0 && desiredReplicas == 0 {
			state = applicationStateActive
		}
	}

	setNestedField(app.Object, state, "status", "state")
	setNestedField(app.Object, availableReplicas, "status", "availableReplicas")
	if apiequality.Semantic.DeepEqual(original.Object, app.Object) {
		return nil
	}
	return c.Update(ctx, app)
}

func updateCRDIfChanged(current, desired *extv1.CustomResourceDefinition) bool {
	if apiequality.Semantic.DeepEqual(current.Labels, desired.Labels) &&
		apiequality.Semantic.DeepEqual(current.Annotations, desired.Annotations) &&
		apiequality.Semantic.DeepEqual(current.Spec, desired.Spec) {
		return false
	}
	current.Labels = desired.Labels
	current.Annotations = desired.Annotations
	current.Spec = desired.Spec
	return true
}

func enqueueApplicationFromManagedResource(obj *unstructured.Unstructured) []reconcile.Request {
	if obj == nil {
		return nil
	}
	labels := obj.GetLabels()
	instanceName := strings.TrimSpace(labels[instanceNameLabelKey])
	if instanceName == "" {
		return nil
	}
	namespace := labels[instanceNamespaceLabelKey]
	if namespace == "" {
		namespace = obj.GetNamespace()
	}
	return []reconcile.Request{{
		NamespacedName: client.ObjectKey{Namespace: namespace, Name: instanceName},
	}}
}

type applicationSpec struct {
	Name           string
	Image          string
	Replicas       int32
	IngressEnabled bool
}

func applicationSpecFromObject(app *unstructured.Unstructured) applicationSpec {
	name := getNestedString(app.Object, "spec", "name")
	if name == "" {
		name = defaultApplicationName
	}
	image := getNestedString(app.Object, "spec", "image")
	if image == "" {
		image = "nginx"
	}
	replicas := getNestedInt32(app.Object, 3, "spec", "replicas")
	return applicationSpec{
		Name:           name,
		Image:          image,
		Replicas:       replicas,
		IngressEnabled: getNestedBool(app.Object, false, "spec", "ingress", "enabled"),
	}
}

func buildApplicationDeployment(app *unstructured.Unstructured, spec applicationSpec) *appsv1.Deployment {
	labels := managedResourceLabels(app, "deployment", spec.Name)
	return &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{APIVersion: appsv1.SchemeGroupVersion.String(), Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      spec.Name,
			Namespace: defaultNamespace(app.GetNamespace()),
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &spec.Replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": spec.Name},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": spec.Name},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  spec.Name,
						Image: spec.Image,
						Ports: []corev1.ContainerPort{{ContainerPort: 80}},
					}},
				},
			},
		},
	}
}

func buildApplicationService(app *unstructured.Unstructured, spec applicationSpec) *corev1.Service {
	return &corev1.Service{
		TypeMeta: metav1.TypeMeta{APIVersion: corev1.SchemeGroupVersion.String(), Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      spec.Name + "-svc",
			Namespace: defaultNamespace(app.GetNamespace()),
			Labels:    managedResourceLabels(app, "service", spec.Name),
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: map[string]string{"app": spec.Name},
			Ports: []corev1.ServicePort{{
				Protocol:   corev1.ProtocolTCP,
				Port:       80,
				TargetPort: intstr.FromInt(80),
			}},
		},
	}
}

func buildApplicationIngress(app *unstructured.Unstructured, spec applicationSpec) *networkingv1.Ingress {
	pathType := networkingv1.PathTypePrefix
	return &networkingv1.Ingress{
		TypeMeta: metav1.TypeMeta{APIVersion: networkingv1.SchemeGroupVersion.String(), Kind: "Ingress"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      spec.Name + "-ingress",
			Namespace: defaultNamespace(app.GetNamespace()),
			Labels:    managedResourceLabels(app, "ingress", spec.Name),
			Annotations: map[string]string{
				"kubernetes.io/ingress.class":                       "alb",
				"alb.ingress.kubernetes.io/scheme":                  "internet-facing",
				"alb.ingress.kubernetes.io/target-type":             "ip",
				"alb.ingress.kubernetes.io/healthcheck-path":        "/health",
				"alb.ingress.kubernetes.io/listen-ports":            `[{"HTTP": 80}]`,
				"alb.ingress.kubernetes.io/target-group-attributes": "stickiness.enabled=true,stickiness.lb_cookie.duration_seconds=60",
			},
		},
		Spec: networkingv1.IngressSpec{
			Rules: []networkingv1.IngressRule{{
				IngressRuleValue: networkingv1.IngressRuleValue{
					HTTP: &networkingv1.HTTPIngressRuleValue{
						Paths: []networkingv1.HTTPIngressPath{{
							Path:     "/",
							PathType: &pathType,
							Backend: networkingv1.IngressBackend{
								Service: &networkingv1.IngressServiceBackend{
									Name: spec.Name + "-svc",
									Port: networkingv1.ServiceBackendPort{Number: 80},
								},
							},
						}},
					},
				},
			}},
		},
	}
}

func managedResourceLabels(app *unstructured.Unstructured, nodeID, appName string) map[string]string {
	return map[string]string{
		managedByLabelKey:         "kro",
		ownedLabelKey:             "true",
		instanceNameLabelKey:      app.GetName(),
		instanceNamespaceLabelKey: defaultNamespace(app.GetNamespace()),
		nodeIDLabelKey:            nodeID,
		"app":                     appName,
	}
}

func buildQuickstartApplicationRGD() *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": applicationAPIVersion,
			"kind":       resourceGraphDefinitionKind,
			"metadata": map[string]any{
				"name": resourceGraphDefinitionName,
			},
			"spec": map[string]any{
				"schema": map[string]any{
					"apiVersion": "v1alpha1",
					"kind":       applicationKind,
					"spec": map[string]any{
						"name":     "string",
						"image":    `string | default="nginx"`,
						"replicas": "integer | default=3",
						"ingress": map[string]any{
							"enabled": "boolean | default=false",
						},
					},
					"status": map[string]any{
						"deploymentConditions": `${deployment.status.conditions}`,
						"availableReplicas":    `${deployment.status.availableReplicas}`,
					},
				},
			},
		},
	}
}

func buildQuickstartApplicationInstance() *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": applicationAPIVersion,
			"kind":       applicationKind,
			"metadata": map[string]any{
				"name":      defaultApplicationInstance,
				"namespace": defaultApplicationNamespace,
			},
			"spec": map[string]any{
				"name":     defaultApplicationName,
				"replicas": int64(1),
				"ingress": map[string]any{
					"enabled": true,
				},
			},
		},
	}
}

func buildApplicationCRD() *extv1.CustomResourceDefinition {
	scope := extv1.NamespaceScoped
	return &extv1.CustomResourceDefinition{
		TypeMeta: metav1.TypeMeta{
			APIVersion: extv1.SchemeGroupVersion.String(),
			Kind:       "CustomResourceDefinition",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: applicationCRDName,
			Labels: map[string]string{
				managedByLabelKey: "kro",
				ownedLabelKey:     "true",
			},
		},
		Spec: extv1.CustomResourceDefinitionSpec{
			Group: kroDomainName,
			Names: extv1.CustomResourceDefinitionNames{
				Plural:     "applications",
				Singular:   "application",
				Kind:       applicationKind,
				ShortNames: []string{"app"},
			},
			Scope: scope,
			Versions: []extv1.CustomResourceDefinitionVersion{{
				Name:    "v1alpha1",
				Served:  true,
				Storage: true,
				Schema: &extv1.CustomResourceValidation{
					OpenAPIV3Schema: &extv1.JSONSchemaProps{
						Type: "object",
						Properties: map[string]extv1.JSONSchemaProps{
							"spec": {
								Type: "object",
								Properties: map[string]extv1.JSONSchemaProps{
									"name":     {Type: "string"},
									"image":    {Type: "string"},
									"replicas": {Type: "integer"},
									"ingress": {
										Type: "object",
										Properties: map[string]extv1.JSONSchemaProps{
											"enabled": {Type: "boolean"},
										},
									},
								},
							},
							"status": {
								Type: "object",
								Properties: map[string]extv1.JSONSchemaProps{
									"state":             {Type: "string"},
									"availableReplicas": {Type: "integer"},
								},
							},
						},
					},
				},
				Subresources: &extv1.CustomResourceSubresources{
					Status: &extv1.CustomResourceSubresourceStatus{},
				},
			}},
		},
		Status: extv1.CustomResourceDefinitionStatus{
			Conditions: []extv1.CustomResourceDefinitionCondition{{
				Type:   extv1.Established,
				Status: extv1.ConditionTrue,
				Reason: "KameraSimulated",
			}},
			AcceptedNames: extv1.CustomResourceDefinitionNames{
				Plural:   "applications",
				Singular: "application",
				Kind:     applicationKind,
			},
		},
	}
}

func getNestedString(obj map[string]any, fields ...string) string {
	value, found, err := unstructured.NestedString(obj, fields...)
	if err != nil || !found {
		return ""
	}
	return value
}

func getNestedBool(obj map[string]any, fallback bool, fields ...string) bool {
	value, found, err := unstructured.NestedBool(obj, fields...)
	if err != nil || !found {
		return fallback
	}
	return value
}

func getNestedInt32(obj map[string]any, fallback int32, fields ...string) int32 {
	value, found, err := unstructured.NestedInt64(obj, fields...)
	if err == nil && found {
		return int32(value)
	}
	// JSON round-tripping converts int64 to float64; handle that gracefully.
	raw, rawFound, rawErr := unstructured.NestedFieldNoCopy(obj, fields...)
	if rawErr != nil || !rawFound {
		return fallback
	}
	if f, ok := raw.(float64); ok {
		return int32(f)
	}
	return fallback
}

func setNestedStringSlice(obj map[string]any, value []string, fields ...string) {
	items := make([]any, 0, len(value))
	for _, item := range value {
		items = append(items, item)
	}
	_ = unstructured.SetNestedSlice(obj, items, fields...)
}

func setNestedField(obj map[string]any, value any, fields ...string) {
	_ = unstructured.SetNestedField(obj, value, fields...)
}

func defaultNamespace(namespace string) string {
	if strings.TrimSpace(namespace) == "" {
		return defaultApplicationNamespace
	}
	return namespace
}
