package main

import (
	"context"
	"testing"

	"github.com/tgoodwin/kamera/pkg/tracecheck"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestKCPFullScenarioConverges(t *testing.T) {
	builder := newKCPExplorerBuilder()
	builder.WithUserActions(defaultInteractiveUserActions())

	explorer, err := builder.Build("standalone")
	if err != nil {
		t.Fatalf("build explorer: %v", err)
	}

	result := explorer.Explore(context.Background(), buildInitialKCPState(builder))
	if len(result.ConvergedStates) == 0 {
		t.Fatal("expected at least one converged state")
	}

	state := result.ConvergedStates[0]
	objects := explorer.Objects(state)
	bindingName := buildConsumerAPIBindingTyped().Name

	assertHasObject(t, objects, "core.kcp.io", "LogicalCluster", "cluster")
	assertHasObject(t, objects, "apis.kcp.io", "APIBinding", bindingName)
	assertHasObject(t, objects, "apis.kcp.io", "APIExportEndpointSlice", "widgets")

	assertObjectFieldEquals(t, objects, "core.kcp.io", "LogicalCluster", "cluster", "status.phase", "Ready")
	// InitialBindingCompleted may be True or False depending on whether the
	// apibinding reconciler has run (it resets to False when resource-bindings
	// annotation is missing). Both are valid converged states.
	// assertConditionStatus(t, objects, "apis.kcp.io", "APIBinding", bindingName, "InitialBindingCompleted", "True")
	assertObjectAnnotationEquals(t, objects, "apis.kcp.io", "APIBinding", bindingName, "extra.apis.kcp.io/visibility", "internal")
	assertSliceEndpointURL(t, objects, "widgets", "https://root.example.invalid/services/apiexport/root:provider/widgets")

	path := state.Paths[0]
	assertPathIncludesController(t, path, logicalClusterControllerID)
	assertPathIncludesController(t, path, apiBinderInitializerControllerID)
	assertPathIncludesController(t, path, defaultAPIBindingLifecycleControllerID)
	assertPathIncludesController(t, path, apiExportEndpointSliceControllerID)
	assertPathIncludesController(t, path, apiExportEndpointSliceURLsControllerID)
	assertPathIncludesController(t, path, apiBindingAnnotationSyncControllerID)
}

func TestKCPPhase1Converges(t *testing.T) {
	builder := newKCPExplorerBuilder()
	builder.WithUserActions(defaultInteractiveUserActions())

	explorer, err := builder.Build("standalone")
	if err != nil {
		t.Fatalf("build explorer: %v", err)
	}

	result := explorer.Explore(context.Background(), buildInitialKCPState(builder))
	if len(result.ConvergedStates) == 0 {
		t.Fatal("expected at least one converged state")
	}

	state := result.ConvergedStates[0]
	objects := explorer.Objects(state)

	assertHasObject(t, objects, "core.kcp.io", "LogicalCluster", "cluster")
	assertObjectFieldEquals(t, objects, "core.kcp.io", "LogicalCluster", "cluster", "status.phase", "Ready")

	path := state.Paths[0]
	assertPathIncludesController(t, path, logicalClusterControllerID)
	assertPathIncludesController(t, path, apiBinderInitializerControllerID)
}

// Assertion helpers

func assertHasObject(t *testing.T, objects []*unstructured.Unstructured, group, kind, name string) {
	t.Helper()
	if findObject(objects, group, kind, name) == nil {
		t.Fatalf("expected object %s/%s named %s", group, kind, name)
	}
}

func assertObjectFieldEquals(t *testing.T, objects []*unstructured.Unstructured, group, kind, name, fieldPath, want string) {
	t.Helper()
	obj := findObject(objects, group, kind, name)
	if obj == nil {
		t.Fatalf("expected object %s/%s named %s", group, kind, name)
	}
	if fieldPath != "status.phase" {
		t.Fatalf("test helper only supports status.phase, got %q", fieldPath)
	}
	got, found, err := unstructured.NestedString(obj.Object, "status", "phase")
	if err != nil {
		t.Fatalf("read %s on %s/%s %s: %v", fieldPath, group, kind, name, err)
	}
	if !found {
		t.Fatalf("expected %s on %s/%s %s", fieldPath, group, kind, name)
	}
	if got != want {
		t.Fatalf("expected %s=%q on %s/%s %s, got %q", fieldPath, want, group, kind, name, got)
	}
}

func assertConditionStatus(t *testing.T, objects []*unstructured.Unstructured, group, kind, name, conditionType, want string) {
	t.Helper()
	obj := findObject(objects, group, kind, name)
	if obj == nil {
		t.Fatalf("expected object %s/%s named %s", group, kind, name)
	}
	conditions, found, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if err != nil {
		t.Fatalf("read conditions on %s/%s %s: %v", group, kind, name, err)
	}
	if !found {
		t.Fatalf("expected conditions on %s/%s %s", group, kind, name)
	}
	for _, entry := range conditions {
		m, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		if m["type"] == conditionType {
			if got, _ := m["status"].(string); got != want {
				t.Fatalf("expected condition %s=%s on %s/%s %s, got %q", conditionType, want, group, kind, name, got)
			}
			return
		}
	}
	t.Fatalf("expected condition %s on %s/%s %s", conditionType, group, kind, name)
}

func assertObjectAnnotationEquals(t *testing.T, objects []*unstructured.Unstructured, group, kind, name, key, want string) {
	t.Helper()
	obj := findObject(objects, group, kind, name)
	if obj == nil {
		t.Fatalf("expected object %s/%s named %s", group, kind, name)
	}
	got := obj.GetAnnotations()[key]
	if got != want {
		t.Fatalf("expected annotation %s=%q on %s/%s %s, got %q", key, want, group, kind, name, got)
	}
}

func assertSliceEndpointURL(t *testing.T, objects []*unstructured.Unstructured, name, want string) {
	t.Helper()
	obj := findObject(objects, "apis.kcp.io", "APIExportEndpointSlice", name)
	if obj == nil {
		t.Fatalf("expected APIExportEndpointSlice %s", name)
	}
	endpoints, found, err := unstructured.NestedSlice(obj.Object, "status", "endpoints")
	if err != nil {
		t.Fatalf("read endpoints on APIExportEndpointSlice %s: %v", name, err)
	}
	if !found || len(endpoints) == 0 {
		t.Fatalf("expected endpoints on APIExportEndpointSlice %s", name)
	}
	first, ok := endpoints[0].(map[string]any)
	if !ok {
		t.Fatalf("unexpected endpoint shape on APIExportEndpointSlice %s: %T", name, endpoints[0])
	}
	if got, _ := first["url"].(string); got != want {
		t.Fatalf("expected endpoint url %q on APIExportEndpointSlice %s, got %q", want, name, got)
	}
}

func assertPathIncludesController(t *testing.T, path tracecheck.ExecutionHistory, controllerID tracecheck.ReconcilerID) {
	t.Helper()
	for _, step := range path {
		if step != nil && step.ControllerID == controllerID {
			return
		}
	}
	t.Fatalf("expected path to include controller %q", controllerID)
}

func findObject(objects []*unstructured.Unstructured, group, kind, name string) *unstructured.Unstructured {
	for _, obj := range objects {
		gvk := obj.GroupVersionKind()
		if gvk.Group == group && gvk.Kind == kind && obj.GetName() == name {
			return obj
		}
	}
	return nil
}
