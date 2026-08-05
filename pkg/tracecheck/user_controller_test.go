package tracecheck

import (
	"context"
	"reflect"
	"testing"

	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/util"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestUserAction_DataOnlyShape(t *testing.T) {
	typeInfo := reflect.TypeOf(UserAction{})
	if typeInfo.NumField() != 3 {
		t.Fatalf("expected UserAction to have exactly 3 fields, got %d", typeInfo.NumField())
	}

	required := map[string]bool{
		"ID":      false,
		"OpType":  false,
		"Payload": false,
	}

	for i := 0; i < typeInfo.NumField(); i++ {
		field := typeInfo.Field(i)
		if _, ok := required[field.Name]; ok {
			required[field.Name] = true
		}
		if field.Type.Kind() == reflect.Func {
			t.Fatalf("UserAction must be data-only; found function field %q", field.Name)
		}
	}

	for fieldName, found := range required {
		if !found {
			t.Fatalf("expected UserAction field %q to exist", fieldName)
		}
	}
}

func TestExplorerBuild_InitializesUserController(t *testing.T) {
	builder := NewExplorerBuilder(runtime.NewScheme())
	explorer, err := builder.Build("standalone")
	if err != nil {
		t.Fatalf("build explorer: %v", err)
	}

	if explorer.userController == nil {
		t.Fatalf("expected explorer to own exactly one user controller instance")
	}
	if _, ok := explorer.userController.container.Strategy.(*ControllerRuntimeStrategy); !ok {
		t.Fatalf("expected user controller to reuse ControllerRuntimeStrategy")
	}
	fromReconcilers, ok := explorer.reconcilers[UserControllerID]
	if !ok {
		t.Fatalf("expected user controller reconciler to come from instantiateReconcilers map")
	}
	if fromReconcilers != explorer.userController.container {
		t.Fatalf("expected explorer.userController.container to reference reconciler container from instantiateReconcilers")
	}
}

func TestUserController_ExecuteNextAction_ByBranchIndex(t *testing.T) {
	builder := NewExplorerBuilder(runtime.NewScheme())
	builder.WithUserActions([]UserAction{{
		ID:     "create-default-cm",
		OpType: event.CREATE,
		Payload: &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "created-by-user-action",
			},
		},
	}})

	explorer, err := builder.Build("standalone")
	if err != nil {
		t.Fatalf("build explorer: %v", err)
	}

	result, err := explorer.userController.ExecuteNextAction(context.Background(), ObjectVersions{}, 0)
	if err != nil {
		t.Fatalf("execute next action: %v", err)
	}

	if result.ControllerID != UserControllerID {
		t.Fatalf("expected controller id %q, got %q", UserControllerID, result.ControllerID)
	}
	if len(result.Changes.Effects) != 1 {
		t.Fatalf("expected exactly one effect, got %d", len(result.Changes.Effects))
	}
	if result.Changes.Effects[0].OpType != event.CREATE {
		t.Fatalf("expected CREATE effect, got %q", result.Changes.Effects[0].OpType)
	}
	if len(result.Changes.ObjectVersions) != 1 {
		t.Fatalf("expected exactly one changed object version, got %d", len(result.Changes.ObjectVersions))
	}

	if _, err := explorer.userController.ExecuteNextAction(context.Background(), ObjectVersions{}, 1); err == nil {
		t.Fatalf("expected out-of-range branch index to return an error")
	}
}

func TestUserActionReconciler_Reconcile_SelectsActionByRequestIndex(t *testing.T) {
	r := &userActionReconciler{
		actions: []UserAction{
			{ID: "a0", OpType: event.INIT},
			{ID: "a1", OpType: event.INIT},
		},
	}

	action, err := r.actionForRequest(reconcile.Request{NamespacedName: types.NamespacedName{Name: "1"}})
	if err != nil {
		t.Fatalf("actionForRequest: %v", err)
	}
	if action.ID != "a1" {
		t.Fatalf("expected action ID a1 from request index 1, got %q", action.ID)
	}
}

func TestUserActionUpdatePayloadIsIsolatedAcrossBranches(t *testing.T) {
	payload := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "shared"},
		Data:       map[string]string{"desired": "value"},
	}
	builder := NewExplorerBuilder(runtime.NewScheme())
	builder.WithUserActions([]UserAction{{ID: "update-shared", OpType: event.UPDATE, Payload: payload}})
	explorer, err := builder.Build("standalone")
	if err != nil {
		t.Fatalf("build explorer: %v", err)
	}

	key := snapshot.NewCompositeKeyWithGroup("", "ConfigMap", "default", "shared", "obj1")
	branchObject := func(value string, rv int64) ObjectVersions {
		obj := &corev1.ConfigMap{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "shared"},
			Data:       map[string]string{"branch": value},
		}
		u, convertErr := util.ConvertToUnstructured(obj)
		if convertErr != nil {
			t.Fatalf("convert branch object: %v", convertErr)
		}
		hash := explorer.versionManager.Publish(u)
		explorer.resourceVersions[hash] = rv
		return ObjectVersions{key: hash}
	}

	firstCtx := withResourceVersionBase(context.Background(), 5)
	if _, err := explorer.userController.ExecuteNextAction(firstCtx, branchObject("one", 5), 0); err != nil {
		t.Fatalf("execute first branch: %v", err)
	}
	if payload.GetResourceVersion() != "" {
		t.Fatalf("first branch mutated shared payload RV to %q", payload.GetResourceVersion())
	}

	secondCtx := withResourceVersionBase(context.Background(), 9)
	if _, err := explorer.userController.ExecuteNextAction(secondCtx, branchObject("two", 9), 0); err != nil {
		t.Fatalf("execute second branch: %v", err)
	}
	if payload.GetResourceVersion() != "" {
		t.Fatalf("second branch mutated shared payload RV to %q", payload.GetResourceVersion())
	}
}
