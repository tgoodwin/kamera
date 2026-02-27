package main

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func TestControllerForObject_PromiseMapsToPromiseController(t *testing.T) {
	obj := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "platform.kratix.io/v1alpha1",
			"kind":       "Promise",
			"metadata": map[string]any{
				"name":      "p",
				"namespace": "default",
			},
		},
	}
	got, ok := controllerForObject(obj)
	if !ok {
		t.Fatalf("controllerForObject() ok = false, want true")
	}
	if got != promiseControllerID {
		t.Fatalf("controllerForObject() = %q, want %q", got, promiseControllerID)
	}
}

func TestInitialPendingReconciles_SeedsKnownKinds(t *testing.T) {
	objects := []ctrlclient.Object{
		&unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "platform.kratix.io/v1alpha1",
				"kind":       "Promise",
				"metadata": map[string]any{
					"name":      "p",
					"namespace": "default",
				},
			},
		},
		&unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "platform.kratix.io/v1alpha1",
				"kind":       "Work",
				"metadata": map[string]any{
					"name":      "w",
					"namespace": "default",
				},
			},
		},
		&unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "example.promise.syntasso.io/v1",
				"kind":       "EasyApp",
				"metadata": map[string]any{
					"name":      "example",
					"namespace": "default",
				},
			},
		},
	}

	pending := initialPendingReconciles(objects)
	if len(pending) != 2 {
		t.Fatalf("len(initialPendingReconciles()) = %d, want 2", len(pending))
	}
	if pending[0].ReconcilerID != promiseControllerID {
		t.Fatalf("pending[0].ReconcilerID = %q, want PromiseController", pending[0].ReconcilerID)
	}
	if pending[1].ReconcilerID != workControllerID {
		t.Fatalf("pending[1].ReconcilerID = %q, want WorkController", pending[1].ReconcilerID)
	}
}

func TestDynamicControllerSpecForPromise_WithAPI(t *testing.T) {
	obj := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "platform.kratix.io/v1alpha1",
			"kind":       "Promise",
			"metadata": map[string]any{
				"name": "easyapp",
			},
			"spec": map[string]any{
				"api": map[string]any{
					"apiVersion": "apiextensions.k8s.io/v1",
					"kind":       "CustomResourceDefinition",
					"metadata": map[string]any{
						"name": "easyapps.example.promise.syntasso.io",
					},
					"spec": map[string]any{
						"group": "example.promise.syntasso.io",
						"names": map[string]any{
							"kind":     "EasyApp",
							"plural":   "easyapps",
							"singular": "easyapp",
						},
						"scope": "Namespaced",
						"versions": []any{
							map[string]any{
								"name":    "v1",
								"served":  true,
								"storage": true,
							},
						},
					},
				},
			},
		},
	}

	spec, ok := dynamicControllerSpecForPromise(obj)
	if !ok {
		t.Fatalf("dynamicControllerSpecForPromise() ok = false, want true")
	}
	if spec.key == "" {
		t.Fatalf("dynamicControllerSpecForPromise() returned empty key")
	}
	if spec.controllerID == "" {
		t.Fatalf("dynamicControllerSpecForPromise() returned empty controller ID")
	}
	if spec.gvk == nil || spec.crd == nil || spec.placeholder == nil {
		t.Fatalf("dynamicControllerSpecForPromise() missing required fields")
	}
}
