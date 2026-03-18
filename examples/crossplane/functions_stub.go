package main

import (
	"context"
	"fmt"

	fnv1 "github.com/crossplane/crossplane/v2/proto/fn/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

const stubFunctionName = "kamera-stub"

// stubFunctionRunner supports multiple function behaviors controlled by function
// name. Scenarios use different function names in the Composition pipeline to
// test different function behaviors without changing harness code.
//
// Supported function names:
//   - "kamera-stub": returns ConfigMap xr-config + status Composed (default)
//   - "kamera-stub-fatal": returns SEVERITY_FATAL result (function failure)
//   - "kamera-stub-different-resources": returns Secret instead of ConfigMap
//     (tests composed resource GC when function changes its output)
//   - "kamera-stub-partial": returns ConfigMap as READY_TRUE and Secret as
//     READY_UNSPECIFIED (tests partial readiness propagation)
type stubFunctionRunner struct{}

func (s stubFunctionRunner) RunFunction(_ context.Context, name string, req *fnv1.RunFunctionRequest) (*fnv1.RunFunctionResponse, error) {
	switch name {
	case stubFunctionName:
		return defaultResponse(), nil
	case stubFunctionName + "-fatal":
		return fatalResponse(), nil
	case stubFunctionName + "-different-resources":
		return differentResourcesResponse(), nil
	case stubFunctionName + "-partial":
		return partialReadinessResponse(), nil
	default:
		return nil, fmt.Errorf("unexpected function name %q", name)
	}
}

// defaultResponse returns ConfigMap xr-config with status Composed.
func defaultResponse() *fnv1.RunFunctionResponse {
	return &fnv1.RunFunctionResponse{
		Desired: &fnv1.State{
			Composite: composedXRStatus(),
			Resources: map[string]*fnv1.Resource{
				"config": configMapResource(),
			},
		},
	}
}

// fatalResponse returns a SEVERITY_FATAL result — the pipeline stops and no
// resources are applied.
func fatalResponse() *fnv1.RunFunctionResponse {
	return &fnv1.RunFunctionResponse{
		Results: []*fnv1.Result{
			{
				Severity: fnv1.Severity_SEVERITY_FATAL,
				Message:  "simulated function failure",
			},
		},
	}
}

// differentResourcesResponse returns a Secret instead of a ConfigMap. When this
// replaces the default function in a Composition update, the GC should delete
// the old ConfigMap and create the new Secret.
func differentResourcesResponse() *fnv1.RunFunctionResponse {
	return &fnv1.RunFunctionResponse{
		Desired: &fnv1.State{
			Composite: composedXRStatus(),
			Resources: map[string]*fnv1.Resource{
				"credentials": {
					Resource: mustStruct(map[string]any{
						"apiVersion": "v1",
						"kind":       "Secret",
						"metadata": map[string]any{
							"name":      "xr-credentials",
							"namespace": "default",
							"labels": map[string]any{
								"app": "kamera",
							},
						},
						"data": map[string]any{
							"token": "a2FtZXJhLXN0dWI=",
						},
					}),
					Ready: fnv1.Ready_READY_TRUE,
				},
			},
		},
	}
}

// partialReadinessResponse returns two resources: ConfigMap (READY_TRUE) and
// Secret (READY_UNSPECIFIED). Tests whether partial readiness is correctly
// reflected in XR status.
func partialReadinessResponse() *fnv1.RunFunctionResponse {
	return &fnv1.RunFunctionResponse{
		Desired: &fnv1.State{
			Composite: composedXRStatus(),
			Resources: map[string]*fnv1.Resource{
				"config": configMapResource(),
				"credentials": {
					Resource: mustStruct(map[string]any{
						"apiVersion": "v1",
						"kind":       "Secret",
						"metadata": map[string]any{
							"name":      "xr-credentials",
							"namespace": "default",
							"labels": map[string]any{
								"app": "kamera",
							},
						},
						"data": map[string]any{
							"token": "a2FtZXJhLXN0dWI=",
						},
					}),
					Ready: fnv1.Ready_READY_UNSPECIFIED,
				},
			},
		},
	}
}

func composedXRStatus() *fnv1.Resource {
	return &fnv1.Resource{
		Resource: mustStruct(map[string]any{
			"status": map[string]any{
				"observedGeneration": 1,
				"phase":              "Composed",
			},
		}),
		Ready: fnv1.Ready_READY_TRUE,
	}
}

func configMapResource() *fnv1.Resource {
	return &fnv1.Resource{
		Resource: mustStruct(map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":      "xr-config",
				"namespace": "default",
				"labels": map[string]any{
					"app": "kamera",
				},
			},
			"data": map[string]any{
				"message": "hello from kamera",
			},
		}),
		Ready: fnv1.Ready_READY_TRUE,
	}
}

func mustStruct(in map[string]any) *structpb.Struct {
	out, err := structpb.NewStruct(in)
	if err != nil {
		panic(err)
	}
	return out
}
