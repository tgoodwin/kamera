package main

import (
	"context"
	"fmt"

	fnv1 "github.com/crossplane/crossplane/v2/proto/fn/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

const stubFunctionName = "kamera-stub"

type stubFunctionRunner struct{}

func (s stubFunctionRunner) RunFunction(_ context.Context, name string, _ *fnv1.RunFunctionRequest) (*fnv1.RunFunctionResponse, error) {
	if name != stubFunctionName {
		return nil, fmt.Errorf("unexpected function name %q", name)
	}

	desired := &fnv1.State{
		Composite: &fnv1.Resource{
			Resource: mustStruct(map[string]any{
				"status": map[string]any{
					"observedGeneration": 1,
					"phase":              "Composed",
				},
			}),
			Ready: fnv1.Ready_READY_TRUE,
		},
		Resources: map[string]*fnv1.Resource{
			"config": {
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
			},
		},
	}

	return &fnv1.RunFunctionResponse{Desired: desired}, nil
}

func mustStruct(in map[string]any) *structpb.Struct {
	out, err := structpb.NewStruct(in)
	if err != nil {
		panic(err)
	}
	return out
}
