package main

import (
	_ "embed"
	"encoding/json"
	"fmt"

	"github.com/kubernetes-sigs/kro/pkg/graph"
	"github.com/tgoodwin/kamera/pkg/apiserver"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/kube-openapi/pkg/spec3"
)

// The Deployment, Service and Ingress models and their referenced components
// are from Kubernetes v1.35.0's published OpenAPI v3 documents.
//
//go:embed kubernetes-v1.35-openapi.json
var kubernetesApplyOpenAPI []byte

func registerKROApplySchemas(builder *tracecheck.ExplorerBuilder, appGraph *graph.Graph) {
	var doc spec3.OpenAPI
	if err := json.Unmarshal(kubernetesApplyOpenAPI, &doc); err != nil {
		panic(fmt.Sprintf("decode Kubernetes apply schemas: %v", err))
	}

	resources := make([]apiserver.OpenAPIResource, 0, len(appGraph.Nodes))
	for _, id := range appGraph.TopologicalOrder {
		node := appGraph.Nodes[id]
		// All three managed kinds in this fixture have a /status route; the
		// graph derives their scope from the harness's Kubernetes REST mapper.
		scope := extv1.ClusterScoped
		if node.Meta.Namespaced {
			scope = extv1.NamespaceScoped
		}
		resources = append(resources, apiserver.OpenAPIResource{
			GVK: node.Template.GroupVersionKind(), Scope: scope, HasStatus: true,
		})
	}
	builder.WithCRD(appGraph.CRD).WithOpenAPIV3(&doc, resources...)
}
