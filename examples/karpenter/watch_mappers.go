package main

import (
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// nodeToNodeClaimMapper approximates Karpenter's providerID-based Node->NodeClaim mapping.
// NOTE: In real Karpenter this mapping is done by listing NodeClaims with a providerID filter.
// The simulator cannot list with field selectors, so we attach a label on Node at creation time.
func nodeToNodeClaimMapper() tracecheck.WatchMapper {
	return func(obj *unstructured.Unstructured) []reconcile.Request {
		if obj == nil {
			return nil
		}
		name, ok := obj.GetLabels()["karpenter.sh/nodeclaim-name"]
		if !ok || name == "" {
			return nil
		}
		return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: name}}}
	}
}
