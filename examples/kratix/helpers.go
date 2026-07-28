package main

import (
	kratix "github.com/syntasso/kratix/api/v1alpha1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func shouldDefaultNamespace(obj ctrlclient.Object) bool {
	switch obj.(type) {
	case *kratix.Promise, *kratix.PromiseRevision:
		return true
	default:
		return false
	}
}
