package main

import (
	"context"

	kratix "github.com/syntasso/kratix/api/v1alpha1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type defaultNamespaceClient struct {
	ctrlclient.Client
	namespace string
}

func (c *defaultNamespaceClient) Get(ctx context.Context, key ctrlclient.ObjectKey, obj ctrlclient.Object, opts ...ctrlclient.GetOption) error {
	if shouldDefaultNamespace(obj) && key.Namespace == "" {
		key.Namespace = c.namespace
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c *defaultNamespaceClient) Create(ctx context.Context, obj ctrlclient.Object, opts ...ctrlclient.CreateOption) error {
	defaultNamespace(obj, c.namespace)
	return c.Client.Create(ctx, obj, opts...)
}

func (c *defaultNamespaceClient) Update(ctx context.Context, obj ctrlclient.Object, opts ...ctrlclient.UpdateOption) error {
	defaultNamespace(obj, c.namespace)
	return c.Client.Update(ctx, obj, opts...)
}

func (c *defaultNamespaceClient) Patch(ctx context.Context, obj ctrlclient.Object, patch ctrlclient.Patch, opts ...ctrlclient.PatchOption) error {
	defaultNamespace(obj, c.namespace)
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func (c *defaultNamespaceClient) Status() ctrlclient.SubResourceWriter {
	return &defaultNamespaceStatusWriter{
		SubResourceWriter: c.Client.Status(),
		namespace:         c.namespace,
	}
}

type defaultNamespaceStatusWriter struct {
	ctrlclient.SubResourceWriter
	namespace string
}

func (w *defaultNamespaceStatusWriter) Update(ctx context.Context, obj ctrlclient.Object, opts ...ctrlclient.SubResourceUpdateOption) error {
	defaultNamespace(obj, w.namespace)
	return w.SubResourceWriter.Update(ctx, obj, opts...)
}

func (w *defaultNamespaceStatusWriter) Patch(ctx context.Context, obj ctrlclient.Object, patch ctrlclient.Patch, opts ...ctrlclient.SubResourcePatchOption) error {
	defaultNamespace(obj, w.namespace)
	return w.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}

func shouldDefaultNamespace(obj ctrlclient.Object) bool {
	switch obj.(type) {
	case *kratix.Promise, *kratix.PromiseRevision:
		return true
	default:
		return false
	}
}

func defaultNamespace(obj ctrlclient.Object, namespace string) {
	if shouldDefaultNamespace(obj) && obj.GetNamespace() == "" {
		obj.SetNamespace(namespace)
	}
}
