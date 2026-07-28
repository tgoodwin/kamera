package replay

import (
	"context"

	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type defaultNamespaceClient struct {
	ctrlclient.Client
	namespace     string
	shouldDefault func(ctrlclient.Object) bool
}

// NewDefaultNamespaceClient wraps a replay client and supplies a namespace for
// selected objects that omit one.
func NewDefaultNamespaceClient(
	client ctrlclient.Client,
	namespace string,
	shouldDefault func(ctrlclient.Object) bool,
) ctrlclient.Client {
	return &defaultNamespaceClient{
		Client:        client,
		namespace:     namespace,
		shouldDefault: shouldDefault,
	}
}

func (c *defaultNamespaceClient) Get(ctx context.Context, key ctrlclient.ObjectKey, obj ctrlclient.Object, opts ...ctrlclient.GetOption) error {
	if c.shouldDefault != nil && c.shouldDefault(obj) && key.Namespace == "" {
		key.Namespace = c.namespace
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c *defaultNamespaceClient) Create(ctx context.Context, obj ctrlclient.Object, opts ...ctrlclient.CreateOption) error {
	defaultObjectNamespace(obj, c.namespace, c.shouldDefault)
	return c.Client.Create(ctx, obj, opts...)
}

func (c *defaultNamespaceClient) Update(ctx context.Context, obj ctrlclient.Object, opts ...ctrlclient.UpdateOption) error {
	defaultObjectNamespace(obj, c.namespace, c.shouldDefault)
	return c.Client.Update(ctx, obj, opts...)
}

func (c *defaultNamespaceClient) Patch(ctx context.Context, obj ctrlclient.Object, patch ctrlclient.Patch, opts ...ctrlclient.PatchOption) error {
	defaultObjectNamespace(obj, c.namespace, c.shouldDefault)
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func (c *defaultNamespaceClient) Status() ctrlclient.SubResourceWriter {
	return &defaultNamespaceStatusWriter{
		SubResourceWriter: c.Client.Status(),
		namespace:         c.namespace,
		shouldDefault:     c.shouldDefault,
	}
}

type defaultNamespaceStatusWriter struct {
	ctrlclient.SubResourceWriter
	namespace     string
	shouldDefault func(ctrlclient.Object) bool
}

func (w *defaultNamespaceStatusWriter) Update(ctx context.Context, obj ctrlclient.Object, opts ...ctrlclient.SubResourceUpdateOption) error {
	defaultObjectNamespace(obj, w.namespace, w.shouldDefault)
	return w.SubResourceWriter.Update(ctx, obj, opts...)
}

func (w *defaultNamespaceStatusWriter) Patch(ctx context.Context, obj ctrlclient.Object, patch ctrlclient.Patch, opts ...ctrlclient.SubResourcePatchOption) error {
	defaultObjectNamespace(obj, w.namespace, w.shouldDefault)
	return w.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}

func defaultObjectNamespace(
	obj ctrlclient.Object,
	namespace string,
	shouldDefault func(ctrlclient.Object) bool,
) {
	if shouldDefault != nil && shouldDefault(obj) && obj.GetNamespace() == "" {
		obj.SetNamespace(namespace)
	}
}
