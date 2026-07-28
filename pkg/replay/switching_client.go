package replay

import (
	"context"
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// switchingClient delegates all client.Client calls to a mutable underlying client.
// This lets shared state (e.g., the cluster cache) use the "current" replay client
// for whatever reconciler is executing, so frame lookups remain consistent.
// SwitchingClient forwards client operations to a replaceable delegate. It is
// useful when an upstream controller retains a client across replay states.
type SwitchingClient struct {
	mu      sync.RWMutex
	current client.Client
}

// NewSwitchingClient constructs an empty forwarding client.
func NewSwitchingClient() *SwitchingClient {
	return &SwitchingClient{}
}

// Set replaces the delegate used by subsequent client operations.
func (c *SwitchingClient) Set(current client.Client) {
	c.mu.Lock()
	c.current = current
	c.mu.Unlock()
}

func (c *SwitchingClient) get() client.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.current == nil {
		panic("switchingClient: current client not set")
	}
	return c.current
}

func (c *SwitchingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return c.get().Get(ctx, key, obj, opts...)
}

func (c *SwitchingClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return c.get().List(ctx, list, opts...)
}

func (c *SwitchingClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	return c.get().Create(ctx, obj, opts...)
}

func (c *SwitchingClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	return c.get().Delete(ctx, obj, opts...)
}

func (c *SwitchingClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	return c.get().Update(ctx, obj, opts...)
}

func (c *SwitchingClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	return c.get().Patch(ctx, obj, patch, opts...)
}

func (c *SwitchingClient) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
	return c.get().Apply(ctx, obj, opts...)
}

func (c *SwitchingClient) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	return c.get().DeleteAllOf(ctx, obj, opts...)
}

func (c *SwitchingClient) Status() client.SubResourceWriter {
	return c.get().Status()
}

func (c *SwitchingClient) SubResource(subResource string) client.SubResourceClient {
	return c.get().SubResource(subResource)
}

func (c *SwitchingClient) Scheme() *runtime.Scheme {
	return c.get().Scheme()
}

func (c *SwitchingClient) RESTMapper() meta.RESTMapper {
	return c.get().RESTMapper()
}

func (c *SwitchingClient) GroupVersionKindFor(obj runtime.Object) (schema.GroupVersionKind, error) {
	return c.get().GroupVersionKindFor(obj)
}

func (c *SwitchingClient) IsObjectNamespaced(obj runtime.Object) (bool, error) {
	return c.get().IsObjectNamespaced(obj)
}
