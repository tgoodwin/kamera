package main

import (
	"context"
	"fmt"
	"sync/atomic"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// nameGeneratingClient simulates API server generateName semantics for CREATE.
// This is required because the replay client does not auto-generate names.
type nameGeneratingClient struct {
	client.Client
	counter uint64
}

func (c *nameGeneratingClient) Reset() {
	atomic.StoreUint64(&c.counter, 0)
}

func (c *nameGeneratingClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if obj.GetName() == "" && obj.GetGenerateName() != "" {
		id := atomic.AddUint64(&c.counter, 1)
		obj.SetName(fmt.Sprintf("%s%05d", obj.GetGenerateName(), id))
	}
	return c.Client.Create(ctx, obj, opts...)
}
