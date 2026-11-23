package main

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/tgoodwin/kamera/pkg/simclock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	// Kubernetes assigns CreationTimestamp on create. Karpenter uses the
	// NodeClaim creation time as the initial registration-condition timestamp.
	creationTimestamp := obj.GetCreationTimestamp()
	if creationTimestamp.Time.IsZero() {
		obj.SetCreationTimestamp(metav1.NewTime(simclock.Now()))
	}
	return c.Client.Create(ctx, obj, opts...)
}
