package client

import (
	"context"

	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/tag"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ client.StatusClient = &Client{}

type SubResourceClient struct {
	// reader client.SubResourceReader
	client *Client
	writer client.SubResourceWriter
}

func (c *Client) Status() client.SubResourceWriter {
	statusClient := c.Client.Status()
	return &SubResourceClient{writer: statusClient, client: c}
}

func (s *SubResourceClient) logOperation(obj client.Object, action event.OperationType) {
	s.client.logOperation(obj, action)
}

func (s *SubResourceClient) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	tag.LabelChange(obj)
	s.logOperation(obj, event.UPDATE)
	s.client.tracker.propagateLabels(obj)
	return s.writer.Update(ctx, obj, opts...)
}

func (s *SubResourceClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	tag.LabelChange(obj)
	s.logOperation(obj, event.PATCH)
	s.client.tracker.propagateLabels(obj)
	return s.writer.Patch(ctx, obj, patch, opts...)
}

func (s *SubResourceClient) Create(ctx context.Context, obj client.Object, sub client.Object, opts ...client.SubResourceCreateOption) error {
	tag.LabelChange(obj)
	s.logOperation(obj, event.CREATE)
	s.client.tracker.propagateLabels(obj)
	return s.writer.Create(ctx, obj, sub, opts...)
}
