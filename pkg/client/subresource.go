package client

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/tgoodwin/sleeve/pkg/tag"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
)

var _ client.StatusClient = &Client{}

type SubResourceClient struct {
	// reader kclient.SubResourceReader
	client *Client
	writer kclient.SubResourceWriter
}

func (c *Client) Status() kclient.SubResourceWriter {
	statusClient := c.Client.Status()
	return &SubResourceClient{writer: statusClient, client: c}
}

func (s *SubResourceClient) logOperation(obj kclient.Object, action OperationType) {
	s.client.logOperation(obj, action)
}

func (s *SubResourceClient) Update(ctx context.Context, obj kclient.Object, opts ...kclient.SubResourceUpdateOption) error {
	s.client.tracker.setReconcileID(ctx)
	tag.LabelChange(obj)
	s.logOperation(obj, UPDATE)
	s.client.tracker.propagateLabels(obj)
	// fmt.Printf("extracted conditions: %v", conditions)
	// persist the labels to the object before updating status

	// update status
	// TODO this does not work. "the object has been modified; please apply your changes to the latest version and try again"
	res := s.writer.Update(ctx, obj, opts...)
	s.client.Update(ctx, obj)
	return res
}

func (s *SubResourceClient) Patch(ctx context.Context, obj kclient.Object, patch kclient.Patch, opts ...kclient.SubResourcePatchOption) error {
	s.client.tracker.setReconcileID(ctx)
	tag.LabelChange(obj)
	s.logOperation(obj, PATCH)
	// persist the labels to the object before updating status
	s.client.Update(ctx, obj)
	return s.writer.Patch(ctx, obj, patch, opts...)
}

func (s *SubResourceClient) Create(ctx context.Context, obj kclient.Object, sub kclient.Object, opts ...kclient.SubResourceCreateOption) error {
	s.client.tracker.setReconcileID(ctx)
	tag.LabelChange(obj)
	s.logOperation(obj, CREATE)
	s.client.tracker.propagateLabels(obj)
	s.client.Update(ctx, obj)

	return s.writer.Create(ctx, obj, sub, opts...)
}

// Extracts status.conditions from an arbitrary client.Object
func getUnstructuredStatusConditions(obj client.Object) ([]metav1.Condition, error) {
	// Convert to unstructured.Unstructured to access fields dynamically
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("object is not of type unstructured.Unstructured")
	}

	// Access the "status" field
	status, found, err := unstructured.NestedFieldNoCopy(u.Object, "status")
	if !found || err != nil {
		return nil, fmt.Errorf("status not found or error occurred: %v", err)
	}

	// Extract the "conditions" field
	conditions, found, err := unstructured.NestedFieldNoCopy(status.(map[string]interface{}), "conditions")
	if !found || err != nil {
		return nil, fmt.Errorf("conditions not found in status or error occurred: %v", err)
	}

	// Marshal and unmarshal conditions into metav1.Condition
	conditionBytes, err := json.Marshal(conditions)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal conditions: %v", err)
	}

	var result []metav1.Condition
	if err := json.Unmarshal(conditionBytes, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal conditions: %v", err)
	}

	return result, nil
}
