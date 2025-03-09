package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/go-logr/logr"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/util"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

var logger logr.Logger

type frameReader interface {
	GetCacheFrame(frameID string) (CacheFrame, error)
}

type Client struct {
	// dummyClient is a useless type that implements the remainder of the client.Client interface
	reconcilerID string

	// TODO address this
	*dummyClient

	frameReader

	recorder EffectRecorder
	// emitter  event.Emitter

	scheme *runtime.Scheme
}

var _ client.Client = (*Client)(nil)

func NewClient(reconcilerID string, scheme *runtime.Scheme, frameReader frameReader, recorder EffectRecorder) *Client {
	return &Client{
		reconcilerID: reconcilerID,
		scheme:       scheme,
		dummyClient:  &dummyClient{},
		frameReader:  frameReader,
		recorder:     recorder,
	}
}

var _ client.Client = (*Client)(nil)

func (c *Client) handleEffect(ctx context.Context, obj client.Object, opType event.OperationType, preconditions *PreconditionInfo) error {
	// TODO validate preconditions
	return c.recorder.RecordEffect(ctx, obj, opType, preconditions)
}

func (c *Client) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	logger = log.FromContext(ctx)
	gvk := obj.GetObjectKind().GroupVersionKind()
	// gvkToTypes := c.scheme.AllKnownTypes()
	// if targetType, ok := gvkToTypes[gvk]; ok {
	// 	// create a new object of the same type as obj
	// 	newObj := reflect.New(targetType).Interface().(client.Object)
	// 	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.(*unstructured.Unstructured).Object, newObj); err != nil {
	// 		return err
	// 	}
	// }

	frameID := FrameIDFromContext(ctx)
	kind := util.GetKind(obj)
	logger.V(2).Info("client:requesting key %s, inferred kind: %s\n", key, kind)
	if frame, err := c.GetCacheFrame(frameID); err == nil {
		if frozenObj, ok := frame[kind][key]; ok {
			if err := c.handleEffect(ctx, frozenObj, event.GET, nil); err != nil {
				return err
			}

			// use json.Marshal to copy the frozen object into the obj
			data, err := json.Marshal(frozenObj)
			if err != nil {
				return err
			}
			if err := json.Unmarshal(data, obj); err != nil {
				return err
			}
		} else {
			return apierrors.NewNotFound(schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}, key.Name)
		}
	} else {
		return fmt.Errorf("frame %s not found", frameID)
	}
	// c.logOperation(ctx, obj, event.GET)
	return nil
}

func (c *Client) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	frameID := FrameIDFromContext(ctx)
	kind := util.InferListKind(list)

	if frame, err := c.GetCacheFrame(frameID); err == nil {
		if objsForKind, ok := frame[kind]; ok {
			// get the Items field of the list object
			itemsValue := reflect.ValueOf(list).Elem().FieldByName("Items")
			if !itemsValue.IsValid() {
				return fmt.Errorf("List object does not have Items field")
			}

			// create a new slice of the correct type
			itemType := itemsValue.Type().Elem()
			newSlice := reflect.MakeSlice(reflect.SliceOf(itemType), 0, len(objsForKind))

			for _, obj := range objsForKind {
				if err := c.handleEffect(ctx, obj, event.LIST, nil); err != nil {
					return err
				}

				// create a new object of the correct type
				newObj := reflect.New(itemType).Interface().(client.Object)

				// use json.Marshal to copy the unstructured object into the new object
				data, err := json.Marshal(obj)
				if err != nil {
					return err
				}
				if err := json.Unmarshal(data, newObj); err != nil {
					return err
				}

				// append the new object to the slice
				newSlice = reflect.Append(newSlice, reflect.ValueOf(newObj).Elem())
			}

			// set the Items field of the list object to the new slice
			itemsValue.Set(newSlice)
		}
		return nil
	}

	return fmt.Errorf("frame %s not found", frameID)
}

// TODO create or set an ObjectID here
func (c *Client) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	// TODO obj.SetUID(uuid.NewUUID())
	preconditions := ExtractCreatePreconditions(opts)
	return c.handleEffect(ctx, obj, event.CREATE, &preconditions)
}

func (c *Client) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	preconditions := ExtractDeletePreconditions(opts)
	return c.handleEffect(ctx, obj, event.DELETE, &preconditions)
}

func (c *Client) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	preconditions := ExtractUpdatePreconditions(opts)
	return c.handleEffect(ctx, obj, event.UPDATE, &preconditions)
}

func (c *Client) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	preconditions := ExtractDeleteAllOfPreconditions(opts)
	return c.handleEffect(ctx, obj, event.DELETE, &preconditions)
}

func (c *Client) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	preconditions := ExtractPatchPreconditions(opts)
	return c.handleEffect(ctx, obj, event.PATCH, &preconditions)
}

func (c *Client) Status() client.SubResourceWriter {
	return &subResourceClient{wrapped: c}
}

type subResourceClient struct {
	wrapped *Client
}

var _ client.SubResourceWriter = (*subResourceClient)(nil)

func (c *subResourceClient) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	preconditions := ExtractStatusUpdatePreconditions(opts)
	return c.wrapped.handleEffect(ctx, obj, event.UPDATE, &preconditions)
}

func (c *subResourceClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	preconditions := ExtractStatusPatchPreconditions(opts)
	return c.wrapped.handleEffect(ctx, obj, event.PATCH, &preconditions)
}

func (c *subResourceClient) Create(ctx context.Context, obj client.Object, sub client.Object, opts ...client.SubResourceCreateOption) error {
	preconditions := ExtractSubResourceCreatePreconditions(opts)
	return c.wrapped.handleEffect(ctx, obj, event.CREATE, &preconditions)
}
