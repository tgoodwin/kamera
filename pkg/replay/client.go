package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/go-logr/logr"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/util"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	frameID := FrameIDFromContext(ctx)
	kind := util.GetKind(obj)
	logger.V(2).Info("client:get", "Key", key, "Kind", kind)
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
			// fmt.Println("not found!!!", kind, key)
			return apierrors.NewNotFound(schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}, key.Name)
		}
	} else {
		logger.V(2).Info("frame NOT found!", "FrameID", frameID)
		return fmt.Errorf("frame %s not found", frameID)
	}
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
	preconditions := ExtractCreatePreconditions(opts)
	return c.handleEffect(ctx, obj, event.CREATE, &preconditions)
}

func (c *Client) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	// in the replay client, we're not actually interacting with the API server
	// so the object won't take on a deletion timestamp unless we set it here.
	ts := v1.Time{Time: time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC)}
	obj.SetDeletionTimestamp(&ts)

	preconditions := ExtractDeletePreconditions(opts)
	return c.handleEffect(ctx, obj, event.MARK_FOR_DELETION, &preconditions)
}

func (c *Client) Remove(ctx context.Context, obj client.Object) error {
	// preconditions := ExtractRemovePreconditions(opts)
	return c.handleEffect(ctx, obj, event.REMOVE, nil)
}

func (c *Client) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	preconditions := ExtractUpdatePreconditions(opts)
	labels := obj.GetLabels()
	// TODO SLE-28 diagnose why this case even exists
	if _, ok := labels[tag.TraceyObjectID]; !ok {
		logger.Error(nil, "no tracey object ID found on object")
		tag.AddSleeveObjectID(obj)
	}
	return c.handleEffect(ctx, obj, event.UPDATE, &preconditions)
}

func (c *Client) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	preconditions := ExtractDeleteAllOfPreconditions(opts)
	return c.handleEffect(ctx, obj, event.MARK_FOR_DELETION, &preconditions)
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
