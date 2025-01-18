package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/go-logr/logr"
	sleeveclient "github.com/tgoodwin/sleeve/pkg/client"
	"github.com/tgoodwin/sleeve/pkg/tag"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

var logger logr.Logger

type Client struct {
	// dummyClient is a useless type that implements the remainder of the client.Client interface
	reconcilerID string

	// TODO address this
	*dummyClient
	framesByID map[string]FrameData

	recorder EffectRecorder

	scheme *runtime.Scheme
}

var _ client.Client = (*Client)(nil)

func NewClient(reconcilerID string, scheme *runtime.Scheme, frameData map[string]FrameData, recorder EffectRecorder) *Client {
	if frameData == nil {
		frameData = make(map[string]FrameData)
	}
	return &Client{
		reconcilerID: reconcilerID,
		scheme:       scheme,
		dummyClient:  &dummyClient{},
		framesByID:   frameData,
		recorder:     recorder,
	}
}

func (c *Client) InsertFrame(id string, data FrameData) {
	if _, ok := c.framesByID[id]; ok {
		panic(fmt.Sprintf("frame %s already exists", id))
	}
	c.framesByID[id] = data
}

var _ client.Client = (*Client)(nil)

func inferKind(obj client.Object) string {
	// assumption: the object is a pointer to a struct
	t := reflect.TypeOf(obj).Elem()
	return t.Name()
}

func inferListKind(list client.ObjectList) string {
	itemsValue := reflect.ValueOf(list).Elem().FieldByName("Items")
	if !itemsValue.IsValid() {
		panic("List object does not have Items field")
	}
	itemType := itemsValue.Type().Elem()
	return itemType.Name()
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
	kind := inferKind(obj)
	logger.V(2).Info("client:requesting key %s, inferred kind: %s\n", key, kind)
	if frame, ok := c.framesByID[frameID]; ok {
		if frozenObj, ok := frame[kind][key]; ok {
			if err := c.recorder.RecordEffect(ctx, frozenObj, sleeveclient.GET); err != nil {
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
	return nil
}

func (c *Client) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	frameID := FrameIDFromContext(ctx)
	kind := inferListKind(list)

	if frame, ok := c.framesByID[frameID]; ok {
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
				if err := c.recorder.RecordEffect(ctx, obj, sleeveclient.LIST); err != nil {
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

func (c *Client) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	logger = log.FromContext(ctx)
	tag.AddSleeveObjectID(obj)
	tag.LabelChange(obj)

	// TODO create a trace
	// TODO propagate labels

	// logger.V(0).Info("client:creating object", "object", obj)
	return c.recorder.RecordEffect(ctx, obj, sleeveclient.CREATE)
}

func (c *Client) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	tag.AddDeletionID(obj)
	return c.recorder.RecordEffect(ctx, obj, sleeveclient.DELETE)
}

func (c *Client) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	tag.LabelChange(obj)
	return c.recorder.RecordEffect(ctx, obj, sleeveclient.UPDATE)
}

func (c *Client) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	tag.AddDeletionID(obj)
	return c.recorder.RecordEffect(ctx, obj, sleeveclient.DELETE)
}

func (c *Client) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	tag.LabelChange(obj)
	return c.recorder.RecordEffect(ctx, obj, sleeveclient.PATCH)
}

func (c *Client) Status() client.SubResourceWriter {
	return &subResourceClient{wrapped: c}
}

type subResourceClient struct {
	wrapped *Client
}

var _ client.SubResourceWriter = (*subResourceClient)(nil)

func (c *subResourceClient) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	tag.LabelChange(obj)
	return c.wrapped.recorder.RecordEffect(ctx, obj, sleeveclient.UPDATE)
}

func (c *subResourceClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	tag.LabelChange(obj)
	return c.wrapped.recorder.RecordEffect(ctx, obj, sleeveclient.PATCH)
}

func (c *subResourceClient) Create(ctx context.Context, obj client.Object, sub client.Object, opts ...client.SubResourceCreateOption) error {
	tag.LabelChange(obj)
	tag.AddSleeveObjectID(sub)
	return c.wrapped.recorder.RecordEffect(ctx, obj, sleeveclient.CREATE)
}
