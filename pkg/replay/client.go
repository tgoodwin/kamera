package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/go-logr/logr"
	sleeveclient "github.com/tgoodwin/sleeve/pkg/client"
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
	*dummyClient
	framesByID map[string]FrameData

	recorder EffectRecorder

	scheme *runtime.Scheme
}

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
		// DumpCacheFrameContents(frame)
		if frozenObj, ok := frame[kind][key]; ok {
			logger.V(2).Info("client:found object in frame")
			c.recorder.RecordEffect(ctx, frozenObj, sleeveclient.GET)

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
			// get a slice of the objects from the map values
			objs := make([]client.Object, 0, len(objsForKind))
			for _, obj := range objsForKind {
				c.recorder.RecordEffect(ctx, obj, sleeveclient.LIST)
				objs = append(objs, obj)
			}
			// set the Items field of the list object to the slice of objects
			itemsValue := reflect.ValueOf(list).Elem().FieldByName("Items")
			itemsValue.Set(reflect.ValueOf(objs))
		}
		return nil
	}

	return fmt.Errorf("frame %s not found", frameID)
}

func (c *Client) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	c.recorder.RecordEffect(ctx, obj, sleeveclient.CREATE)
	return nil
}

func (c *Client) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	c.recorder.RecordEffect(ctx, obj, sleeveclient.DELETE)
	return nil
}

func (c *Client) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	c.recorder.RecordEffect(ctx, obj, sleeveclient.UPDATE)
	return nil
}

func (c *Client) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	c.recorder.RecordEffect(ctx, obj, sleeveclient.DELETE)
	return nil
}

func (c *Client) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	c.recorder.RecordEffect(ctx, obj, sleeveclient.PATCH)
	return nil
}
