package client

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"

	"time"

	"github.com/go-logr/logr"
	"github.com/google/uuid"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"github.com/tgoodwin/sleeve/pkg/tag"
	"github.com/tgoodwin/sleeve/pkg/util"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var log = logf.Log.WithName(tag.LoggerName)

type Client struct {
	// this syntax is "embedding" the client.Client interface in the Client struct
	// this means that the Client struct will have all the methods of the client.Client interface.
	// below, we will override some of these methods to add our own behavior.
	client.Client

	// identifier for the reconciler (controller name)
	reconcilerID string

	logger logr.Logger // legacy
	// handles logging of events
	emitter event.Emitter

	config *Config

	tracker *ContextTracker
}

var _ client.Client = (*Client)(nil)

func New(wrapped client.Client, reconcilerID string, emitter event.Emitter, tracker *ContextTracker) *Client {
	return &Client{
		reconcilerID: reconcilerID,
		Client:       wrapped,
		logger:       log,
		emitter:      emitter,
		config:       NewConfig(),
		tracker:      tracker,
	}
}

func newClient(wrapped client.Client, id string) *Client {
	return &Client{
		reconcilerID: id,
		Client:       wrapped,
		logger:       log,
		emitter:      event.NewLogEmitter(log),
		config:       NewConfig(),
		tracker:      NewProdTracker(id),
	}
}

func Wrap(c client.Client, id string) *Client {
	return newClient(c, id)
}

func (c *Client) WithName(name string) *Client {
	c.reconcilerID = name
	c.tracker.reconcilerID = name
	return c
}

func (c *Client) WithEnvConfig() *Client {
	c.logger = log

	// Get the current environment variables
	envVars := make(map[string]string)
	for _, env := range os.Environ() {
		pair := strings.SplitN(env, "=", 2)
		if len(pair) == 2 {
			envVars[pair[0]] = pair[1]
		}
	}
	if logSnapshots, ok := envVars["SLEEVE_LOG_SNAPSHOTS"]; ok {
		c.config.LogObjectSnapshots = logSnapshots == "1"
	}

	// Log the environment variables
	for key, value := range envVars {
		c.logger.WithValues("key", key, "value", value).Info("configuring sleeve client from env")
	}

	return c
}

func Operation(obj client.Object, reconcileID, controllerID, rootEventID string, op event.OperationType) *event.Event {
	id := uuid.New().String()
	e := &event.Event{
		ID:           id,
		Timestamp:    event.FormatTimeStr(time.Now()),
		ReconcileID:  reconcileID,
		ControllerID: controllerID,
		RootEventID:  rootEventID,
		OpType:       string(op),
		Kind:         util.GetKind(obj),
		// CHANGE TO SLEEVE OBJECT ID
		ObjectID: string(obj.GetUID()),
		Version:  obj.GetResourceVersion(),
		Labels:   obj.GetLabels(),
	}
	changeID := e.ChangeID()
	if changeID == "" {
		panic(fmt.Sprintf("event does not have a change ID: %v", e))
	}
	return e
}

func (c *Client) logOperation(obj client.Object, op event.OperationType) {
	reconcileID := c.tracker.rc.GetReconcileID()
	event := Operation(
		obj,
		reconcileID,
		c.reconcilerID,
		c.tracker.rc.GetRootID(reconcileID),
		op,
	)
	r := snapshot.AsRecord(obj, reconcileID)
	c.emitter.LogOperation(event)
	// associate the operation with the object's snapshot
	r.OperationID = event.ID
	r.OperationType = string(op)
	c.emitter.LogObjectVersion(r)
}

func (c *Client) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	currLabels := obj.GetLabels()
	tag.AddSleeveObjectID(obj)
	tag.LabelChange(obj)
	c.tracker.propagateLabels(obj)

	if err := c.Client.Create(ctx, obj, opts...); err != nil {
		// revert object labels to original state if the operation fails
		obj.SetLabels(currLabels)
		return err
	}

	// this is the *first* time the object is being updated (definition of create)
	// so we don't need to worry about logging before propagating labels here
	c.logOperation(obj, event.CREATE)
	return nil
}

func (c *Client) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	origLabels := obj.GetLabels()
	tag.AddDeletionID(obj)
	if err := c.Client.Delete(ctx, obj, opts...); err != nil {
		c.logger.Error(err, "deleting object")
		// revert object labels to original state if the operation fails
		obj.SetLabels(origLabels)
		return err
	}
	// c.logObjectVersion(obj)
	c.logOperation(obj, event.DELETE)
	// c.trackOperation(ctx, obj, DELETE)
	return nil
}

func (c *Client) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	origLabels := obj.GetLabels()
	tag.AddDeletionID(obj)
	if err := c.Client.DeleteAllOf(ctx, obj, opts...); err != nil {
		c.logger.Error(err, "deleting objects")
		// revert object labels to original state
		obj.SetLabels(origLabels)
		return err
	}
	// c.logObjectVersion(obj)
	c.logOperation(obj, event.DELETE)
	// c.trackOperation(ctx, obj, DELETE)
	return nil
}

func (c *Client) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	// cast back to a client.Ojbject
	objCopy, ok := obj.DeepCopyObject().(client.Object)
	if !ok {
		panic("object does not implement client.Object")
	}

	if err := c.Client.Get(ctx, key, objCopy, opts...); err != nil {
		return err
	}
	isVisible := c.isVisible(objCopy)
	if !isVisible {
		return apierrors.NewNotFound(schema.GroupResource{}, key.Name)
	}
	err := c.Client.Get(ctx, key, obj, opts...)
	c.tracker.TrackOperation(ctx, obj, event.GET)
	return err
}

func (c *Client) isVisible(obj client.Object) bool {
	kind := obj.GetObjectKind().GroupVersionKind().Kind
	if visDelay, ok := c.config.visibilityDelayByKind[kind]; ok {
		now := time.Now()
		created := obj.GetCreationTimestamp().Time
		if now.Sub(created) < visDelay {
			c.logger.WithValues(
				"ObjectKind", kind,
				"ObjectUID", obj.GetUID(),
				"TimeSinceCreated", now.Sub(created),
			).V(1).Info("Object not visible yet")
			return false
		}
		return true
	}
	return true
}

func (c *Client) filterVisible(objs []client.Object) []client.Object {
	visible := make([]client.Object, 0)
	for _, obj := range objs {
		if c.isVisible(obj) {
			visible = append(visible, obj)
		}
	}
	return visible
}

func (c *Client) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	// Perform the List operation on the wrapped client
	lc := list.DeepCopyObject().(client.ObjectList)
	if err := c.Client.List(ctx, lc, opts...); err != nil {
		return err
	}

	// use reflection to get the Items field from the result
	itemsValue := reflect.ValueOf(lc).Elem().FieldByName("Items")
	if !itemsValue.IsValid() {
		return fmt.Errorf("unable to get Items field from list")
	}

	// create a new slice to hold the items
	out := reflect.MakeSlice(itemsValue.Type(), 0, itemsValue.Len())
	for i := 0; i < itemsValue.Len(); i++ {
		item := itemsValue.Index(i).Addr().Interface().(client.Object)
		// instead of treating the LIST operation as a singular observation event,
		// we treat each item in the list as a separate event
		c.tracker.TrackOperation(ctx, item, event.LIST)
		out = reflect.Append(out, itemsValue.Index(i))
	}

	// Set the items back to the original list
	originalItemsValue := reflect.ValueOf(list).Elem().FieldByName("Items")
	if !originalItemsValue.IsValid() {
		return fmt.Errorf("unable to get Items field from original list")
	}
	originalItemsValue.Set(out)

	return nil
}

func (c *Client) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	currLabels := obj.GetLabels()

	// generate a label to the object to associate it with the change event
	tag.LabelChange(obj)
	// make a copy of the object before we propagate labels
	objPrePropagation := obj.DeepCopyObject().(client.Object)
	c.tracker.propagateLabels(obj)

	// TODO log object version here??

	if err := c.Client.Update(ctx, obj, opts...); err != nil {
		c.logger.Error(err, "operation failed, not tracking it")
		// revert object labels to original state
		obj.SetLabels(currLabels)
		return err
	}

	// happy path! the update went through successfully - let's record that!
	c.logOperation(objPrePropagation, event.UPDATE)

	return nil
}

func (c *Client) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	currLabels := obj.GetLabels()
	tag.LabelChange(obj)
	objPrePropagation := obj.DeepCopyObject().(client.Object)
	c.tracker.propagateLabels(obj)

	if err := c.Client.Patch(ctx, obj, patch, opts...); err != nil {
		c.logger.Error(err, "operation failed, not tracking it")
		obj.SetLabels(currLabels)
		return err
	}
	c.logOperation(objPrePropagation, event.PATCH)
	return nil
}
