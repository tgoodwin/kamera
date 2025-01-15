package client

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"

	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/go-logr/logr"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"github.com/tgoodwin/sleeve/pkg/tag"
	"github.com/tgoodwin/sleeve/pkg/util"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	ctrl "sigs.k8s.io/controller-runtime/pkg/controller"
)

var log = logf.Log.WithName(tag.LoggerName)

// enum for controller operation types
type OperationType string

const (
	INIT   OperationType = "INIT"
	GET    OperationType = "GET"
	LIST   OperationType = "LIST"
	CREATE OperationType = "CREATE"
	UPDATE OperationType = "UPDATE"
	DELETE OperationType = "DELETE"
	PATCH  OperationType = "PATCH"
)

var mutationTypes = map[OperationType]struct{}{
	CREATE: {},
	UPDATE: {},
	DELETE: {},
	PATCH:  {},
}

func createFixedLengthHash() string {
	// Get the current time
	currentTime := time.Now()

	// Convert the current time to a byte slice
	timeBytes := []byte(currentTime.String())

	// Hash the byte slice using SHA256
	hash := sha256.Sum256(timeBytes)

	// Convert the hash to a fixed length string
	hashString := hex.EncodeToString(hash[:])

	// Take the first 6 characters of the hash string
	shortHash := hashString[:6]

	return shortHash
}

type Client struct {
	// this syntax is "embedding" the client.Client interface in the Client struct
	// this means that the Client struct will have all the methods of the client.Client interface.
	// below, we will override some of these methods to add our own behavior.
	client.Client

	// identifier for the reconciler (controller name)
	id string

	reconcileContext *ReconcileContext

	logger logr.Logger

	config *Config
}

var _ client.Client = (*Client)(nil)

func newClient(wrapped client.Client) *Client {
	return &Client{
		Client:           wrapped,
		logger:           log,
		reconcileContext: &ReconcileContext{},
		config:           NewConfig(),
	}
}

func Wrap(c client.Client) *Client {
	return newClient(c)
}

func (c *Client) WithName(name string) *Client {
	c.id = name
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

func (c *Client) setReconcileID(ctx context.Context) {
	rid := string(ctrl.ReconcileIDFromContext(ctx))
	if rid == "" {
		// this should never happen given our assumptions
		panic("reconcileID not set in context")
	}

	currReconcileID := c.reconcileContext.GetReconcileID()
	if currReconcileID == "" {
		// first time setting reconcileID
		c.reconcileContext.SetReconcileID(string(rid))
	} else if rid != currReconcileID {
		c.logger.V(2).Info("reconcileID changed", "old", currReconcileID, "new", rid)
		// unset rootID if reconcileID changes
		c.reconcileContext.SetRootID("")
		c.reconcileContext.SetReconcileID(string(rid))
	}
}

func Operation(obj client.Object, reconcileID, controllerID, rootEventID string, op OperationType) *event.Event {
	return &event.Event{
		Timestamp:    event.FormatTimeStr(time.Now()),
		ReconcileID:  reconcileID,
		ControllerID: controllerID,
		RootEventID:  rootEventID,
		OpType:       string(op),
		Kind:         util.GetKind(obj),
		ObjectID:     string(obj.GetUID()),
		Version:      obj.GetResourceVersion(),
		Labels:       obj.GetLabels(),
	}
}

func (c *Client) logOperation(obj client.Object, op OperationType) {
	event := Operation(
		obj,
		c.reconcileContext.GetReconcileID(),
		c.id,
		c.reconcileContext.GetRootID(),
		op,
	)
	eventJSON, err := json.Marshal(event)
	if err != nil {
		panic("failed to marshal event")
	}
	c.logger.WithValues("LogType", tag.ControllerOperationKey).Info(string(eventJSON))
}

func (c *Client) logObjectVersion(obj client.Object) {
	r := snapshot.RecordValue(obj)
	c.logger.WithValues("LogType", tag.ObjectVersionKey, "Observer", c.id).Info(r)
}

func (c *Client) setRootContext(obj client.Object) {
	labels := obj.GetLabels()
	// set by the webhook
	rootID, ok := labels[tag.TraceyWebhookLabel]
	if !ok {
		rootID, ok = labels[tag.TraceyRootID]
		if !ok {
			// no root context to set
			c.logger.V(2).Error(nil, fmt.Sprintf("Root context not set on %s object", util.GetKind(obj)))
			return
		}
	}
	currRootID := c.reconcileContext.GetRootID()
	if currRootID != "" && currRootID != rootID {
		c.logger.WithValues(
			"ControllerID", c.id,
			"ReconcileID", c.reconcileContext.GetReconcileID(),
			"RootID", c.reconcileContext.GetRootID(),
			"NewRootID", rootID,
		).V(2).Error(nil, "Root context changed during reconcile")
	}
	c.reconcileContext.SetRootID(rootID)
}

func (c *Client) propagateLabels(obj client.Object) {
	currLabels := obj.GetLabels()
	out := make(map[string]string)
	for k, v := range currLabels {
		out[k] = v
	}
	out[tag.TraceyCreatorID] = c.id
	out[tag.TraceyRootID] = c.reconcileContext.GetRootID()
	out[tag.TraceyReconcileID] = c.reconcileContext.GetReconcileID()

	obj.SetLabels(out)
}

func (c *Client) trackOperation(ctx context.Context, obj client.Object, op OperationType) {
	// crash if any of our labeling assumptions are violated
	if err := tag.SanityCheckLabels(obj); err != nil {
		panic(err)
	}

	c.setReconcileID(ctx)

	// for read operations, set the root context for this reconcile invocation
	if op == GET || op == LIST {
		c.setRootContext(obj)

		// only log versions as they are observed during read operations
		// otherwise the logged version might miss some defaulted values
		// that downstream APIs may depend on
		if c.config.LogObjectSnapshots {
			c.logObjectVersion(obj)
		}
	}
	// assign a label to each event that changes the object
	if _, ok := mutationTypes[op]; ok {
		tag.LabelChange(obj)
	}

	// assign a sleeve object ID to the object if it is being created
	if op == CREATE {
		tag.AddSleeveObjectID(obj)
	}

	c.logOperation(obj, op)
	// propagate labels after logging so we capture the label values prior to the operation
	// e.g. we want to log out "prev-write-reconcile-id" before it gets overwritten with the current reconcileID
	c.propagateLabels(obj)
}

func (c *Client) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	c.trackOperation(ctx, obj, CREATE)
	return c.Client.Create(ctx, obj, opts...)
}

func (c *Client) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	c.trackOperation(ctx, obj, DELETE)
	return c.Client.Delete(ctx, obj, opts...)
}

func (c *Client) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	c.trackOperation(ctx, obj, DELETE)
	return c.Client.DeleteAllOf(ctx, obj, opts...)
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
	c.trackOperation(ctx, obj, GET)
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
		c.trackOperation(ctx, item, LIST)
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
	err := c.Client.Update(ctx, obj, opts...)
	if err == nil {
		// only track successful updates
		c.trackOperation(ctx, obj, UPDATE)
	} else {
		c.logger.Error(err, "operation failed, not tracking it")
	}
	return err
}

func (c *Client) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	err := c.Client.Patch(ctx, obj, patch, opts...)
	if err == nil {
		c.trackOperation(ctx, obj, PATCH)
	} else {
		c.logger.Error(err, "operation failed, not tracking it")
	}
	return err
}
