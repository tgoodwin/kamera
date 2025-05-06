package tracegen

import (
	"context"
	"fmt"
	"os"

	"strings"

	"github.com/go-logr/logr"
	"github.com/tgoodwin/sleeve/pkg/emitter"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/tag"

	// Import meta package for EachListItem
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var log = logf.Log.WithName("tracegen")

// Client wraps a controller-runtime client, adding trace generation logic
type Client struct {
	client.Client

	reconcilerID string
	logger       logr.Logger
	emitter      emitter.Emitter
	config       *Config
	tracker      *ContextTracker
}

var _ client.Client = (*Client)(nil)

func New(wrapped client.Client, reconcilerID string, emitter emitter.Emitter, tracker *ContextTracker) *Client {

	// 2. Create the tracegen Client, embedding the metrics wrapper
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
		emitter:      emitter.NewLogEmitter(log),
		config:       NewConfig(),
		tracker:      NewProdTracker(id),
	}
}

// Wrap provides a simple way to wrap an existing client.
func Wrap(c client.Client, id string) *Client {
	return newClient(c, id)
}

// WithEmitter configures the event emitter.
func (c *Client) WithEmitter(emitter emitter.Emitter) *Client {
	c.emitter = emitter
	return c
}

// WithEnvConfig configures the client based on environment variables.
func (c *Client) WithEnvConfig() *Client {
	c.logger = log
	envVars := make(map[string]string)
	for _, env := range os.Environ() {
		pair := strings.SplitN(env, "=", 2)
		if len(pair) == 2 {
			envVars[pair[0]] = pair[1]
		}
	}
	for key, value := range envVars {
		if strings.HasPrefix(key, "SLEEVE_") {
			c.logger.WithValues("key", key, "value", value).Info("configuring sleeve client from env")
		}
	}
	if logSnapshots, ok := envVars["SLEEVE_LOG_SNAPSHOTS"]; ok {
		c.config.LogObjectSnapshots = logSnapshots == "1"
	}
	if disableLogging, ok := envVars["SLEEVE_DISABLE_LOGGING"]; ok {
		c.config.disableLogging = disableLogging == "1"
	}
	return c
}

// LogOperation performs the tracegen-specific logging.
func (c *Client) LogOperation(ctx context.Context, obj client.Object, op event.OperationType) {
	if c.config.disableLogging {
		return
	}
	reconcileID := c.tracker.rc.GetReconcileID()
	rootID := c.tracker.rc.GetRootID(reconcileID)
	c.emitter.Emit(ctx, obj, op, c.reconcilerID, reconcileID, rootID)
}

func (c *Client) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	currLabels := obj.GetLabels()
	tag.AddSleeveObjectID(obj)
	tag.LabelChange(obj)
	c.tracker.propagateLabels(obj)
	if err := c.Client.Create(ctx, obj, opts...); err != nil {
		c.logger.WithValues("OpType", "CREATE").Error(err, "operation failed, not tracking it")
		obj.SetLabels(currLabels)
		return err
	}
	c.LogOperation(ctx, obj, event.CREATE)
	return nil
}

func (c *Client) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	origLabels := obj.GetLabels()
	tag.AddDeletionID(obj)
	if err := c.Client.Delete(ctx, obj, opts...); err != nil {
		c.logger.WithValues("OpType", "DELETE").Error(err, "operation failed, not tracking it")
		obj.SetLabels(origLabels)
		return err
	}
	c.LogOperation(ctx, obj, event.MARK_FOR_DELETION)
	return nil
}

func (c *Client) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	if err := c.Client.DeleteAllOf(ctx, obj, opts...); err != nil {
		c.logger.Error(err, "deleting objects")
		return err
	}
	return nil
}

func (c *Client) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := c.Client.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	c.tracker.TrackOperation(ctx, obj, event.GET)
	c.LogOperation(ctx, obj, event.GET)
	return nil
}

// --- Refactored List Method ---
func (c *Client) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	err := c.Client.List(ctx, list, opts...)
	if err != nil {
		return err
	}

	listCopy := list.DeepCopyObject()

	go func(bgCtx context.Context, bgList runtime.Object) {
		bgLogger := c.logger.WithValues("goroutine", "list_logger")
		bgLogger.Info("Starting list item logging")
		itererr := meta.EachListItem(list, func(obj runtime.Object) error {
			select {
			case <-bgCtx.Done():
				bgLogger.V(1).Info("Context cancelled during list item logging iteration")
				return bgCtx.Err() // Stop iteration if context is cancelled
			default:
			}
			// The object returned by EachListItem is runtime.Object.
			// We need to convert it to client.Object for LogOperation.
			clientObj, ok := obj.(client.Object)
			if !ok {
				// Log an error if conversion fails, but don't stop the iteration
				// unless absolutely necessary. This might happen for non-standard list types.
				bgLogger.Error(fmt.Errorf("item in list is not a client.Object: %T", obj), "List logging error")
				return nil // Continue iterating
			}
			// Log each item using the converted object
			c.LogOperation(bgCtx, clientObj, event.LIST)
			return nil // Continue iteration
		})

		if itererr != nil {
			bgLogger.Error(itererr, "Error during list item iteration/logging")
		}

	}(ctx, listCopy)

	return nil
}

func (c *Client) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	currLabels := obj.GetLabels()
	tag.LabelChange(obj)
	objPrePropagation := obj.DeepCopyObject().(client.Object)
	c.tracker.propagateLabels(obj)
	if err := c.Client.Update(ctx, obj, opts...); err != nil {
		c.logger.WithValues("OpType", "UPDATE").Error(err, "operation failed, not tracking it")
		obj.SetLabels(currLabels)
		return err
	}
	c.LogOperation(ctx, objPrePropagation, event.UPDATE)
	return nil
}

func (c *Client) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	currLabels := obj.GetLabels()
	tag.LabelChange(obj)
	objPrePropagation := obj.DeepCopyObject().(client.Object)
	c.tracker.propagateLabels(obj)
	if err := c.Client.Patch(ctx, obj, patch, opts...); err != nil {
		c.logger.WithValues("OpType", "PATCH").Error(err, "operation failed, not tracking it")
		obj.SetLabels(currLabels)
		return err
	}
	c.LogOperation(ctx, objPrePropagation, event.PATCH)
	return nil
}
