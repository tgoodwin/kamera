package replay

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/util"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
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

// Scheme overrides the embedded dummyClient.Scheme to return the actual scheme.
func (c *Client) Scheme() *runtime.Scheme {
	return c.scheme
}

func (c *Client) objectGVK(obj runtime.Object) schema.GroupVersionKind {
	if obj == nil {
		return schema.GroupVersionKind{}
	}
	if c.scheme != nil {
		if gvk, err := apiutil.GVKForObject(obj, c.scheme); err == nil {
			return gvk
		}
	}
	return util.GetGroupVersionKind(obj)
}

func (c *Client) canonicalKindFor(obj runtime.Object, fallback string) string {
	gvk := c.objectGVK(obj)
	kind := gvk.Kind
	if strings.HasSuffix(kind, "List") {
		kind = strings.TrimSuffix(kind, "List")
	}
	if kind == "" {
		kind = fallback
	}
	return util.CanonicalGroupKind(gvk.Group, kind)
}

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

func (c *Client) handleEffect(ctx context.Context, obj client.Object, opType event.OperationType, preconditions *PreconditionInfo, options *EffectOptions) error {
	// TODO validate preconditions
	tag.EnsureDeterministicIdentity(obj)
	return c.recorder.RecordEffect(ctx, obj, opType, preconditions, options)
}

func (c *Client) copyInto(obj client.Object, from *unstructured.Unstructured) error {
	if unstr, ok := obj.(runtime.Unstructured); ok {
		unstr.SetUnstructuredContent(from.DeepCopy().Object)
		return nil
	}

	if c.scheme != nil {
		if err := c.scheme.Convert(from, obj, nil); err == nil {
			return nil
		}
	}

	return runtime.DefaultUnstructuredConverter.FromUnstructured(from.Object, obj)
}

func (c *Client) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	logger = log.FromContext(ctx)
	frameID := FrameIDFromContext(ctx)
	kind := util.GetKind(obj)
	canonicalKind := c.canonicalKindFor(obj, kind)
	if canonicalKind == "" {
		return fmt.Errorf("unable to determine canonical kind for object %T", obj)
	}
	logger.V(2).Info("client:get", "Key", key, "Kind", kind, "CanonicalKind", canonicalKind)

	frame, err := c.GetCacheFrame(frameID)
	if err != nil {
		logger.V(2).Info("frame NOT found!", "FrameID", frameID)
		return fmt.Errorf("frame %s not found", frameID)
	}

	gvk := c.objectGVK(obj)
	if gvk.Kind == "" {
		gvk.Kind = kind
	}

	nn := types.NamespacedName{Namespace: key.Namespace, Name: key.Name}
	objsForKind := frame[canonicalKind]
	frozenObj, ok := objsForKind[nn]
	if !ok {
		logger.V(1).Info("client:get cache miss",
			"canonicalKind", canonicalKind,
			"namespace", key.Namespace,
			"name", key.Name)
		return apierrors.NewNotFound(schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}, key.Name)
	}

		if err := c.handleEffect(ctx, frozenObj, event.GET, nil, nil); err != nil {
		logger.V(1).Error(err,
			"canonicalKind", canonicalKind,
			"namespace", key.Namespace,
			"name", key.Name)
		return err
	}

	if err := c.copyInto(obj, frozenObj); err != nil {
		return fmt.Errorf("converting cached object: %w", err)
	}

	return nil
}

func (c *Client) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	frameID := FrameIDFromContext(ctx)
	kind := util.InferListKind(list)
	canonicalKind := c.canonicalKindFor(list.(runtime.Object), kind)
	if canonicalKind == "" {
		return fmt.Errorf("unable to determine canonical kind for list %T", list)
	}

	frame, err := c.GetCacheFrame(frameID)
	if err != nil {
		return fmt.Errorf("frame %s not found", frameID)
	}

	// Extract list options for filtering.
	listOpts := &client.ListOptions{}
	for _, opt := range opts {
		opt.ApplyToList(listOpts)
	}

	itemsValue := reflect.ValueOf(list).Elem().FieldByName("Items")
	if !itemsValue.IsValid() {
		return fmt.Errorf("List object does not have Items field")
	}

	itemType := itemsValue.Type().Elem()
	objsForKind := frame[canonicalKind]
	newSlice := reflect.MakeSlice(reflect.SliceOf(itemType), 0, len(objsForKind))

	if logger.V(5).Enabled() {
		keys := make([]types.NamespacedName, 0, len(objsForKind))
		for nk := range objsForKind {
			keys = append(keys, nk)
		}
		logger.V(5).Info("client:list frame keys",
			"canonicalKind", canonicalKind,
			"keys", keys)
	}

	for _, obj := range objsForKind {
		// Namespace filter.
		if listOpts.Namespace != "" && obj.GetNamespace() != listOpts.Namespace {
			continue
		}
		// Label selector filter.
		if listOpts.LabelSelector != nil && !listOpts.LabelSelector.Matches(labels.Set(obj.GetLabels())) {
			continue
		}
		// Field selector filter.
		if listOpts.FieldSelector != nil && !listOpts.FieldSelector.Empty() {
			if !matchesFieldSelector(obj, listOpts.FieldSelector) {
				continue
			}
		}

		if err := c.handleEffect(ctx, obj, event.LIST, nil, nil); err != nil {
			return err
		}

		newObj := reflect.New(itemType).Interface().(client.Object)
		if err := c.copyInto(newObj, obj); err != nil {
			return fmt.Errorf("converting cached object: %w", err)
		}

		newSlice = reflect.Append(newSlice, reflect.ValueOf(newObj).Elem())
	}

	itemsValue.Set(newSlice)
	return nil
}

// matchesFieldSelector checks whether an unstructured object satisfies the
// given field selector. It extracts only the fields referenced by the
// selector's requirements, using dot-separated paths to navigate the nested
// object map (e.g. "spec.providerID", "status.providerID").
func matchesFieldSelector(obj *unstructured.Unstructured, sel fields.Selector) bool {
	fs := fields.Set{
		"metadata.name":      obj.GetName(),
		"metadata.namespace": obj.GetNamespace(),
	}
	for _, req := range sel.Requirements() {
		if _, ok := fs[req.Field]; ok {
			continue // already populated
		}
		parts := strings.Split(req.Field, ".")
		val, found, _ := unstructured.NestedFieldNoCopy(obj.Object, parts...)
		if found {
			if s, ok := val.(string); ok {
				fs[req.Field] = s
			}
		}
	}
	return sel.Matches(fs)
}

// TODO create or set an ObjectID here
func (c *Client) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	preconditions := ExtractCreatePreconditions(opts)
	return c.handleEffect(ctx, obj, event.CREATE, &preconditions, nil)
}

func (c *Client) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	// in the replay client, we're not actually interacting with the API server
	// so the object won't take on a deletion timestamp unless we set it here.
	ts := v1.Time{Time: time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC)}
	obj.SetDeletionTimestamp(&ts)

	preconditions := ExtractDeletePreconditions(opts)
	return c.handleEffect(ctx, obj, event.MARK_FOR_DELETION, &preconditions, nil)
}

func (c *Client) Remove(ctx context.Context, obj client.Object) error {
	// preconditions := ExtractRemovePreconditions(opts)
	return c.handleEffect(ctx, obj, event.REMOVE, nil, nil)
}

func (c *Client) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	preconditions := ExtractUpdatePreconditions(opts)
	return c.handleEffect(ctx, obj, event.UPDATE, &preconditions, nil)
}

func (c *Client) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	preconditions := ExtractDeleteAllOfPreconditions(opts)
	return c.handleEffect(ctx, obj, event.MARK_FOR_DELETION, &preconditions, nil)
}

func (c *Client) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	preconditions := ExtractPatchPreconditions(opts)
	op := event.PATCH
	if patch.Type() == types.ApplyPatchType {
		// Server-side apply uses PATCH with apply semantics; model it separately.
		op = event.APPLY
	}
	return c.handleEffect(ctx, obj, op, &preconditions, nil)
}

// Apply models server-side apply operations introduced in controller-runtime v0.22+.
func (c *Client) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
	preconditions := ExtractApplyPreconditions(opts)
	// NOTE: We only record the APPLY effect; we do not materialize merged fields.
	applyObj, err := applyConfigToUnstructured(obj)
	if err != nil {
		return err
	}
	return c.handleEffect(ctx, applyObj, event.APPLY, &preconditions, nil)
}

func applyConfigToUnstructured(obj runtime.ApplyConfiguration) (*unstructured.Unstructured, error) {
	// Check if the apply configuration wraps an unstructured.Unstructured
	// (e.g., controller-runtime's unstructuredApplyConfiguration used for SSA
	// on unstructured objects). This is the most common case for CAPI controllers.
	type unstructuredProvider interface {
		GetUnstructured() *unstructured.Unstructured
	}
	if up, ok := obj.(unstructuredProvider); ok {
		return up.GetUnstructured().DeepCopy(), nil
	}

	// Check for embedded *unstructured.Unstructured via the runtime.Object interface.
	if ro, ok := obj.(interface{ DeepCopyObject() runtime.Object }); ok {
		if u, ok := ro.DeepCopyObject().(*unstructured.Unstructured); ok {
			return u, nil
		}
	}

	// Fall back to the typed apply configuration interface.
	ac, ok := obj.(interface {
		GetName() *string
		GetNamespace() *string
		GetKind() *string
		GetAPIVersion() *string
	})
	if !ok {
		return nil, fmt.Errorf("%T is a runtime.ApplyConfiguration but not an applyConfiguration", obj)
	}

	u := &unstructured.Unstructured{}
	if name := ptr.Deref(ac.GetName(), ""); name != "" {
		u.SetName(name)
	}
	if ns := ptr.Deref(ac.GetNamespace(), ""); ns != "" {
		u.SetNamespace(ns)
	}
	u.SetKind(ptr.Deref(ac.GetKind(), ""))
	u.SetAPIVersion(ptr.Deref(ac.GetAPIVersion(), ""))
	return u, nil
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
	return c.wrapped.handleEffect(ctx, obj, event.UPDATE, &preconditions, &EffectOptions{Subresource: "status"})
}

func (c *subResourceClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	preconditions := ExtractStatusPatchPreconditions(opts)
	op := event.PATCH
	if patch.Type() == types.ApplyPatchType {
		// Server-side apply uses PATCH with apply semantics; model it separately.
		op = event.APPLY
	}
	return c.wrapped.handleEffect(ctx, obj, op, &preconditions, &EffectOptions{Subresource: "status"})
}

func (c *subResourceClient) Create(ctx context.Context, obj client.Object, sub client.Object, opts ...client.SubResourceCreateOption) error {
	preconditions := ExtractSubResourceCreatePreconditions(opts)
	return c.wrapped.handleEffect(ctx, obj, event.CREATE, &preconditions, nil)
}

func (c *subResourceClient) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
	u, err := applyConfigToUnstructured(obj)
	if err != nil {
		return err
	}
	return c.wrapped.handleEffect(ctx, u, event.APPLY, nil, &EffectOptions{Subresource: "status"})
}
