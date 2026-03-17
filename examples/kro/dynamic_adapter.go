package main

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// replayDynamicClient adapts a controller-runtime client.Client to the
// k8s.io/client-go/dynamic.Interface used by KRO's controllers.
type replayDynamicClient struct {
	inner  client.Client
	mapper meta.RESTMapper
}

var _ dynamic.Interface = (*replayDynamicClient)(nil)

func newReplayDynamicClient(c client.Client, mapper meta.RESTMapper) *replayDynamicClient {
	return &replayDynamicClient{inner: c, mapper: mapper}
}

func (d *replayDynamicClient) Resource(resource schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	return &replayNamespaceableResource{inner: d.inner, mapper: d.mapper, gvr: resource}
}

type replayNamespaceableResource struct {
	inner  client.Client
	mapper meta.RESTMapper
	gvr    schema.GroupVersionResource
}

var _ dynamic.NamespaceableResourceInterface = (*replayNamespaceableResource)(nil)

func (r *replayNamespaceableResource) Namespace(ns string) dynamic.ResourceInterface {
	return &replayResourceClient{inner: r.inner, mapper: r.mapper, gvr: r.gvr, namespace: ns}
}

func (r *replayNamespaceableResource) clusterScoped() dynamic.ResourceInterface {
	return &replayResourceClient{inner: r.inner, mapper: r.mapper, gvr: r.gvr}
}

func (r *replayNamespaceableResource) Create(ctx context.Context, obj *unstructured.Unstructured, opts metav1.CreateOptions, subresources ...string) (*unstructured.Unstructured, error) {
	return r.clusterScoped().Create(ctx, obj, opts, subresources...)
}
func (r *replayNamespaceableResource) Update(ctx context.Context, obj *unstructured.Unstructured, opts metav1.UpdateOptions, subresources ...string) (*unstructured.Unstructured, error) {
	return r.clusterScoped().Update(ctx, obj, opts, subresources...)
}
func (r *replayNamespaceableResource) UpdateStatus(ctx context.Context, obj *unstructured.Unstructured, opts metav1.UpdateOptions) (*unstructured.Unstructured, error) {
	return r.clusterScoped().UpdateStatus(ctx, obj, opts)
}
func (r *replayNamespaceableResource) Delete(ctx context.Context, name string, opts metav1.DeleteOptions, subresources ...string) error {
	return r.clusterScoped().Delete(ctx, name, opts, subresources...)
}
func (r *replayNamespaceableResource) DeleteCollection(ctx context.Context, opts metav1.DeleteOptions, listOpts metav1.ListOptions) error {
	return r.clusterScoped().DeleteCollection(ctx, opts, listOpts)
}
func (r *replayNamespaceableResource) Get(ctx context.Context, name string, opts metav1.GetOptions, subresources ...string) (*unstructured.Unstructured, error) {
	return r.clusterScoped().Get(ctx, name, opts, subresources...)
}
func (r *replayNamespaceableResource) List(ctx context.Context, opts metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	return r.clusterScoped().List(ctx, opts)
}
func (r *replayNamespaceableResource) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	return r.clusterScoped().Watch(ctx, opts)
}
func (r *replayNamespaceableResource) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (*unstructured.Unstructured, error) {
	return r.clusterScoped().Patch(ctx, name, pt, data, opts, subresources...)
}
func (r *replayNamespaceableResource) Apply(ctx context.Context, name string, obj *unstructured.Unstructured, opts metav1.ApplyOptions, subresources ...string) (*unstructured.Unstructured, error) {
	return r.clusterScoped().Apply(ctx, name, obj, opts, subresources...)
}
func (r *replayNamespaceableResource) ApplyStatus(ctx context.Context, name string, obj *unstructured.Unstructured, opts metav1.ApplyOptions) (*unstructured.Unstructured, error) {
	return r.clusterScoped().ApplyStatus(ctx, name, obj, opts)
}

// replayResourceClient implements dynamic.ResourceInterface by delegating to
// the replay client.Client. Resolves GVR→GVK via REST mapper.
type replayResourceClient struct {
	inner     client.Client
	mapper    meta.RESTMapper
	gvr       schema.GroupVersionResource
	namespace string
}

var _ dynamic.ResourceInterface = (*replayResourceClient)(nil)

func (rc *replayResourceClient) gvk() (schema.GroupVersionKind, error) {
	kinds, err := rc.mapper.KindsFor(rc.gvr)
	if err != nil {
		return schema.GroupVersionKind{}, fmt.Errorf("resolve GVK for %s: %w", rc.gvr, err)
	}
	if len(kinds) == 0 {
		return schema.GroupVersionKind{}, fmt.Errorf("no GVK found for %s", rc.gvr)
	}
	return kinds[0], nil
}

func (rc *replayResourceClient) ensureTypeMeta(obj *unstructured.Unstructured) error {
	if obj.GetAPIVersion() != "" && obj.GetKind() != "" {
		return nil
	}
	gvk, err := rc.gvk()
	if err != nil {
		return err
	}
	obj.SetGroupVersionKind(gvk)
	return nil
}

func (rc *replayResourceClient) Create(ctx context.Context, obj *unstructured.Unstructured, opts metav1.CreateOptions, subresources ...string) (*unstructured.Unstructured, error) {
	if err := rc.ensureTypeMeta(obj); err != nil {
		return nil, err
	}
	if rc.namespace != "" && obj.GetNamespace() == "" {
		obj.SetNamespace(rc.namespace)
	}
	if err := rc.inner.Create(ctx, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func (rc *replayResourceClient) Update(ctx context.Context, obj *unstructured.Unstructured, opts metav1.UpdateOptions, subresources ...string) (*unstructured.Unstructured, error) {
	if err := rc.ensureTypeMeta(obj); err != nil {
		return nil, err
	}
	if len(subresources) > 0 && subresources[0] == "status" {
		return rc.UpdateStatus(ctx, obj, metav1.UpdateOptions{})
	}
	if err := rc.inner.Update(ctx, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func (rc *replayResourceClient) UpdateStatus(ctx context.Context, obj *unstructured.Unstructured, opts metav1.UpdateOptions) (*unstructured.Unstructured, error) {
	if err := rc.ensureTypeMeta(obj); err != nil {
		return nil, err
	}
	if err := rc.inner.Status().Update(ctx, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func (rc *replayResourceClient) Delete(ctx context.Context, name string, opts metav1.DeleteOptions, subresources ...string) error {
	gvk, err := rc.gvk()
	if err != nil {
		return err
	}
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	obj.SetName(name)
	obj.SetNamespace(rc.namespace)
	return rc.inner.Delete(ctx, obj)
}

func (rc *replayResourceClient) DeleteCollection(ctx context.Context, opts metav1.DeleteOptions, listOpts metav1.ListOptions) error {
	gvk, err := rc.gvk()
	if err != nil {
		return err
	}
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	return rc.inner.DeleteAllOf(ctx, obj, client.InNamespace(rc.namespace))
}

func (rc *replayResourceClient) Get(ctx context.Context, name string, opts metav1.GetOptions, subresources ...string) (*unstructured.Unstructured, error) {
	gvk, err := rc.gvk()
	if err != nil {
		return nil, err
	}
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	key := client.ObjectKey{Namespace: rc.namespace, Name: name}
	if err := rc.inner.Get(ctx, key, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func (rc *replayResourceClient) List(ctx context.Context, opts metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	gvk, err := rc.gvk()
	if err != nil {
		return nil, err
	}
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(gvk.GroupVersion().WithKind(gvk.Kind + "List"))
	listOpts := []client.ListOption{client.InNamespace(rc.namespace)}
	if opts.LabelSelector != "" {
		selector, err := labels.Parse(opts.LabelSelector)
		if err != nil {
			return nil, err
		}
		listOpts = append(listOpts, client.MatchingLabelsSelector{Selector: selector})
	}
	if err := rc.inner.List(ctx, list, listOpts...); err != nil {
		return nil, err
	}
	return list, nil
}

func (rc *replayResourceClient) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	return nil, fmt.Errorf("Watch not supported in replay mode")
}

func (rc *replayResourceClient) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (*unstructured.Unstructured, error) {
	gvk, err := rc.gvk()
	if err != nil {
		return nil, err
	}
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	obj.SetName(name)
	obj.SetNamespace(rc.namespace)
	patch := client.RawPatch(pt, data)
	if err := rc.inner.Patch(ctx, obj, patch); err != nil {
		return nil, err
	}
	return obj, nil
}

func (rc *replayResourceClient) Apply(ctx context.Context, name string, obj *unstructured.Unstructured, opts metav1.ApplyOptions, subresources ...string) (*unstructured.Unstructured, error) {
	if err := rc.ensureTypeMeta(obj); err != nil {
		return nil, err
	}
	if rc.namespace != "" && obj.GetNamespace() == "" {
		obj.SetNamespace(rc.namespace)
	}
	patch := client.Apply
	fo := client.ForceOwnership
	fm := client.FieldOwner(opts.FieldManager)
	if len(subresources) > 0 && subresources[0] == "status" {
		if err := rc.inner.Status().Patch(ctx, obj, patch, fo, fm); err != nil {
			return nil, err
		}
		return obj, nil
	}
	if err := rc.inner.Patch(ctx, obj, patch, fo, fm); err != nil {
		return nil, err
	}
	return obj, nil
}

func (rc *replayResourceClient) ApplyStatus(ctx context.Context, name string, obj *unstructured.Unstructured, opts metav1.ApplyOptions) (*unstructured.Unstructured, error) {
	return rc.Apply(ctx, name, obj, opts, "status")
}
