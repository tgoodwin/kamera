package apiserver

import (
	"fmt"
	"reflect"

	"github.com/tgoodwin/kamera/pkg/tag"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apivalidation "k8s.io/apimachinery/pkg/api/validation"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/managedfields"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Apply materializes one server-side apply request against the live object.
// A nil live object uses SSA create semantics.
func (s *ResourceSchema) Apply(
	live *unstructured.Unstructured,
	intent *unstructured.Unstructured,
	manager string,
	force bool,
	subresource string,
) (*unstructured.Unstructured, error) {
	if manager == "" {
		return nil, apierrors.NewBadRequest("fieldManager is required for server-side apply")
	}
	if intent == nil {
		return nil, apierrors.NewBadRequest("apply configuration is nil")
	}
	if intent.GroupVersionKind() != s.GVK {
		return nil, apierrors.NewBadRequest(fmt.Sprintf(
			"apply configuration GVK %s does not match registered schema %s",
			intent.GroupVersionKind(), s.GVK,
		))
	}

	creating := live == nil
	base := live
	if base == nil {
		base = newUnstructured(s.GVK)
	}

	fieldManager, err := s.managerFor(subresource)
	if err != nil {
		return nil, err
	}
	result, err := fieldManager.Apply(base.DeepCopy(), intent.DeepCopy(), manager, force)
	if err != nil {
		return nil, err
	}
	merged, err := asUnstructured(result)
	if err != nil {
		return nil, err
	}
	merged = s.enforceSubresource(base, merged, subresource, creating)
	normalizeManagedFieldTimes(merged)
	s.finishServerMetadata(base, merged, subresource, creating)
	if err := s.validate(merged, base, creating); err != nil {
		return nil, err
	}
	return merged, nil
}

// Create materializes field ownership for a newly-created object.
func (s *ResourceSchema) Create(
	desired *unstructured.Unstructured,
	manager string,
) (*unstructured.Unstructured, error) {
	if desired == nil {
		return nil, apierrors.NewBadRequest("create object is nil")
	}
	base := newUnstructured(s.GVK)
	prepared := desired.DeepCopy()
	if s.HasStatus {
		delete(prepared.Object, "status")
	}
	result, err := s.mainManager.Update(base, prepared, manager)
	if err != nil {
		return nil, err
	}
	created, err := asUnstructured(result)
	if err != nil {
		return nil, err
	}
	normalizeManagedFieldTimes(created)
	s.finishServerMetadata(base, created, "", true)
	if err := s.validate(created, base, true); err != nil {
		return nil, err
	}
	return created, nil
}

// Update updates managed fields after a full-object or already-materialized
// non-apply write. Patch documents must be materialized before calling Update.
func (s *ResourceSchema) Update(
	live *unstructured.Unstructured,
	desired *unstructured.Unstructured,
	manager string,
	subresource string,
) (*unstructured.Unstructured, error) {
	if live == nil {
		return nil, apierrors.NewNotFound(schema.GroupResource{Group: s.GVK.Group, Resource: s.GVK.Kind}, desired.GetName())
	}
	fieldManager, err := s.managerFor(subresource)
	if err != nil {
		return nil, err
	}
	prepared := s.enforceSubresource(live, desired.DeepCopy(), subresource, false)
	result, err := fieldManager.Update(live.DeepCopy(), prepared, manager)
	if err != nil {
		return nil, err
	}
	updated, err := asUnstructured(result)
	if err != nil {
		return nil, err
	}
	updated = s.enforceSubresource(live, updated, subresource, false)
	normalizeManagedFieldTimes(updated)
	s.finishServerMetadata(live, updated, subresource, false)
	if err := s.validate(updated, live, false); err != nil {
		return nil, err
	}
	return updated, nil
}

func (s *ResourceSchema) managerFor(subresource string) (*managedfields.FieldManager, error) {
	switch subresource {
	case "":
		return s.mainManager, nil
	case "status":
		if !s.HasStatus || s.statusManager == nil {
			return nil, apierrors.NewBadRequest(fmt.Sprintf("%s does not expose a status subresource", s.GVK))
		}
		return s.statusManager, nil
	default:
		return nil, apierrors.NewBadRequest(fmt.Sprintf("unsupported subresource %q for %s", subresource, s.GVK))
	}
}

func (s *ResourceSchema) enforceSubresource(
	live *unstructured.Unstructured,
	candidate *unstructured.Unstructured,
	subresource string,
	creating bool,
) *unstructured.Unstructured {
	if creating {
		if s.HasStatus && subresource == "" {
			delete(candidate.Object, "status")
		}
		return candidate
	}
	if subresource == "status" {
		result := live.DeepCopy()
		if status, found := candidate.Object["status"]; found {
			result.Object["status"] = runtime.DeepCopyJSONValue(status)
		} else {
			delete(result.Object, "status")
		}
		result.SetManagedFields(candidate.GetManagedFields())
		return result
	}
	if s.HasStatus {
		if status, found := live.Object["status"]; found {
			candidate.Object["status"] = runtime.DeepCopyJSONValue(status)
		} else {
			delete(candidate.Object, "status")
		}
	}
	return candidate
}

func (s *ResourceSchema) finishServerMetadata(
	live *unstructured.Unstructured,
	result *unstructured.Unstructured,
	subresource string,
	creating bool,
) {
	result.SetGroupVersionKind(s.GVK)
	if creating {
		tag.EnsureDeterministicIdentity(result)
		if result.GetResourceVersion() == "" {
			result.SetResourceVersion("1")
		}
		if result.GetGeneration() == 0 {
			result.SetGeneration(1)
		}
		return
	}

	result.SetUID(live.GetUID())
	result.SetCreationTimestamp(live.GetCreationTimestamp())
	result.SetResourceVersion(live.GetResourceVersion())
	if subresource == "status" {
		result.SetGeneration(live.GetGeneration())
		return
	}
	if !reflect.DeepEqual(live.Object["spec"], result.Object["spec"]) {
		generation := live.GetGeneration()
		if generation == 0 {
			generation = 1
		} else {
			generation++
		}
		result.SetGeneration(generation)
	} else {
		result.SetGeneration(live.GetGeneration())
	}
}

func (s *ResourceSchema) validate(
	result *unstructured.Unstructured,
	live *unstructured.Unstructured,
	creating bool,
) error {
	path := field.NewPath("metadata")
	var errs field.ErrorList
	if creating {
		errs = apivalidation.ValidateObjectMetaAccessor(result, s.Namespaced, apivalidation.NameIsDNSSubdomain, path)
	} else {
		errs = apivalidation.ValidateObjectMetaAccessorUpdate(result, live, path)
		errs = append(errs, apivalidation.ValidateOwnerReferences(result.GetOwnerReferences(), path.Child("ownerReferences"))...)
	}
	if len(errs) > 0 {
		return apierrors.NewInvalid(s.GVK.GroupKind(), result.GetName(), errs)
	}
	return nil
}

func asUnstructured(obj runtime.Object) (*unstructured.Unstructured, error) {
	result, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("managed fields returned %T, want *unstructured.Unstructured", obj)
	}
	return result, nil
}

func normalizeManagedFieldTimes(obj *unstructured.Unstructured) {
	entries := obj.GetManagedFields()
	for idx := range entries {
		// Wall-clock timestamps are observational metadata and would make equal
		// exploration branches hash differently. Ownership and operation data
		// remain byte-for-byte compatible with Kubernetes managedFields.
		entries[idx].Time = nil
	}
	obj.SetManagedFields(entries)
}
