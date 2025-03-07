package replay

import (
	"sync"

	"github.com/tgoodwin/sleeve/pkg/event"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ResourceKey struct {
	Kind      string
	Namespace string
	Name      string
}

type ResourceConflictValidator struct {
	resources map[ResourceKey]struct{}
	mu        sync.RWMutex
}

func NewResourceConflictValidator() *ResourceConflictValidator {
	return &ResourceConflictValidator{
		resources: make(map[ResourceKey]struct{}),
	}
}

// ValidateOperation checks if an operation would result in a conflict
// and automatically updates the internal tracking state.
func (v *ResourceConflictValidator) ValidateOperation(op event.OperationType, obj client.Object) error {
	gvk := obj.GetObjectKind().GroupVersionKind()
	key := ResourceKey{
		Kind:      gvk.Kind,
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	_, exists := v.resources[key]

	switch op {
	case event.CREATE:
		if exists {
			return apierrors.NewAlreadyExists(
				schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind},
				obj.GetName())
		}
		// Automatically track the resource if CREATE is valid
		v.resources[key] = struct{}{}

	case event.GET:
		if !exists {
			return apierrors.NewNotFound(
				schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind},
				obj.GetName())
		}
		// GET is read-only, no state changes

	case event.LIST:
		// LIST doesn't operate on a specific object
		// Always succeeds, returns empty list if no objects match
		return nil

	case event.UPDATE, event.PATCH:
		if !exists {
			return apierrors.NewNotFound(
				schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind},
				obj.GetName())
		}
		// No need to change tracking state for UPDATE/PATCH

	case event.DELETE:
		if !exists {
			return apierrors.NewNotFound(
				schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind},
				obj.GetName())
		}
		// Automatically remove tracking if DELETE is valid
		delete(v.resources, key)

	case event.APPLY:
		// APPLY implements upsert semantics - creates or updates as needed
		if !exists {
			// Add it for a new resource
			v.resources[key] = struct{}{}
		}
		// Existing resource just gets updated, no change to tracking state
	}

	return nil
}
