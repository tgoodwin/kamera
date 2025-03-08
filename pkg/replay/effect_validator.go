package replay

import (
	"fmt"
	"sync"

	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ResourceConflictManager struct {
	resources map[snapshot.ResourceKey]struct{}
	mu        sync.RWMutex
}

func NewResourceConflictManager(keyStore map[snapshot.ResourceKey]struct{}) *ResourceConflictManager {
	return &ResourceConflictManager{
		resources: keyStore,
	}
}

// ValidateOperation checks if an operation would result in a conflict
// and automatically updates the internal tracking state.
func (v *ResourceConflictManager) ValidateOperation(op event.OperationType, obj client.Object) error {
	gvk := obj.GetObjectKind().GroupVersionKind()
	key := snapshot.ResourceKey{
		Kind:      gvk.Kind,
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	_, exists := v.resources[key]
	if !exists && op != event.CREATE {
		fmt.Println("resource does not exist in the following keys")
		for k := range v.resources {
			fmt.Println(k)
		}
	}

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
			fmt.Println("KEY NOT FOUND", key)
			return apierrors.NewNotFound(
				schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind},
				obj.GetName())
		}
		// Automatically remove tracking if DELETE is valid
		fmt.Println("----deleting key----", key)
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
