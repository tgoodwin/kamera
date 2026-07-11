package tracecheck

import (
	"fmt"

	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const initialFieldManager = "kamera-initial-state"

func (b *ExplorerBuilder) seedInitialFieldOwnership(objs []client.Object) error {
	// Identity must exist before ownerReference validation and ownership seeding.
	for idx, obj := range objs {
		if obj == nil {
			return fmt.Errorf("initial object %d is nil", idx)
		}
		tag.EnsureDeterministicIdentity(obj)
		ensureObjectGVK(obj, b.scheme)
	}
	fixupOwnerReferenceUIDs(objs)

	for idx, obj := range objs {
		gvk := ensureObjectGVK(obj, b.scheme)
		resourceSchema, registered := b.schemaRegistry.Lookup(gvk)
		if !registered {
			if b.requireSchemas {
				return fmt.Errorf("initial object %d requires a registered schema for %s", idx, gvk)
			}
			continue
		}
		if len(obj.GetManagedFields()) > 0 {
			continue
		}

		desired, err := util.ConvertToUnstructured(obj)
		if err != nil {
			return fmt.Errorf("converting initial object %d: %w", idx, err)
		}
		desired.SetGroupVersionKind(gvk)
		created, err := resourceSchema.Create(desired, initialFieldManager)
		if err != nil {
			return fmt.Errorf("seeding field ownership for initial %s %s/%s: %w", gvk, obj.GetNamespace(), obj.GetName(), err)
		}

		if resourceSchema.HasStatus {
			if status, found := desired.Object["status"]; found {
				statusIntent := &unstructured.Unstructured{Object: map[string]interface{}{
					"apiVersion": gvk.GroupVersion().String(),
					"kind":       gvk.Kind,
					"metadata": map[string]interface{}{
						"name":      desired.GetName(),
						"namespace": desired.GetNamespace(),
					},
					"status": status,
				}}
				created, err = resourceSchema.Apply(created, statusIntent, initialFieldManager+"-status", true, "status")
				if err != nil {
					return fmt.Errorf("seeding status ownership for initial %s %s/%s: %w", gvk, obj.GetNamespace(), obj.GetName(), err)
				}
			}
		}
		if err := copyUnstructuredInto(obj, created, b.scheme); err != nil {
			return fmt.Errorf("copying initial schema-backed object %d: %w", idx, err)
		}
	}
	return nil
}
