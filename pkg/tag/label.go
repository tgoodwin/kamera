package tag

import (
	"fmt"

	"github.com/google/uuid"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// set by the webhook only
	TraceyWebhookLabel = "tracey-uid"

	// the ID of the reconcile invocation in which the object was acted upon
	TraceyReconcileID = "discrete.events/prev-write-reconcile-id"

	// the ID of the controller that acted upon the object
	TraceyCreatorID = "discrete.events/creator-id"

	// an stable ID assigned at creation time that is separate from the object's UID.
	// we use this ID to to address the fact that an object's UID is not yet present
	// at creation time until the create operation is processed by the API server.
	TraceyObjectID = "discrete.events/sleeve-object-id"

	// the ID of the root event that caused the object to be acted upon.
	// the value originates from a TraceyWebhookLabel value but we just
	// use a different name when propagating the value.
	TraceyRootID = "discrete.events/root-event-id"

	ChangeID = "discrete.events/change-id"
)

// LabelChange sets a change-id on the object to associate an object's current value with the change event that produced it.
// It returns a function that can be used to revert the object to its original state
// because we add a label before issuing a mutting API call, but if the call fails we need to revert the object to its original state
// so we dont incorrectly associate the object with a change event that did not actually occur.
func LabelChange(obj client.Object) func(o client.Object) {
	originalLabels := obj.GetLabels()
	labels := make(map[string]string)
	for k, v := range originalLabels {
		labels[k] = v
	}
	labels[ChangeID] = uuid.New().String()
	obj.SetLabels(labels)
	return func(o client.Object) {
		o.SetLabels(originalLabels)
	}
}

func AddSleeveObjectID(obj client.Object) {
	labels := obj.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}
	labels[TraceyObjectID] = uuid.New().String()
	obj.SetLabels(labels)
}

func GetChangeLabel() map[string]string {
	labels := make(map[string]string)
	labels[ChangeID] = uuid.New().String()
	return labels
}

func SanityCheckLabels(obj client.Object) error {
	labels := obj.GetLabels()
	if labels == nil {
		return nil
	}
	if webhookLabel, ok := labels[TraceyWebhookLabel]; ok {
		if rootID, ok := labels[TraceyRootID]; ok {
			if webhookLabel != rootID {
				// logf.Log.WithValues("key", "val").Error(nil, "labeling assumptions violated")
				return fmt.Errorf("labeling assumptions violated: tracey-uid=%s, root-event-id=%s", webhookLabel, rootID)

			}
		}
	}
	return nil
}

type LabelContext struct {
	RootID       string
	TraceID      string
	ParentID     string
	SourceObject string
}
