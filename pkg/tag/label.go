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

	// Special stable ID for deletion events
	DeletionID = "discrete.events/deletion-id"
)

// LabelChange sets a change-id on the object to associate an object's current value with the change event that produced it.
func LabelChange(obj client.Object) {
	addUIDTag(obj, ChangeID)
}

func AddSleeveObjectID(obj client.Object) {
	addUIDTag(obj, TraceyObjectID)
}

func AddDeletionID(obj client.Object) {
	labels := obj.GetLabels()
	if _, ok := labels[DeletionID]; !ok {
		addUIDTag(obj, DeletionID)
	}
}

func addUIDTag(obj client.Object, key string) {
	labels := obj.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}
	labels[key] = uuid.New().String()
	obj.SetLabels(labels)
}

func GetChangeLabel() map[string]string {
	labels := make(map[string]string)
	labels[ChangeID] = uuid.New().String()
	return labels
}

func GetSleeveObjectID(obj client.Object) string {
	fallback := string(obj.GetUID())
	labels := obj.GetLabels()
	if labels == nil {
		return fallback
	}
	if id, ok := labels[TraceyObjectID]; ok {
		return id
	}
	return fallback
}

func SanityCheckLabels(obj client.Object) error {
	labels := obj.GetLabels()
	if labels == nil {
		return nil
	}
	if webhookLabel, ok := labels[TraceyWebhookLabel]; ok {
		if rootID, ok := labels[TraceyRootID]; ok {
			if webhookLabel != rootID && rootID != "" {
				// logf.Log.WithValues("key", "val").Error(nil, "labeling assumptions violated")
				return fmt.Errorf("labeling assumptions violated: tracey-uid=%s, root-event-id=%s", webhookLabel, rootID)

			}
		}
	}
	return nil
}

func GetRootID(obj client.Object) (string, error) {
	labels := obj.GetLabels()
	if labels == nil {
		return "", fmt.Errorf("no labels found")
	}
	// set by the webhook
	rootID, ok := labels[TraceyWebhookLabel]
	if ok {
		return rootID, nil
	}
	if !ok {
		rootID, ok = labels[TraceyRootID]
		if !ok || rootID == "" {
			return "", fmt.Errorf("no root ID set on object labels")
		}
	}
	return rootID, nil
}

type LabelContext struct {
	RootID       string
	TraceID      string
	ParentID     string
	SourceObject string
}
