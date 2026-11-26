package tracecheck

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/evanphx/json-patch/v5"

	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/replay"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/util"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	kubejson "k8s.io/apimachinery/pkg/util/json"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type missingSchemaError struct {
	op          event.OperationType
	subresource string
	reconciler  ReconcilerID
	gvk         schema.GroupVersionKind
	key         client.ObjectKey
}

func (e *missingSchemaError) Error() string {
	op := string(e.op)
	if e.subresource != "" {
		op += " /" + e.subresource
	}
	return fmt.Sprintf("schema-backed %s by reconciler %q requires a registered schema for %s resource %s; register it with WithCRD, WithResourceSchema, or WithOpenAPIV3 with explicit resource metadata", op, e.reconciler, e.gvk, e.key)
}

func isMissingSchemaError(err error) bool {
	var missing *missingSchemaError
	return errors.As(err, &missing)
}

// materializeSchemaWrite applies API-server semantics before the effect is
// published. Dry runs and unchanged writes return a response without recording
// an effect. RecordEffect owns version allocation for every durable write.
func (m *manager) materializeSchemaWrite(
	ctx context.Context,
	obj client.Object,
	op event.OperationType,
	precondition *replay.PreconditionInfo,
	options *replay.EffectOptions,
) (materialized bool, changed bool, err error) {
	if obj == nil {
		return false, false, apierrors.NewBadRequest("write object is nil")
	}
	gvk := ensureObjectGVK(obj, m.scheme)
	resourceSchema, registered := m.schemaRegistry.Lookup(gvk)
	if !registered {
		if m.requireSchemas || (m.requireApplySchema && op == event.APPLY) {
			reconcilerID := "unknown"
			if options != nil && options.ReconcilerID != "" {
				reconcilerID = options.ReconcilerID
			}
			setupErr := &missingSchemaError{op: op, reconciler: ReconcilerID(reconcilerID), gvk: gvk, key: client.ObjectKeyFromObject(obj)}
			if options != nil {
				setupErr.subresource = options.Subresource
				if options.ReconcilerID != "" {
					setupErr.reconciler = ReconcilerID(options.ReconcilerID)
				}
			}
			m.rememberFrameError(ctx, setupErr)
			return false, false, setupErr
		}
		return false, false, nil
	}

	// Non-apply patches use the legacy path for unregistered resources. Registered
	// resources can materialize merge patches against the frame-live object.
	if op == event.PATCH && (options == nil || options.PatchType != types.MergePatchType) {
		return false, false, nil
	}

	intent, err := util.ConvertToUnstructured(obj)
	if err != nil {
		return false, false, err
	}
	intent.SetGroupVersionKind(gvk)
	frameID := replay.FrameIDFromContext(ctx)
	primary := canonicalResourceKeyString(gvk.Group, gvk.Kind, intent.GetNamespace(), intent.GetName())
	live := m.Resolve(m.effectVersions[frameID][primary])
	if live != nil {
		if rv := m.effectRVs[frameID][primary]; rv > 0 {
			live.SetResourceVersion(strconv.FormatInt(rv, 10))
		}
	}
	if op == event.PATCH {
		if live == nil {
			return false, false, apierrors.NewNotFound(
				schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}, intent.GetName())
		}
		patchJSON := options.PatchData
		if len(patchJSON) == 0 {
			patchJSON, err = json.Marshal(intent.Object)
			if err != nil {
				return false, false, fmt.Errorf("encoding merge patch: %w", err)
			}
		}
		patchObject := map[string]interface{}{}
		if err := kubejson.Unmarshal(patchJSON, &patchObject); err != nil {
			return false, false, apierrors.NewBadRequest(fmt.Sprintf("decoding merge patch: %v", err))
		}
		patchIntent := &unstructured.Unstructured{Object: patchObject}
		if claimed := patchIntent.GetResourceVersion(); claimed != "" {
			if err := checkWriteResourceVersion(gvk, intent.GetName(), claimed, m.effectRVs[frameID][primary]); err != nil {
				return false, false, err
			}
		}
		liveJSON, err := json.Marshal(live.Object)
		if err != nil {
			return false, false, fmt.Errorf("encoding live object for merge patch: %w", err)
		}
		mergedJSON, err := jsonpatch.MergePatch(liveJSON, patchJSON)
		if err != nil {
			return false, false, fmt.Errorf("applying merge patch: %w", err)
		}
		merged := &unstructured.Unstructured{}
		if err := kubejson.Unmarshal(mergedJSON, &merged.Object); err != nil {
			return false, false, apierrors.NewBadRequest(fmt.Sprintf("decoding merged object: %v", err))
		}
		if merged.GroupVersionKind() != gvk {
			return false, false, apierrors.NewBadRequest("merge patch type or version does not match the target object")
		}
		intent = merged
	}

	subresource := ""
	if options != nil {
		subresource = options.Subresource
	}
	manager := ""
	force := false
	if precondition != nil {
		manager = precondition.FieldManager
		force = precondition.Force
	}

	var result *unstructured.Unstructured
	switch op {
	case event.CREATE:
		if live != nil {
			return false, false, apierrors.NewAlreadyExists(
				schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}, intent.GetName())
		}
		result, err = resourceSchema.Create(intent, manager)
	case event.UPDATE:
		// Materialization replaces the request's resourceVersion with the live one.
		if live != nil {
			if err := m.validateEffect(ctx, op, intent, precondition, options); err != nil {
				return false, false, err
			}
		}
		result, err = resourceSchema.Update(live, intent, manager, subresource)
	case event.PATCH:
		result, err = resourceSchema.Update(live, intent, manager, subresource)
	case event.APPLY:
		if live != nil && intent.GetResourceVersion() != "" {
			claimedRV, parseErr := strconv.ParseInt(intent.GetResourceVersion(), 10, 64)
			currentRV := m.effectRVs[frameID][primary]
			if parseErr != nil || claimedRV <= 0 {
				return false, false, invalidResourceVersion(gvk, intent.GetName(), intent.GetResourceVersion(), "must be a positive integer")
			}
			if currentRV > 0 && claimedRV != currentRV {
				return false, false, apierrors.NewConflict(
					schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}, intent.GetName(),
					fmt.Errorf("the object has been modified; please apply your changes to the latest version"),
				)
			}
		}
		if subresource == "status" && live == nil {
			return false, false, apierrors.NewNotFound(
				schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}, intent.GetName())
		}
		result, err = resourceSchema.Apply(live, intent, manager, force, subresource)
	default:
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}

	rKeyExisted := m.effectRKeys[frameID].Contains(primary)
	if op != event.UPDATE {
		if err := m.validateEffect(ctx, op, result, precondition, options); err != nil {
			return false, false, err
		}
	}
	if precondition != nil && precondition.DryRun {
		if !rKeyExisted {
			m.effectRKeys[frameID].Delete(primary)
			m.effectIKeys[frameID].Delete(snapshot.IdentityKey{
				Group: gvk.Group, Kind: gvk.Kind, ObjectID: tag.GetSleeveObjectID(result),
			})
		}
		if err := copyUnstructuredInto(obj, result, m.scheme); err != nil {
			return false, false, fmt.Errorf("copying schema-backed dry-run response into %T: %w", obj, err)
		}
		return true, false, nil
	}

	changed = live == nil || !sameUnstructuredJSON(live, result)
	if err := copyUnstructuredInto(obj, result, m.scheme); err != nil {
		return false, false, fmt.Errorf("copying schema-backed response into %T: %w", obj, err)
	}
	return true, changed, nil
}

func checkWriteResourceVersion(gvk schema.GroupVersionKind, name, claimed string, current int64) error {
	if claimed == "" {
		return nil
	}
	parsed, err := strconv.ParseInt(claimed, 10, 64)
	if err != nil || parsed <= 0 {
		return invalidResourceVersion(gvk, name, claimed, "must be a positive integer")
	}
	if current > 0 && parsed != current {
		return apierrors.NewConflict(
			schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}, name,
			fmt.Errorf("the object has been modified; please apply your changes to the latest version"),
		)
	}
	return nil
}

func sameUnstructuredJSON(left, right *unstructured.Unstructured) bool {
	leftJSON, leftErr := json.Marshal(left.Object)
	rightJSON, rightErr := json.Marshal(right.Object)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}

func copyUnstructuredInto(obj client.Object, from *unstructured.Unstructured, scheme *runtime.Scheme) error {
	if unstr, ok := obj.(runtime.Unstructured); ok {
		unstr.SetUnstructuredContent(from.DeepCopy().Object)
		return nil
	}
	if scheme != nil {
		if err := scheme.Convert(from, obj, nil); err == nil {
			return nil
		}
	}
	return runtime.DefaultUnstructuredConverter.FromUnstructured(from.Object, obj)
}

func schemaResponseToUnstructured(obj client.Object, gvk schema.GroupVersionKind) (*unstructured.Unstructured, error) {
	if existing, ok := obj.(*unstructured.Unstructured); ok {
		return existing.DeepCopy(), nil
	}
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return nil, err
	}
	result := &unstructured.Unstructured{Object: raw}
	result.SetGroupVersionKind(gvk)
	return result, nil
}
