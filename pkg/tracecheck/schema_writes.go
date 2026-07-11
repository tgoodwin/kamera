package tracecheck

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/replay"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/util"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// materializeSchemaWrite applies API-server semantics before the effect is
// published. The returned bool distinguishes exact, schema-backed effects from
// legacy effects that apply their approximation later in Explorer.applyEffects.
func (m *manager) materializeSchemaWrite(
	ctx context.Context,
	obj client.Object,
	op event.OperationType,
	precondition *replay.PreconditionInfo,
	options *replay.EffectOptions,
) (client.Object, bool, bool, string, error) {
	if obj == nil {
		return obj, false, false, "", apierrors.NewBadRequest("write object is nil")
	}
	gvk := ensureObjectGVK(obj, m.scheme)
	resourceSchema, registered := m.schemaRegistry.Lookup(gvk)
	if !registered {
		if m.requireSchemas {
			reconcilerID := "unknown"
			if options != nil && options.ReconcilerID != "" {
				reconcilerID = options.ReconcilerID
			}
			return obj, false, false, "", fmt.Errorf(
				"schema-backed %s by reconciler %q requires a registered schema for %s; register it with WithCRD, WithResourceSchema, or WithOpenAPIV3",
				op, reconcilerID, gvk,
			)
		}
		return obj, false, false, "", nil
	}

	// Non-apply patch payload materialization remains on the legacy path. SSA
	// patches arrive as APPLY and are handled below.
	if op == event.PATCH {
		return obj, false, false, "", nil
	}

	intent, err := util.ConvertToUnstructured(obj)
	if err != nil {
		return obj, false, false, "", err
	}
	intent.SetGroupVersionKind(gvk)
	frameID := replay.FrameIDFromContext(ctx)
	primary := canonicalResourceKeyString(gvk.Group, gvk.Kind, intent.GetNamespace(), intent.GetName())
	live := m.effectObjects[frameID][primary]
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
			return obj, false, false, primary, apierrors.NewAlreadyExists(
				schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}, intent.GetName())
		}
		result, err = resourceSchema.Create(intent, manager)
	case event.UPDATE:
		result, err = resourceSchema.Update(live, intent, manager, subresource)
	case event.APPLY:
		if subresource == "status" && live == nil {
			return obj, false, false, primary, apierrors.NewNotFound(
				schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}, intent.GetName())
		}
		result, err = resourceSchema.Apply(live, intent, manager, force, subresource)
	default:
		return obj, false, false, "", nil
	}
	if err != nil {
		return obj, false, false, primary, err
	}

	rKeyExisted := m.effectRKeys[frameID].Contains(primary)
	if err := m.validateEffect(ctx, op, result, precondition, options); err != nil {
		return obj, false, false, primary, err
	}
	if precondition != nil && precondition.DryRun {
		if !rKeyExisted {
			m.effectRKeys[frameID].Delete(primary)
		}
		if err := copyUnstructuredInto(obj, result, m.scheme); err != nil {
			return obj, false, false, primary, fmt.Errorf("copying schema-backed dry-run response into %T: %w", obj, err)
		}
		return obj, true, false, primary, nil
	}

	changed := live == nil || !sameUnstructuredJSON(live, result)
	if changed {
		m.effectNextRV[frameID]++
		result.SetResourceVersion(strconv.FormatInt(m.effectNextRV[frameID], 10))
		m.effectRVs[frameID][primary] = m.effectNextRV[frameID]
	} else if live != nil {
		result.SetResourceVersion(live.GetResourceVersion())
	}
	m.effectObjects[frameID][primary] = result.DeepCopy()
	if live == nil {
		m.effectIKeys[frameID].Add(snapshot.IdentityKey{
			Group: gvk.Group, Kind: gvk.Kind, ObjectID: tag.GetSleeveObjectID(result),
		})
	}

	if err := copyUnstructuredInto(obj, result, m.scheme); err != nil {
		return obj, false, false, primary, fmt.Errorf("copying schema-backed response into %T: %w", obj, err)
	}
	return obj, true, changed, primary, nil
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
