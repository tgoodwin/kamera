package tracecheck

import (
	"encoding/json"
	"fmt"
	"strings"

	"sort"

	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"github.com/tgoodwin/sleeve/pkg/tag"
	"github.com/wI2L/jsondiff"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type LensManager struct {
	state                    *StateSnapshot
	manager                  *manager
	dataEffectsByReconcileID map[string]reconcileEffects
	effects                  []Effect
}

func NewLensManager(state *StateSnapshot, mgr *manager) *LensManager {
	dataEffectByReconcileID := make(map[string]reconcileEffects)
	byReconcile := lo.GroupBy(state.stateEvents, func(e StateEvent) string {
		return e.ReconcileID
	})

	effects := make([]Effect, 0)
	for reconcileID, events := range byReconcile {
		// reads, writes := event.FilterReadsWrites(events)
		reconcileEffects := reconcileEffects{
			reads:  make([]Effect, 0),
			writes: make([]Effect, 0),
		}
		for _, e := range events {
			if event.IsWriteOp(event.OperationType(e.OpType)) {
				reconcileEffects.writes = append(reconcileEffects.writes, e.Effect)
			}
			if event.IsReadOp(event.OperationType(e.OpType)) {
				reconcileEffects.reads = append(reconcileEffects.reads, e.Effect)
			}

			effects = append(effects, e.Effect)
		}
		dataEffectByReconcileID[reconcileID] = reconcileEffects
	}

	return &LensManager{
		state:                    state,
		manager:                  mgr,
		dataEffectsByReconcileID: dataEffectByReconcileID,
		effects:                  effects,
	}
}

func (lm *LensManager) getLatestVersion(key snapshot.CompositeKey) (*unstructured.Unstructured, error) {
	relevantEvents := lo.Filter(lm.state.stateEvents, func(e StateEvent, _ int) bool {
		return e.Effect.Key.IdentityKey == key.IdentityKey && event.IsWriteOp(event.OperationType(e.OpType))
	})
	sort.Slice(relevantEvents, func(i, j int) bool {
		return relevantEvents[i].Timestamp < relevantEvents[j].Timestamp
	})
	for _, e := range relevantEvents {
		fmt.Printf("controllerID: %s, ReconcileID: %s, opType: %s\n", e.ControllerID, e.ReconcileID, e.OpType)
	}
	for _, e := range lo.Reverse(relevantEvents) {
		if e.Effect.Key == key {
			obj := lm.manager.versionStore.Resolve(e.Effect.Version)
			if obj == nil {
				return nil, errors.New("could not resolve object")
			}
			return obj, nil

		}
	}
	return nil, errors.Errorf("could not find object with key %s", key)
}

type NoPrevReconcile struct {
	Key         snapshot.ResourceKey
	ReconcileID string
}

func (NoPrevReconcile) Error() string {
	return "key not in read set"
}

func (lm *LensManager) getPrevReconileWrite(currReconcileID string, key snapshot.CompositeKey) (string, error) {
	fmt.Println("getPrevReconileWrite for", currReconcileID, key)
	effectsForReconcile, ok := lm.dataEffectsByReconcileID[currReconcileID]
	if !ok {
		return "", errors.Errorf("no data effects for reconcile ID %s", currReconcileID)
	}
	reads := effectsForReconcile.reads
	for _, read := range reads {
		if read.Key.IdentityKey == key.IdentityKey {
			readVersion := read.Version
			readObj := lm.manager.versionStore.Resolve(readVersion)
			if readObj == nil {
				return "", errors.New("could not resolve object")
			}
			labels := readObj.GetLabels()
			if labels == nil {
				return "", errors.New("object has no labels")
			}
			lastReconcile, ok := labels[tag.TraceyReconcileID]
			_, ok2 := labels[tag.TraceyWebhookLabel]
			if !ok {
				if ok2 {
					return "", NoPrevReconcile{
						Key:         key.ResourceKey,
						ReconcileID: currReconcileID,
					}
				}
				return "", errors.New("object has no last reconcile ID label")
			}
			return lastReconcile, nil
		}
	}
	return "", NoPrevReconcile{
		Key:         key.ResourceKey,
		ReconcileID: currReconcileID,
	}
}

func (lm *LensManager) JSONDelta(jsonStr1, jsonStr2 string) (jsondiff.Patch, error) {
	if jsonStr1 == "" || jsonStr2 == "" {
		return nil, errors.New("one or both JSON strings are empty")
	}

	// Parse JSON strings into map[string]interface{}
	var map1, map2 map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr1), &map1); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal first JSON string")
	}
	if err := json.Unmarshal([]byte(jsonStr2), &map2); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal second JSON string")
	}

	// Use the jsondiff library to compute the delta
	diff, err := jsondiff.Compare(map1, map2)
	return diff, errors.Wrap(err, "failed to compute JSON diff")
}

func (lm *LensManager) LifecycleLens(slug string) error {
	ckey, err := lm.ckeyFromSlug(slug)
	if err != nil {
		return errors.Wrap(err, "getting ckey from slug")
	}

	relevantEvents := lo.Filter(lm.state.stateEvents, func(e StateEvent, _ int) bool {
		return e.Effect.Key.IdentityKey == ckey.IdentityKey && event.IsWriteOp(event.OperationType(e.OpType))
	})
	sort.Slice(relevantEvents, func(i, j int) bool {
		return relevantEvents[i].Timestamp < relevantEvents[j].Timestamp
	})
	for i, e := range relevantEvents {
		fmt.Printf("controllerID: %s, ReconcileID: %s, opType: %s\n", e.ControllerID, e.ReconcileID, e.OpType)
		if i == 0 {
			fmt.Println("Delta: CREATE")
			continue
		}
		prevEvent := relevantEvents[i-1]
		diff, err := lm.JSONDelta(prevEvent.Effect.Version.Value, e.Effect.Version.Value)
		if err != nil {
			fmt.Printf("error computing JSON delta: %v\n", err)
			continue
		}
		fmt.Println("Delta:", diff.String())
	}
	return nil

	// obj, err := lm.getLatestVersion(ckey)
	// if err != nil {
	// 	return errors.Wrap(err, "getting object")
	// }
	// labels := obj.GetLabels()
	// if labels == nil {
	// 	return errors.New("object has no labels")
	// }
	// lastReconcile, ok := labels[tag.TraceyReconcileID]
	// if !ok {
	// 	return errors.New("object has no last reconcile ID label")
	// }
	// reconcileLineage := []string{lastReconcile}
	// var prevReconcile string
	// for {
	// 	prevReconcile, err = lm.getPrevReconileWrite(lastReconcile, ckey)
	// 	if err != nil {
	// 		if _, ok := err.(NoPrevReconcile); ok {
	// 			break
	// 		}
	// 		return err
	// 	}
	// 	reconcileLineage = append(reconcileLineage, prevReconcile)
	// 	lastReconcile = prevReconcile
	// }
	// fmt.Println("reconcile lineage")
	// for _, reconcileID := range lo.Reverse(reconcileLineage) {
	// 	fmt.Println(reconcileID)
	// }
	// return nil
}

func (lm *LensManager) ckeyFromSlug(slug string) (snapshot.CompositeKey, error) {
	for _, effect := range lm.effects {
		ckey := effect.Key
		if strings.HasPrefix(ckey.IdentityKey.ObjectID, slug) {
			return ckey, nil
		}
	}
	return snapshot.CompositeKey{}, errors.Errorf("could not find object with slug %s", slug)
}
