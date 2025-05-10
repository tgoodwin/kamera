package tracecheck

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

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
	eventsByReconcileID      map[string][]StateEvent
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
		eventsByReconcileID:      byReconcile,
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
			diff, err := lm.JSONDelta("{}", e.Effect.Version.Value)
			if err != nil {
				fmt.Printf("error computing JSON delta: %v\n", err)
				continue
			}
			fmt.Println("Delta:", diff.String())
			fmt.Println("==========================")
			continue
		}
		prevEvent := relevantEvents[i-1]
		diff, err := lm.JSONDelta(prevEvent.Effect.Version.Value, e.Effect.Version.Value)
		if err != nil {
			fmt.Printf("error computing JSON delta: %v\n", err)
			continue
		}
		fmt.Println("Delta:", diff.String())
		fmt.Println("==========================")
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

// type ProvenanceNode struct {
// 	Key            snapshot.CompositeKey
// 	Event          StateEvent
// 	causalChildren []*ProvenanceNode
// }

// func (lm *LensManager) ProvenanceLens(slug string) error {
// 	ckey, err := lm.ckeyFromSlug(slug)
// 	if err != nil {
// 		return errors.Wrap(err, "getting ckey from slug")
// 	}
// 	// find all trace events where this object was modified
// 	relevantEvents := lo.Filter(lm.state.stateEvents, func(e StateEvent, _ int) bool {
// 		return e.Effect.Key.IdentityKey == ckey.IdentityKey && event.IsWriteOp(event.OperationType(e.OpType))
// 	})
// 	if len(relevantEvents) == 0 {
// 		return errors.Errorf("no events found for object with slug %s", slug)
// 	}
// 	sort.Slice(relevantEvents, func(i, j int) bool {
// 		return relevantEvents[i].Timestamp < relevantEvents[j].Timestamp
// 	})
// 	// this is the event where the resource was created (first modification)
// 	first := relevantEvents[0]
// 	labels := tag.FilterSleeveLabels(first.Labels)
// 	fmt.Println("labels:", labels)

// 	// start building the causality tree downwards
// 	provenanceNode := &ProvenanceNode{
// 		Key:   first.Effect.Key,
// 		Event: first,
// 	}

// 	tree := lm.buildCausalTree(provenanceNode)

// 	return nil
// }

// func (lm *LensManager) getCausalChildren(writeEvent StateEvent) []StateEvent {
// 	changeID := writeEvent.Labels[tag.ChangeID]
// 	children := make([]StateEvent, 0)
// 	for reconcileID, events := range lm.eventsByReconcileID {
// 		for _, e := range events {
// 			if event.IsReadOp(event.OperationType(e.OpType)) {
// 				labels := tag.FilterSleeveLabels(e.Labels)
// 				observedChangeID := labels[tag.ChangeID]
// 				// we've identified a downstream reconcile where this change ID was observed
// 				if observedChangeID == changeID {
// 					fmt.Printf("reconcile ID: %s, observed change ID: %s\n", reconcileID, observedChangeID)
// 					// this means that all the write events under this reconcileID were causally affected by
// 					// the change ID
// 					// we need to find all the write events under this reconcile ID and add them to the children list
// 					eventsUnderReconcile := lm.eventsByReconcileID[reconcileID]
// 					for _, e := range eventsUnderReconcile {
// 						if event.IsWriteOp(event.OperationType(e.OpType)) {
// 							children = append(children, e)
// 						}
// 					}
// 				}
// 			}
// 		}
// 	}
// 	return children
// }

// func (lm *LensManager) buildCausalTree(node *ProvenanceNode) *ProvenanceNode {
// 	children := lm.getCausalChildren(node.Event)
// 	for _, child := range children {
// 		childNode := &ProvenanceNode{
// 			Key:   child.Effect.Key,
// 			Event: child,
// 		}
// 		childTree := lm.buildCausalTree(childNode)
// 		node.causalChildren = append(node.causalChildren, childTree)
// 	}
// 	return node
// }

// ProvenanceNode represents a node in the causality tree.
// Each node corresponds to a specific write StateEvent.
type ProvenanceNode struct {
	Key            snapshot.CompositeKey // Identifies the object affected by the event
	Event          StateEvent            // The actual write event
	CausalChildren []ProvenanceNode      // Downstream write events causally linked to this event
}

// ProvenanceLens analyzes and prints the downward causal provenance from an initial write event.
func (lm *LensManager) ProvenanceLens(slug string) error {
	if lm.state == nil {
		return errors.New("LensManager state is nil")
	}
	if lm.eventsByReconcileID == nil {
		return errors.New("LensManager eventsByReconcileID is nil")
	}

	ckey, err := lm.ckeyFromSlug(slug)
	if err != nil {
		return errors.Wrapf(err, "getting ckey from slug '%s'", slug)
	}

	relevantEvents := lo.Filter(lm.state.stateEvents, func(e StateEvent, _ int) bool {
		return e.Effect.Key.IdentityKey == ckey.IdentityKey && event.IsWriteOp(event.OperationType(e.OpType))
	})

	if len(relevantEvents) == 0 {
		return errors.Errorf("no write events found for object with slug '%s' (IdentityKey: %s)", slug, ckey.IdentityKey)
	}

	sort.Slice(relevantEvents, func(i, j int) bool {
		return relevantEvents[i].Timestamp < relevantEvents[j].Timestamp
	})

	firstWriteEvent := relevantEvents[0]
	initialChangeID := ""
	if firstWriteEvent.Labels != nil { // Safe access to Labels
		initialChangeID = firstWriteEvent.Labels[tag.ChangeID]
	}

	fmt.Printf("Starting Provenance Analysis for object: %s\n", firstWriteEvent.Effect.Key.String())
	fmt.Printf("Initial Write Event ID: %s, ChangeID: %s, ReconcileID: %s, Controller: %s, Timestamp: %d\n",
		firstWriteEvent.ID, initialChangeID, firstWriteEvent.ReconcileID, firstWriteEvent.ControllerID, firstWriteEvent.Timestamp)

	if initialChangeID == "" {
		return errors.Errorf("initial write event (ID: %s) for object %s has no ChangeID in its labels. Cannot trace provenance.", firstWriteEvent.ID, slug)
	}

	rootNode := ProvenanceNode{
		Key:   firstWriteEvent.Effect.Key,
		Event: firstWriteEvent,
	}

	expandedEventIDs := make(map[string]bool) // Changed from expandedEventUIDs
	lm.buildCausalityRecursively(&rootNode, expandedEventIDs)

	fmt.Println("\nDownward Causality Tree:")
	lm.printProvenanceTree(&rootNode, 0, "", true)

	return nil
}

// buildCausalityRecursively populates the CausalChildren of the given parentNode.
func (lm *LensManager) buildCausalityRecursively(parentNode *ProvenanceNode, expandedEventIDsInPath map[string]bool) { // Changed parameter name
	parentEventID := parentNode.Event.ID // Changed from UID
	parentChangeID := ""
	if parentNode.Event.Labels != nil {
		parentChangeID = parentNode.Event.Labels[tag.ChangeID]
	}

	if expandedEventIDsInPath[parentEventID] { // Changed from parentEventUID
		fmt.Printf("DEBUG: Cycle detected for Event ID %s (ChangeID: %s)\n", parentEventID, parentChangeID) // Changed from UID
		return
	}

	currentPathExpandedIDs := make(map[string]bool, len(expandedEventIDsInPath)+1) // Changed variable name
	for id, val := range expandedEventIDsInPath {                                  // Changed loop var
		currentPathExpandedIDs[id] = val
	}
	currentPathExpandedIDs[parentEventID] = true // Changed from parentEventUID

	if parentChangeID == "" {
		return
	}

	directChildrenEvents := make(map[string]StateEvent) // Key: Event ID
	processedTriggersForThisParent := make(map[string]bool)

	for triggeringReconcileID, eventsInReconcile := range lm.eventsByReconcileID {
		triggerKey := parentChangeID + "_" + triggeringReconcileID
		if processedTriggersForThisParent[triggerKey] {
			continue
		}

		parentChangeWasRead := false
		for _, readEvent := range eventsInReconcile {
			if event.IsReadOp(event.OperationType(readEvent.OpType)) && readEvent.Labels != nil {
				if observedChangeID := readEvent.Labels[tag.ChangeID]; observedChangeID == parentChangeID {
					parentChangeWasRead = true
					break
				}
			}
		}

		if parentChangeWasRead {
			processedTriggersForThisParent[triggerKey] = true
			for _, childCandidateEvent := range lm.eventsByReconcileID[triggeringReconcileID] {
				if event.IsWriteOp(event.OperationType(childCandidateEvent.OpType)) {
					if childCandidateEvent.ID == parentEventID { // Changed from UID
						continue
					}
					directChildrenEvents[childCandidateEvent.ID] = childCandidateEvent // Changed from UID
				}
			}
		}
	}

	sortedChildStateEvents := make([]StateEvent, 0, len(directChildrenEvents))
	for _, event := range directChildrenEvents {
		sortedChildStateEvents = append(sortedChildStateEvents, event)
	}
	sort.Slice(sortedChildStateEvents, func(i, j int) bool {
		return sortedChildStateEvents[i].Timestamp < sortedChildStateEvents[j].Timestamp
	})

	parentNode.CausalChildren = make([]ProvenanceNode, 0, len(sortedChildStateEvents))
	for _, childEvent := range sortedChildStateEvents {
		childNode := ProvenanceNode{
			Key:   childEvent.Effect.Key,
			Event: childEvent,
		}
		lm.buildCausalityRecursively(&childNode, currentPathExpandedIDs) // Pass updated map
		parentNode.CausalChildren = append(parentNode.CausalChildren, childNode)
	}
}

// printProvenanceTree prints the causality tree with reconcile instance grouping.
func (lm *LensManager) printProvenanceTree(eventNode *ProvenanceNode, depth int, indentPrefix string, isLastInPreviousLevel bool) {
	currentLineOutput := indentPrefix
	if depth > 0 {
		if isLastInPreviousLevel {
			currentLineOutput += "└─ "
		} else {
			currentLineOutput += "├─ "
		}
	} else {
		currentLineOutput += "ROOT: "
	}

	// eventChangeID := "N/A"
	// if eventNode.Event.Labels != nil && eventNode.Event.Labels[tag.ChangeID] != "" {
	// eventChangeID = eventNode.Event.Labels[tag.ChangeID]
	// } else if eventNode.Event.Labels != nil {
	// eventChangeID = "EMPTY"
	// }

	// performingControllerID := eventNode.Event.ControllerID
	// if performingControllerID == "" {
	// performingControllerID = "UnknownController"
	// }

	timestamp := eventNode.Event.Timestamp
	timestampInt, err := strconv.ParseInt(timestamp, 10, 64) // Convert string to int64
	if err != nil {
		fmt.Printf("Error parsing timestamp: %v\n", err)
		timestampInt = 0 // Fallback to 0 if parsing fails
	}
	parsedTime := time.Unix(0, timestampInt*int64(time.Microsecond)) // Convert to time.Time
	formattedTime := parsedTime.Format(time.RFC3339)                 // Format as ISO 8601

	fmt.Printf("%s%s %s %q (Time: %s)\n", // Changed UID to ID
		currentLineOutput,
		eventNode.Event.OpType,
		eventNode.Event.Effect.Key.ResourceKey.Kind,
		eventNode.Event.Effect.Key.Name,
		formattedTime,
	)

	childrenBaseIndent := indentPrefix
	if depth > 0 {
		if isLastInPreviousLevel {
			childrenBaseIndent += "  "
		} else {
			childrenBaseIndent += "│ "
		}
	}

	type ReconcileInstanceKey struct {
		ControllerID string
		ReconcileID  string
	}
	type ReconcileInstanceGroup struct {
		Key                ReconcileInstanceKey
		Events             []ProvenanceNode
		MinTimestamp       string
		ActualControllerID string
	}

	groupedChildren := make(map[ReconcileInstanceKey]*ReconcileInstanceGroup)
	// Corrected loop variable name from childProvNode to childNode for clarity
	for _, childNode := range eventNode.CausalChildren { // Iterate over children of current eventNode
		groupControllerID := childNode.Event.ControllerID
		if groupControllerID == "" {
			groupControllerID = "UnknownController"
		}
		key := ReconcileInstanceKey{
			ControllerID: groupControllerID,
			ReconcileID:  childNode.Event.ReconcileID,
		}

		group, exists := groupedChildren[key]
		if !exists {
			group = &ReconcileInstanceGroup{
				Key:                key,
				Events:             make([]ProvenanceNode, 0),
				MinTimestamp:       childNode.Event.Timestamp,
				ActualControllerID: groupControllerID,
			}
			groupedChildren[key] = group
		}
		group.Events = append(group.Events, childNode)
		if childNode.Event.Timestamp < group.MinTimestamp {
			group.MinTimestamp = childNode.Event.Timestamp
		}
	}

	sortedGroups := make([]*ReconcileInstanceGroup, 0, len(groupedChildren))
	for _, group := range groupedChildren {
		sort.Slice(group.Events, func(i, j int) bool {
			return group.Events[i].Event.Timestamp < group.Events[j].Event.Timestamp
		})
		sortedGroups = append(sortedGroups, group)
	}

	sort.Slice(sortedGroups, func(i, j int) bool {
		if sortedGroups[i].MinTimestamp == sortedGroups[j].MinTimestamp {
			if sortedGroups[i].Key.ControllerID == sortedGroups[j].Key.ControllerID {
				return sortedGroups[i].Key.ReconcileID < sortedGroups[j].Key.ReconcileID
			}
			return sortedGroups[i].Key.ControllerID < sortedGroups[j].Key.ControllerID
		}
		return sortedGroups[i].MinTimestamp < sortedGroups[j].MinTimestamp
	})

	for i, group := range sortedGroups {
		isLastGroup := (i == len(sortedGroups)-1)

		reconcileGroupLine := childrenBaseIndent
		if isLastGroup {
			reconcileGroupLine += "└─ "
		} else {
			reconcileGroupLine += "├─ "
		}

		fmt.Printf("%s%s reconcile (reconcile ID: %s)\n",
			reconcileGroupLine,
			group.ActualControllerID,
			group.Key.ReconcileID,
			// group.MinTimestamp
		)

		operationsIndent := childrenBaseIndent
		if isLastGroup {
			operationsIndent += "  "
		} else {
			operationsIndent += "│ "
		}

		for j, actualWriteEventNodeInGroup := range group.Events {
			isLastEventInThisGroup := (j == len(group.Events)-1)
			lm.printProvenanceTree(&actualWriteEventNodeInGroup, depth+1, operationsIndent, isLastEventInThisGroup)
		}
	}
}
