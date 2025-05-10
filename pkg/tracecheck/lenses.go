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

const (
	rootTypeTrueWebhookWrite          = "true_webhook_write"
	rootTypeWebhookInitiatedReconcile = "webhook_initiated_reconcile"
	rootTypeNoLinkFound               = "no_link_found"
	rootTypeCycleDetected             = "cycle_detected"
)

// formatTimestamp is a helper function to parse and format timestamp strings.
func formatTimestamp(tsStr string) string {
	timestampInt, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return fmt.Sprintf("InvalidTimestamp(%s)", tsStr)
	}
	// Assuming timestamp is in microseconds
	parsedTime := time.Unix(0, timestampInt*int64(time.Microsecond))
	return parsedTime.Format(time.RFC3339Nano) // Use Nano for more precision
}

// ProvenanceLens analyzes and prints causal provenance.
func (lm *LensManager) ProvenanceLens(slug string) error {
	if lm.state == nil {
		return errors.New("LensManager state is nil")
	}
	if lm.eventsByReconcileID == nil {
		return errors.New("LensManager eventsByReconcileID is nil")
	}
	if lm.state.stateEvents == nil {
		return errors.New("LensManager state.stateEvents is nil")
	}

	ckey, err := lm.ckeyFromSlug(slug)
	if err != nil {
		return errors.Wrapf(err, "getting ckey from slug '%s'", slug)
	}

	relevantEvents := lo.Filter(lm.state.stateEvents, func(e StateEvent, _ int) bool {
		return e.Effect.Key.IdentityKey == ckey.IdentityKey &&
			event.IsWriteOp(event.OperationType(e.OpType))
	})

	if len(relevantEvents) == 0 {
		return errors.Errorf("no write events found for object with slug '%s' (IdentityKey: %s)", slug, ckey.IdentityKey)
	}

	sort.Slice(relevantEvents, func(i, j int) bool {
		tsI, _ := strconv.ParseInt(relevantEvents[i].Timestamp, 10, 64)
		tsJ, _ := strconv.ParseInt(relevantEvents[j].Timestamp, 10, 64)
		return tsI < tsJ
	})

	firstWriteEvent := relevantEvents[0]

	fmt.Printf("Starting Provenance Analysis from event ID: %s for object: %s\n", firstWriteEvent.ID, firstWriteEvent.Effect.Key.String())

	visitedUpwards := make(map[string]bool)
	rootLevelWriteEvents, _, rootWebhookIDValue, err := lm.findCausalRootAndSiblings(firstWriteEvent, visitedUpwards)
	if err != nil {
		fmt.Printf("Warning during upward trace: %v\n", err)
		if len(rootLevelWriteEvents) == 0 {
			return errors.Wrapf(err, "failed to find causal root starting from event ID %s and no events returned", firstWriteEvent.ID)
		}
	}

	if len(rootLevelWriteEvents) == 0 {
		return errors.Errorf("no root level events identified from event ID %s", firstWriteEvent.ID)
	}

	sort.Slice(rootLevelWriteEvents, func(i, j int) bool {
		tsI, _ := strconv.ParseInt(rootLevelWriteEvents[i].Timestamp, 10, 64)
		tsJ, _ := strconv.ParseInt(rootLevelWriteEvents[j].Timestamp, 10, 64)
		return tsI < tsJ
	})

	fmt.Println("\nProvenance:")

	if len(rootLevelWriteEvents) > 0 {
		firstRootEvt := rootLevelWriteEvents[0]
		controllerForHeader := firstRootEvt.ControllerID
		if controllerForHeader == "" { // Default if missing
			controllerForHeader = "UnknownController"
		}
		reconcileIDForHeader := firstRootEvt.ReconcileID
		timestampForHeader := firstRootEvt.Timestamp

		topEvent, err := lm.findTopLevelEvent(rootWebhookIDValue, "TraceyWebhook")
		if err != nil {
			fmt.Printf("Error finding top-level event: %v\n", err)
		}
		fmt.Printf("%s (ID: %s, timestamp: %s)\n",
			"External Request",
			// topEvent.ControllerID,
			rootWebhookIDValue,
			formatTimestamp(timestampForHeader),
		)
		prefix := "└─"
		fmt.Printf("%s %s %s %s\n", prefix, topEvent.OpType, topEvent.Kind, topEvent.Effect.Key.ResourceKey.Name)

		fmt.Printf("%s %s reconcile (reconcile ID%s, timestamp: %s)\n", "  "+prefix, controllerForHeader, reconcileIDForHeader, formatTimestamp(timestampForHeader))

		// Now print each rootLevelWriteEvent as an item under this header.
		// These are the actual write operations within that root reconcile instance.
		for i, topEvent := range rootLevelWriteEvents {
			rootNode := ProvenanceNode{Key: topEvent.Effect.Key, Event: topEvent}
			expandedDownwards := make(map[string]bool)
			lm.buildCausalityRecursively(&rootNode, expandedDownwards)
			// These events are direct children of the conceptual root reconcile header.
			// Pass depth 1, empty indentPrefix, and isLast status.
			lm.printProvenanceTree(&rootNode, 1, "     ", (i == len(rootLevelWriteEvents)-1))
		}
	} else {
		fmt.Println("No root events to display.")
	}

	return nil
}

// findCausalRootAndSiblings traces upwards from currentWriteEvent.
func (lm *LensManager) findCausalRootAndSiblings(
	currentWriteEvent StateEvent,
	visitedUpwards map[string]bool,
) ([]StateEvent, string, string, error) {

	if visitedUpwards[currentWriteEvent.ID] {
		siblings := lm.getSiblingWriteEvents(currentWriteEvent)
		allEventsInCycleReconcile := append([]StateEvent{currentWriteEvent}, siblings...)
		return allEventsInCycleReconcile, rootTypeCycleDetected, "", errors.Errorf("upward cycle detected at event ID %s", currentWriteEvent.ID)
	}
	visitedUpwards[currentWriteEvent.ID] = true
	nextVisitedUpwards := make(map[string]bool)
	for k, v := range visitedUpwards {
		nextVisitedUpwards[k] = v
	}

	if currentWriteEvent.Labels != nil {
		if webhookID, ok := currentWriteEvent.Labels[tag.TraceyWebhookLabel]; ok && webhookID != "" {
			siblings := lm.getSiblingWriteEvents(currentWriteEvent)
			allEvents := append([]StateEvent{currentWriteEvent}, siblings...)
			return allEvents, rootTypeTrueWebhookWrite, webhookID, nil
		}
	}

	eventsInCurrentReconcile := lm.eventsByReconcileID[currentWriteEvent.ReconcileID]
	readEventsInReconcile := []StateEvent{}
	for _, e := range eventsInCurrentReconcile {
		if event.IsReadOp(event.OperationType(e.OpType)) {
			readEventsInReconcile = append(readEventsInReconcile, e)
		}
	}

	if len(readEventsInReconcile) == 0 {
		siblings := lm.getSiblingWriteEvents(currentWriteEvent)
		allEvents := append([]StateEvent{currentWriteEvent}, siblings...)
		return allEvents, rootTypeNoLinkFound, "", nil
	}

	var potentialParentReads []StateEvent
	for _, r := range readEventsInReconcile {
		if r.Labels != nil {
			_, hasChangeID := r.Labels[tag.ChangeID]
			_, hasTraceyWebhook := r.Labels[tag.TraceyWebhookLabel]
			if (hasChangeID && r.Labels[tag.ChangeID] != "") || (hasTraceyWebhook && r.Labels[tag.TraceyWebhookLabel] != "") {
				potentialParentReads = append(potentialParentReads, r)
			}
		}
	}

	if len(potentialParentReads) == 0 {
		siblings := lm.getSiblingWriteEvents(currentWriteEvent)
		allEvents := append([]StateEvent{currentWriteEvent}, siblings...)
		return allEvents, rootTypeNoLinkFound, "", nil
	}

	sort.Slice(potentialParentReads, func(i, j int) bool {
		tsI, _ := strconv.ParseInt(potentialParentReads[i].Timestamp, 10, 64)
		tsJ, _ := strconv.ParseInt(potentialParentReads[j].Timestamp, 10, 64)
		if tsI == tsJ {
			return potentialParentReads[i].ID < potentialParentReads[j].ID
		}
		return tsI < tsJ
	})

	selectedParentReadEvent := potentialParentReads[0]
	if len(potentialParentReads) > 1 {
		tsSelected, _ := strconv.ParseInt(selectedParentReadEvent.Timestamp, 10, 64)
		tsNext, _ := strconv.ParseInt(potentialParentReads[1].Timestamp, 10, 64)
		if tsSelected == tsNext {
			fmt.Printf("WARNING: Tie in timestamps for selecting causal parent read event in ReconcileID %s. Selected %s.\n", currentWriteEvent.ReconcileID, selectedParentReadEvent.ID)
		}
	}

	parentReadChangeID := ""
	webhookIDFromRead := ""
	if selectedParentReadEvent.Labels != nil {
		parentReadChangeID = selectedParentReadEvent.Labels[tag.ChangeID]
		webhookIDFromRead = selectedParentReadEvent.Labels[tag.TraceyWebhookLabel]
	}

	if parentReadChangeID != "" {
		var actualUpstreamWriteEvent *StateEvent
		for i := range lm.state.stateEvents {
			candidateEvent := lm.state.stateEvents[i]
			if event.IsWriteOp(event.OperationType(candidateEvent.OpType)) &&
				candidateEvent.Labels != nil &&
				candidateEvent.Labels[tag.ChangeID] == parentReadChangeID {
				actualUpstreamWriteEvent = &lm.state.stateEvents[i]
				break
			}
		}
		if actualUpstreamWriteEvent == nil {
			fmt.Printf("WARNING: Upstream write event for ChangeID %s (read by %s) not found. Treating current reconcile as root.\n", parentReadChangeID, selectedParentReadEvent.ID)
			siblings := lm.getSiblingWriteEvents(currentWriteEvent)
			allEvents := append([]StateEvent{currentWriteEvent}, siblings...)
			if webhookIDFromRead != "" {
				return allEvents, rootTypeWebhookInitiatedReconcile, webhookIDFromRead, nil
			}
			return allEvents, rootTypeNoLinkFound, "", nil
		}
		return lm.findCausalRootAndSiblings(*actualUpstreamWriteEvent, nextVisitedUpwards)
	}

	if webhookIDFromRead != "" {
		allWritesInThisReconcile := []StateEvent{}
		for _, e := range eventsInCurrentReconcile { // Use eventsInCurrentReconcile (already fetched)
			if event.IsWriteOp(event.OperationType(e.OpType)) {
				allWritesInThisReconcile = append(allWritesInThisReconcile, e)
			}
		}
		return allWritesInThisReconcile, rootTypeWebhookInitiatedReconcile, webhookIDFromRead, nil
	}

	siblings := lm.getSiblingWriteEvents(currentWriteEvent)
	allEvents := append([]StateEvent{currentWriteEvent}, siblings...)
	return allEvents, rootTypeNoLinkFound, "", nil
}

func (lm *LensManager) getSiblingWriteEvents(evt StateEvent) []StateEvent {
	siblings := []StateEvent{}
	eventsInReconcile := lm.eventsByReconcileID[evt.ReconcileID]
	for _, e := range eventsInReconcile {
		if e.ID != evt.ID && event.IsWriteOp(event.OperationType(e.OpType)) {
			siblings = append(siblings, e)
		}
	}
	return siblings
}

func (lm *LensManager) buildCausalityRecursively(parentNode *ProvenanceNode, expandedEventIDsInPath map[string]bool) {
	parentEventID := parentNode.Event.ID
	parentChangeID := ""
	if parentNode.Event.Labels != nil {
		parentChangeID = parentNode.Event.Labels[tag.ChangeID]
	}

	if expandedEventIDsInPath[parentEventID] {
		fmt.Printf("DEBUG: Downward cycle detected for Event ID %s (ChangeID: %s)\n", parentEventID, parentChangeID)
		return
	}
	currentPathExpandedIDs := make(map[string]bool, len(expandedEventIDsInPath)+1)
	for id, val := range expandedEventIDsInPath {
		currentPathExpandedIDs[id] = val
	}
	currentPathExpandedIDs[parentEventID] = true

	if parentChangeID == "" {
		return
	}

	directChildrenEvents := make(map[string]StateEvent)
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
					if childCandidateEvent.ID == parentEventID {
						continue
					}
					directChildrenEvents[childCandidateEvent.ID] = childCandidateEvent
				}
			}
		}
	}

	sortedChildStateEvents := make([]StateEvent, 0, len(directChildrenEvents))
	for _, ev := range directChildrenEvents {
		sortedChildStateEvents = append(sortedChildStateEvents, ev)
	}
	sort.Slice(sortedChildStateEvents, func(i, j int) bool {
		tsI, _ := strconv.ParseInt(sortedChildStateEvents[i].Timestamp, 10, 64)
		tsJ, _ := strconv.ParseInt(sortedChildStateEvents[j].Timestamp, 10, 64)
		return tsI < tsJ
	})

	parentNode.CausalChildren = make([]ProvenanceNode, 0, len(sortedChildStateEvents))
	for _, childEvent := range sortedChildStateEvents {
		childNode := ProvenanceNode{Key: childEvent.Effect.Key, Event: childEvent}
		lm.buildCausalityRecursively(&childNode, currentPathExpandedIDs)
		parentNode.CausalChildren = append(parentNode.CausalChildren, childNode)
	}
}

func (lm *LensManager) printProvenanceTree(eventNode *ProvenanceNode, depth int, indentPrefix string, isLastInPreviousLevel bool) {
	currentLineOutput := indentPrefix
	if depth > 0 { // Apply tree branching characters for nodes not at the immediate root of this print call
		if isLastInPreviousLevel {
			currentLineOutput += "└─ "
		} else {
			currentLineOutput += "├─ "
		}
	} else {
		// Depth 0 for this function means it's the first event being printed in a sequence
		// (e.g., the items directly under the "Shared Root Reconcile" or a single root event).
		// The main "ROOT" headers are handled by ProvenanceLens.
		// For consistency with the user's example "│   └─ CREATE...",
		// items at depth 0 in this *specific* call (which are actually depth 1 from the overall root header)
		// still need a branch prefix if they are part of a list.
		// This is handled by the caller setting isLastInPreviousLevel correctly.
		// So, if depth is 0 here, it means it's a top-level event *for this particular tree rendering pass*.
		// The `isLastInPreviousLevel` will determine └─ or ├─.
		// The `indentPrefix` will be "" if it's truly the start.
		// Let's assume the caller (ProvenanceLens) correctly sets depth and isLast for the first level of events.
		// If this function is called with depth 0, it's a standalone event print.
		// No, the logic in ProvenanceLens calls this with depth 1 for items under the main header.
		// If called with depth 0, it's a single overall root.
		if isLastInPreviousLevel { // If it's the only/last item at this effective "root" level for this call
			currentLineOutput += "└─ "
		} else { // If there are more siblings at this effective "root" level for this call
			currentLineOutput += "├─ "
		}
		// This makes depth 0 behave like depth 1 for prefixing, which simplifies the caller.
		// The very first "ROOT" or "Shared Reconcile" header is printed outside.
	}

	kind := eventNode.Event.Effect.Key.ResourceKey.Kind
	name := eventNode.Event.Effect.Key.ResourceKey.Name

	timestampStr := eventNode.Event.Timestamp
	formattedTime := formatTimestamp(timestampStr) // Use helper

	// Format from user's canvas: OpType Kind "Name" (Time: formattedTime)
	fmt.Printf("%s%s %s %q (Time: %s)\n",
		currentLineOutput,
		eventNode.Event.OpType,
		kind,
		name,
		formattedTime,
	)

	childrenBaseIndent := indentPrefix
	// This logic correctly extends the indent string for the next level.
	if isLastInPreviousLevel { // If current node is last, its children don't need vertical bar from this level
		childrenBaseIndent += "  "
	} else { // If current node is not last, its children need vertical bar from this level
		childrenBaseIndent += "│ "
	}

	type ReconcileInstanceKey struct {
		ControllerID string
		ReconcileID  string
	}
	type ReconcileInstanceGroup struct {
		Key                ReconcileInstanceKey
		Events             []ProvenanceNode
		MinTimestampStr    string
		MinTimestampInt    int64
		ActualControllerID string
	}

	groupedChildren := make(map[ReconcileInstanceKey]*ReconcileInstanceGroup)
	for _, childNode := range eventNode.CausalChildren {
		groupControllerID := childNode.Event.ControllerID
		if groupControllerID == "" {
			groupControllerID = "UnknownController"
		}
		key := ReconcileInstanceKey{
			ControllerID: groupControllerID,
			ReconcileID:  childNode.Event.ReconcileID,
		}
		childTsInt, _ := strconv.ParseInt(childNode.Event.Timestamp, 10, 64)

		group, exists := groupedChildren[key]
		if !exists {
			group = &ReconcileInstanceGroup{
				Key:                key,
				Events:             make([]ProvenanceNode, 0),
				MinTimestampStr:    childNode.Event.Timestamp,
				MinTimestampInt:    childTsInt,
				ActualControllerID: groupControllerID,
			}
			groupedChildren[key] = group
		}
		group.Events = append(group.Events, childNode)
		if childTsInt < group.MinTimestampInt {
			group.MinTimestampStr = childNode.Event.Timestamp
			group.MinTimestampInt = childTsInt
		}
	}

	sortedGroups := make([]*ReconcileInstanceGroup, 0, len(groupedChildren))
	for _, group := range groupedChildren {
		sort.Slice(group.Events, func(i, j int) bool {
			tsI, _ := strconv.ParseInt(group.Events[i].Event.Timestamp, 10, 64)
			tsJ, _ := strconv.ParseInt(group.Events[j].Event.Timestamp, 10, 64)
			return tsI < tsJ
		})
		sortedGroups = append(sortedGroups, group)
	}

	sort.Slice(sortedGroups, func(i, j int) bool {
		if sortedGroups[i].MinTimestampInt == sortedGroups[j].MinTimestampInt {
			if sortedGroups[i].Key.ControllerID == sortedGroups[j].Key.ControllerID {
				return sortedGroups[i].Key.ReconcileID < sortedGroups[j].Key.ReconcileID
			}
			return sortedGroups[i].Key.ControllerID < sortedGroups[j].Key.ControllerID
		}
		return sortedGroups[i].MinTimestampInt < sortedGroups[j].MinTimestampInt
	})

	for i, group := range sortedGroups {
		isLastGroup := (i == len(sortedGroups)-1)
		reconcileGroupLine := childrenBaseIndent
		if isLastGroup {
			reconcileGroupLine += "└─ "
		} else {
			reconcileGroupLine += "├─ "
		}

		formattedGroupTime := formatTimestamp(group.MinTimestampStr) // Use helper

		fmt.Printf("%s%s reconcile (reconcile ID: %s, timestamp: %s)\n",
			reconcileGroupLine,
			group.ActualControllerID,
			group.Key.ReconcileID,
			formattedGroupTime,
		)

		operationsIndent := childrenBaseIndent
		if isLastGroup {
			operationsIndent += "  "
		} else {
			operationsIndent += "│ "
		}

		for j, actualWriteEventNodeInGroup := range group.Events {
			isLastEventInThisGroup := (j == len(group.Events)-1)
			// Depth increases for each level of *event* nesting.
			// Reconcile groups are part of the presentation of an event's children.
			lm.printProvenanceTree(&actualWriteEventNodeInGroup, depth+1, operationsIndent, isLastEventInThisGroup)
		}
	}
}

func (lm *LensManager) findTopLevelEvent(rootEventID string, controllerID string) (*StateEvent, error) {
	for _, event := range lm.state.stateEvents {
		if event.Labels[tag.TraceyWebhookLabel] == rootEventID && event.ControllerID == controllerID {
			return &event, nil
		}
	}
	return nil, errors.Errorf("no top-level event found for ID %s and controller ID %s", rootEventID, controllerID)
}
