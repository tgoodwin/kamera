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

// ProvenanceLens analyzes and prints causal provenance, tracing upwards to a root
// and then downwards for the root and its siblings.
func (lm *LensManager) ProvenanceLens(slug string) error {
	if lm.state == nil {
		return errors.New("LensManager state is nil")
	}
	if lm.eventsByReconcileID == nil {
		return errors.New("LensManager eventsByReconcileID is nil")
	}
	if lm.state.stateEvents == nil { // Ensure all events are available for lookup
		return errors.New("LensManager state.stateEvents is nil")
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
		tsI, _ := strconv.ParseInt(relevantEvents[i].Timestamp, 10, 64)
		tsJ, _ := strconv.ParseInt(relevantEvents[j].Timestamp, 10, 64)
		return tsI < tsJ
	})

	firstWriteEvent := relevantEvents[0] // This is the event identified by the slug

	fmt.Printf("Starting Provenance Analysis from event ID: %s for object: %s\n", firstWriteEvent.ID, firstWriteEvent.Effect.Key.String())

	// Trace upwards to find the effective root and its siblings
	visitedUpwards := make(map[string]bool)
	effectiveRootEvent, siblingEvents, isWebhookRoot, webhookID, err := lm.findCausalRootAndSiblings(firstWriteEvent, visitedUpwards)
	if err != nil {
		return errors.Wrapf(err, "failed to find causal root starting from event ID %s", firstWriteEvent.ID)
	}

	// Prepare a list of top-level nodes to process (the effective root and its siblings)
	topLevelEventsToProcess := []StateEvent{}
	if effectiveRootEvent != nil {
		topLevelEventsToProcess = append(topLevelEventsToProcess, *effectiveRootEvent)
	}
	topLevelEventsToProcess = append(topLevelEventsToProcess, siblingEvents...)

	// Deduplicate topLevelEventsToProcess just in case (e.g., if effectiveRootEvent was also found as a sibling)
	processedTopLevelIDs := make(map[string]bool)
	uniqueTopLevelEvents := []StateEvent{}
	for _, ev := range topLevelEventsToProcess {
		if !processedTopLevelIDs[ev.ID] {
			uniqueTopLevelEvents = append(uniqueTopLevelEvents, ev)
			processedTopLevelIDs[ev.ID] = true
		}
	}
	// Sort them by timestamp for consistent output order
	sort.Slice(uniqueTopLevelEvents, func(i, j int) bool {
		tsI, _ := strconv.ParseInt(uniqueTopLevelEvents[i].Timestamp, 10, 64)
		tsJ, _ := strconv.ParseInt(uniqueTopLevelEvents[j].Timestamp, 10, 64)
		return tsI < tsJ
	})

	fmt.Println("\nCausality Tree(s):")
	if isWebhookRoot {
		fmt.Printf("ROOT (Webhook)\nRootID: %s\n", webhookID)
	} else if effectiveRootEvent != nil {
		// Standard root, the printProvenanceTree will handle its "ROOT: " prefix if it's the first.
		// If there are multiple top-level trees (e.g. from siblings of a non-webhook root)
		// the first one will be prefixed ROOT, others will just start.
	}

	for i, topEvent := range uniqueTopLevelEvents {
		if i > 0 && len(uniqueTopLevelEvents) > 1 {
			fmt.Println("\n--- Sibling Tree ---") // Separator for multiple top-level trees
		}

		// For the very first tree being printed, it's the main root.
		// For subsequent trees (siblings), they are roots of their own sub-provenance.
		// isActualRoot := (i == 0 && effectiveRootEvent != nil && topEvent.ID == effectiveRootEvent.ID)

		rootNode := ProvenanceNode{
			Key:   topEvent.Effect.Key,
			Event: topEvent,
		}
		// klog.V(2).Infof("Building downward tree for top-level event: ID %s, ChangeID %s", topEvent.ID, topEvent.Labels[tag.ChangeID])
		expandedDownwards := make(map[string]bool)
		lm.buildCausalityRecursively(&rootNode, expandedDownwards)

		// The first event printed by printProvenanceTree gets "ROOT: " prefix.
		// If we have multiple top-level events, only the true effectiveRootEvent should be the "ROOT".
		// We can pass a flag or adjust printProvenanceTree.
		// For now, the current printProvenanceTree prints "ROOT: " if depth is 0.
		// This means each tree from uniqueTopLevelEvents will start with "ROOT: " if called with depth 0.
		// This might be acceptable if each is considered a root of its own provenance chain.
		// If only one true "ROOT" is desired, the printing logic would need more context.
		// Let's assume for now, each top-level event starts a tree that can be prefixed "ROOT:".
		lm.printProvenanceTree(&rootNode, 0, "", true)
	}

	return nil
}

// findCausalRootAndSiblings traces upwards from startEvent to find an effective root.
// Returns: (effectiveRootEvent, siblingEvents, isWebhookRoot, webhookID, error)
func (lm *LensManager) findCausalRootAndSiblings(
	currentEvent StateEvent,
	visitedUpwards map[string]bool,
) (*StateEvent, []StateEvent, bool, string, error) {

	if visitedUpwards[currentEvent.ID] {
		// klog.Warningf("Upward cycle detected at event ID %s. Stopping upward search for this path.", currentEvent.ID)
		// Return currentEvent as the "root" for this path to break the cycle. Its siblings are writes in its own reconcile.
		siblings := lm.getSiblingWriteEvents(currentEvent)
		return &currentEvent, siblings, false, "", errors.Errorf("upward cycle detected at event ID %s", currentEvent.ID)
	}
	visitedUpwards[currentEvent.ID] = true
	// Create a new map for the next recursive call to ensure path-specific cycle detection
	nextVisitedUpwards := make(map[string]bool)
	for k, v := range visitedUpwards {
		nextVisitedUpwards[k] = v
	}

	eventsInCurrentReconcile := lm.eventsByReconcileID[currentEvent.ReconcileID]
	// if eventsInCurrentReconcile == nil { // This case should ideally not happen if currentEvent is valid
	// 	// klog.Warningf("No events found for ReconcileID %s of event %s. Treating as root.", currentEvent.ReconcileID, currentEvent.ID)
	// 	return &currentEvent, nil, false, "", nil // No siblings if reconcile events are missing
	// }

	readEventsInReconcile := []StateEvent{}
	for _, e := range eventsInCurrentReconcile {
		if event.IsReadOp(event.OperationType(e.OpType)) {
			readEventsInReconcile = append(readEventsInReconcile, e)
		}
	}

	if len(readEventsInReconcile) == 0 {
		// No reads in this reconcile, so currentEvent is a root for this path.
		siblings := lm.getSiblingWriteEvents(currentEvent)
		return &currentEvent, siblings, false, "", nil
	}

	// Parent Selection Heuristic:
	var potentialParentReads []StateEvent
	for _, r := range readEventsInReconcile {
		if r.Labels != nil {
			_, hasChangeID := r.Labels[tag.ChangeID]
			_, hasTraceyUID := r.Labels[tag.TraceyWebhookLabel] // Use the correct constant
			if hasChangeID || hasTraceyUID {
				potentialParentReads = append(potentialParentReads, r)
			}
		}
	}

	if len(potentialParentReads) == 0 {
		// No reads that can link upwards, currentEvent is a root.
		siblings := lm.getSiblingWriteEvents(currentEvent)
		return &currentEvent, siblings, false, "", nil
	}

	sort.Slice(potentialParentReads, func(i, j int) bool {
		tsI, _ := strconv.ParseInt(potentialParentReads[i].Timestamp, 10, 64)
		tsJ, _ := strconv.ParseInt(potentialParentReads[j].Timestamp, 10, 64)
		if tsI == tsJ { // Tie-break by ID for deterministic behavior
			return potentialParentReads[i].ID < potentialParentReads[j].ID
		}
		return tsI < tsJ
	})

	selectedParentReadEvent := potentialParentReads[0]
	if len(potentialParentReads) > 1 {
		tsSelected, _ := strconv.ParseInt(selectedParentReadEvent.Timestamp, 10, 64)
		tsNext, _ := strconv.ParseInt(potentialParentReads[1].Timestamp, 10, 64)
		if tsSelected == tsNext {
			fmt.Printf("WARNING: Tie in timestamps for selecting causal parent read event in ReconcileID %s. Selected %s based on earliest timestamp and ID.\n", currentEvent.ReconcileID, selectedParentReadEvent.ID)
		}
	}
	// klog.V(3).Infof("Selected parent read event %s (Timestamp: %s) in ReconcileID %s for upward trace from %s", selectedParentReadEvent.ID, selectedParentReadEvent.Timestamp, currentEvent.ReconcileID, currentEvent.ID)

	parentReadChangeID := ""
	traceyUID := ""
	if selectedParentReadEvent.Labels != nil {
		parentReadChangeID = selectedParentReadEvent.Labels[tag.ChangeID]
		traceyUID = selectedParentReadEvent.Labels[tag.TraceyWebhookLabel] // Use the correct constant
	}

	if parentReadChangeID == "" && traceyUID != "" {
		// This is a webhook root. currentEvent is the first write event under this webhook-initiated reconcile.
		// The "effective root" is currentEvent. Its siblings are other writes in its own reconcile.
		// klog.V(2).Infof("Webhook root identified by TraceyWebhook ID %s via read event %s. Effective root is %s.", traceyUID, selectedParentReadEvent.ID, currentEvent.ID)
		siblings := lm.getSiblingWriteEvents(currentEvent)
		return &currentEvent, siblings, true, traceyUID, nil
	}

	if parentReadChangeID != "" {
		// Find the upstream write event that produced this parentReadChangeID.
		var actualUpstreamWriteEvent *StateEvent
		for i := range lm.state.stateEvents { // Must iterate by index to take address for pointer
			candidateEvent := lm.state.stateEvents[i]
			if event.IsWriteOp(event.OperationType(candidateEvent.OpType)) &&
				candidateEvent.Labels != nil &&
				candidateEvent.Labels[tag.ChangeID] == parentReadChangeID {
				actualUpstreamWriteEvent = &lm.state.stateEvents[i] // Assign address
				break
			}
		}

		if actualUpstreamWriteEvent == nil {
			fmt.Printf("WARNING: Upstream write event for ChangeID %s (read by %s in Reconcile %s) not found. Treating event %s as a root.\n",
				parentReadChangeID, selectedParentReadEvent.ID, selectedParentReadEvent.ReconcileID, currentEvent.ID)
			siblings := lm.getSiblingWriteEvents(currentEvent)
			return &currentEvent, siblings, false, "", nil
		}
		// klog.V(3).Infof("Found upstream write event %s for ChangeID %s. Recursing upwards.", actualUpstreamWriteEvent.ID, parentReadChangeID)
		return lm.findCausalRootAndSiblings(*actualUpstreamWriteEvent, nextVisitedUpwards)
	}

	// If selectedParentReadEvent had neither a valid ChangeID to trace nor a TraceyWebhook label,
	// or if parentReadChangeID was empty and traceyUID was also empty.
	// Treat currentEvent as a root.
	// klog.V(2).Infof("No further upward link from read event %s. Treating event %s as a root.", selectedParentReadEvent.ID, currentEvent.ID)
	siblings := lm.getSiblingWriteEvents(currentEvent)
	return &currentEvent, siblings, false, "", nil
}

// getSiblingWriteEvents finds other write events in the same reconcile as the given event.
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

// buildCausalityRecursively populates the CausalChildren of the given parentNode.
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
	for _, event := range directChildrenEvents {
		sortedChildStateEvents = append(sortedChildStateEvents, event)
	}
	sort.Slice(sortedChildStateEvents, func(i, j int) bool {
		tsI, _ := strconv.ParseInt(sortedChildStateEvents[i].Timestamp, 10, 64)
		tsJ, _ := strconv.ParseInt(sortedChildStateEvents[j].Timestamp, 10, 64)
		return tsI < tsJ
	})

	parentNode.CausalChildren = make([]ProvenanceNode, 0, len(sortedChildStateEvents))
	for _, childEvent := range sortedChildStateEvents {
		childNode := ProvenanceNode{
			Key:   childEvent.Effect.Key,
			Event: childEvent,
		}
		lm.buildCausalityRecursively(&childNode, currentPathExpandedIDs)
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
		// Only the very first node of a top-level tree gets "ROOT: "
		// This is handled by the initial call in ProvenanceLens.
		// If we want to distinguish subsequent top-level trees (siblings),
		// the ProvenanceLens loop should handle their specific "header".
		// For now, this makes each top-level start with "ROOT: "
		currentLineOutput += "ROOT: "
	}

	// Use Effect.Key.Kind and Effect.Key.Name as per user's canvas version of this function
	// The canvas version had: eventNode.Event.Effect.Key.ResourceKey.Kind
	// Assuming CompositeKey has Kind and Name directly for now.
	// If CompositeKey has a ResourceKey field, it should be:
	// eventNode.Event.Effect.Key.ResourceKey.Kind and eventNode.Event.Effect.Key.ResourceKey.Name
	// Sticking to the user's latest canvas version for this print line.
	kind := eventNode.Event.Effect.Key.ResourceKey.Kind
	name := eventNode.Event.Effect.Key.ResourceKey.Name

	timestampStr := eventNode.Event.Timestamp
	var formattedTime string
	timestampInt, err := strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		// klog.Errorf("Error parsing timestamp string '%s': %v", timestampStr, err)
		formattedTime = fmt.Sprintf("InvalidTimestamp(%s)", timestampStr)
	} else {
		// Assuming timestamp is in microseconds as per user's canvas
		parsedTime := time.Unix(0, timestampInt*int64(time.Microsecond))
		formattedTime = parsedTime.Format(time.RFC3339)
	}

	// Printing format from user's canvas: OpType Kind "Name" (Time: formattedTime)
	fmt.Printf("%s%s %s %q (Time: %s)\n",
		currentLineOutput,
		eventNode.Event.OpType,
		kind, // Use kind derived above
		name, // Use name derived above
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
		MinTimestampStr    string // Keep as string for consistency with Event.Timestamp
		MinTimestampInt    int64  // For actual numeric sorting
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

		// Format the MinTimestamp for the reconcile group header
		var formattedGroupTime string
		groupTsInt, err := strconv.ParseInt(group.MinTimestampStr, 10, 64)
		if err != nil {
			formattedGroupTime = fmt.Sprintf("InvalidTimestamp(%s)", group.MinTimestampStr)
		} else {
			parsedGroupTime := time.Unix(0, groupTsInt*int64(time.Microsecond))
			formattedGroupTime = parsedGroupTime.Format(time.RFC3339Nano) // Use RFC3339Nano for more precision if available
		}

		// Re-added timestamp to reconcile group header as per earlier user request style
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
			lm.printProvenanceTree(&actualWriteEventNodeInGroup, depth+1, operationsIndent, isLastEventInThisGroup)
		}
	}
}
