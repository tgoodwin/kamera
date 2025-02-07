package replay

import (
	"fmt"
	"sort"

	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"github.com/tgoodwin/sleeve/pkg/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

func (b *Builder) FindMissedObservations(controllerID string) (map[string]util.Set[event.CausalKey], error) {
	if _, ok := b.ReconcilerIDs[controllerID]; !ok {
		return nil, fmt.Errorf("controllerID not found in trace: %s", controllerID)
	}

	harness, err := b.BuildHarness(controllerID)
	if err != nil {
		return nil, err
	}

	readDependencies := make(map[string]struct{})
	knowledgeByKind := make(map[string]map[event.CausalKey]struct{})

	paStates := make(map[string]struct{})
	for _, effects := range harness.tracedEffects {
		for _, e := range effects.Reads {
			readDependencies[e.Kind] = struct{}{}
			if _, ok := knowledgeByKind[e.Kind]; !ok {
				knowledgeByKind[e.Kind] = make(map[event.CausalKey]struct{})
			}

			// TODO this is a hack due to the fact that my faknative code
			// simulates race conditions by purposefully not acting on some reads
			// if the object's creationTimestamp is before or after a certain time
			// so "missed observations" do in fact appear in the trace data
			if len(effects.Writes) > 0 {
				knowledgeByKind[e.Kind][e.CausalKey()] = struct{}{}
			}

			if e.Kind == "FakePodAutoscaler" {
				k := e.VersionKey()
				cid := e.ChangeID()
				str := fmt.Sprintf("read %s:%s:%s@%s", k.Kind, k.ObjectID, k.Version, cid)
				paStates[str] = struct{}{}
			}
		}

		for _, e := range effects.Writes {
			if _, ok := knowledgeByKind[e.Kind]; !ok {
				knowledgeByKind[e.Kind] = make(map[event.CausalKey]struct{})
			}
			if e.Kind == "FakePodAutoscaler" {
				k := e.VersionKey()
				cid := e.ChangeID()
				str := fmt.Sprintf("wrote %s:%s:%s@%s", k.Kind, k.ObjectID, k.Version, cid)
				paStates[str] = struct{}{}
			}

			knowledgeByKind[e.Kind][e.CausalKey()] = struct{}{}
		}
	}

	// knowledgeset by kind
	out := make(map[string]util.Set[event.CausalKey])

	for kind := range readDependencies {
		allOfKind := b.AllOfKind(kind)
		// diffList, err := showDiffs(allOfKind)
		if err != nil {
			fmt.Printf("error showing diffs for kind %s: %v\n", kind, err)
		}

		out[kind] = make(util.Set[event.CausalKey])

		// diffByCurrVersion := make(map[event.CausalKey]diff)
		// for _, d := range diffList {
		// 	diffByCurrVersion[d.curr] = d
		// }

		allKnowledge := asKnowledge(allOfKind)
		localKnowledge := knowledgeByKind[kind]
		knowledgeDiff := allKnowledge.Diff(localKnowledge)
		if len(knowledgeDiff) > 0 {
			fmt.Printf("controller %s %d missed observations for %s\n", controllerID, len(knowledgeDiff), kind)
			for key := range knowledgeDiff {
				missedObj, ok := b.store[key]
				if !ok {
					return nil, fmt.Errorf("failed to find object with causalID %s", key)
				}
				cid, _ := event.GetChangeID(missedObj)
				// fmt.Printf("missed object's labels: %v\n", missedObj.GetLabels())
				fmt.Printf("missed observation: %#v @ %s\n", key, cid)
				// if d, ok := diffByCurrVersion[key]; ok {
				// 	fmt.Printf("%s\n\t%s\n", key, d.delta)
				// }
				out[kind].Add(key)
			}
		}
	}

	return out, nil
}

// KnowledgeSet is keyed by ChangeID
func (b *Builder) InterpolateFrames(controllerID string, missedKnowledge util.Set[event.CausalKey]) (*Harness, error) {
	harness, err := b.BuildHarness(controllerID)
	if err != nil {
		return nil, fmt.Errorf("building harness: %w", err)
	}

	HARDCODED_KEY := "81e0be03-fa11-4103-9054-e8e4e1b6abeb"

	for causalKey := range missedKnowledge {
		if causalKey.ChangeID != event.ChangeID(HARDCODED_KEY) {
			continue
		}
		// key represents a causalID
		storeObj, ok := b.store[causalKey]
		if !ok {
			return nil, fmt.Errorf("failed to find object with causalID %s", causalKey)
		}
		// find
		ts, err := b.getEarlistTimestampForKey(causalKey)
		if err != nil {
			return nil, fmt.Errorf("failed to find earliest timestamp for key %s: %w", causalKey, err)
		}

		// fmt.Printf("missing object version: %s\n", causalKey)
		// fmt.Printf("earliest timestamp for missing object version: %s\n", ts)
		for i, frame := range harness.frames {
			fmt.Printf("frame %d: %s @ time %s\n", i, frame.ID, frame.sequenceID)
		}

		nearestFrame := harness.nearestFrame(ts)
		fmt.Printf("nearest frame to time %s is %s\n", ts, nearestFrame.ID)
		data := harness.frameDataByFrameID[nearestFrame.ID].Copy()

		overwriteKey := types.NamespacedName{
			Namespace: storeObj.GetNamespace(),
			Name:      storeObj.GetName(),
		}
		if _, ok := data[storeObj.GetKind()][overwriteKey]; !ok {
			// print out the frame data
			fmt.Printf("frame data for frame %s\n", nearestFrame.ID)
			for objs := range data[storeObj.GetKind()] {
				fmt.Printf("%s\n", objs)
			}
			return nil, fmt.Errorf("%s with namespace/name %s/%s not found in frame data", storeObj.GetKind(), storeObj.GetNamespace(), storeObj.GetName())
		}
		data[storeObj.GetKind()][overwriteKey] = storeObj

		newFrameID := util.UUID()
		newFrame := Frame{
			Type:         FrameTypeSynthetic,
			ID:           newFrameID,
			sequenceID:   ts,
			Req:          nearestFrame.Req,
			TraceyRootID: nearestFrame.TraceyRootID,
		}
		harness.frameDataByFrameID[newFrameID] = data
		harness.insertFrame(newFrame)

		// summary
		fmt.Println("\nIterplation strategy:")
		for i, frame := range harness.frames {
			fmt.Printf("frame %d: %s:%s @ time %s\n", i, frame.Type, frame.ID, frame.sequenceID)
		}
		fmt.Println("")
	}

	return harness, nil
}

func (b *Builder) getEarlistTimestampForKey(key event.CausalKey) (string, error) {
	for _, event := range b.events {
		if event.ChangeID() == key.ChangeID {
			if event.OpType == "GET" || event.OpType == "LIST" {
				return event.Timestamp, nil
			}
		}
	}
	return "", fmt.Errorf("failed to find change event for key %s", key)

}

func asKnowledge(elems []*unstructured.Unstructured) util.Set[event.CausalKey] {
	versions := make(util.Set[event.CausalKey])
	for _, elem := range elems {
		cid, _ := event.GetChangeID(elem)
		key := event.CausalKey{
			Kind:     elem.GetKind(),
			ObjectID: string(elem.GetUID()),
			ChangeID: cid,
		}
		versions[key] = struct{}{}
	}
	return versions
}

type diff struct {
	prev  snapshot.VersionKey
	curr  snapshot.VersionKey
	delta string
}

func showDiffs(versions []*unstructured.Unstructured) ([]diff, error) {

	if len(versions) == 0 {
		return nil, nil
	}

	kind := versions[0].GetKind()
	for _, v := range versions {
		if v.GetKind() != kind {
			return nil, fmt.Errorf("error: versions contain different kinds: %s and %s", kind, v.GetKind())
		}
	}

	// defensively sort the versions vy resource version
	sort.Slice(versions, func(i, j int) bool {
		return versions[i].GetResourceVersion() < versions[j].GetResourceVersion()
	})

	diffList := make([]diff, 0)

	// iterate over the list, comparing each element to the next
	for i := 0; i < len(versions)-1; i++ {
		a := versions[i]
		b := versions[i+1]
		diffstr := snapshot.ComputeDelta(a, b)
		d := diff{
			// TODO ITS UNCLEAR WHETHER OR NOT WE SHOULD USE UID OR NAME HERE, AS UID IS NOT ALWAYS SET
			prev:  snapshot.VersionKey{Kind: kind, ObjectID: string(a.GetUID()), Version: a.GetResourceVersion()},
			curr:  snapshot.VersionKey{Kind: kind, ObjectID: string(b.GetUID()), Version: b.GetResourceVersion()},
			delta: diffstr,
		}
		diffList = append(diffList, d)
		// fmt.Printf("diff between %s and %s:\n%s\n", cidA, cidB, diffstr)
	}
	return diffList, nil
}
