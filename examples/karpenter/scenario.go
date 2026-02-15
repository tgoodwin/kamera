package main

import (
	"fmt"

	"github.com/tgoodwin/kamera/pkg/coverage"
	"github.com/tgoodwin/kamera/pkg/explore"
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/test"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
)

func newScenarioObjects() ([]client.Object, error) {
	// TestNodeClass (fake cloud provider)
	nc := test.NodeClass(v1alpha1.TestNodeClass{ObjectMeta: metav1.ObjectMeta{Name: "default", UID: types.UID("testnodeclass-uid")}})
	tag.AddSleeveObjectID(nc)

	// NodePool referencing the TestNodeClass
	np := test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "default", UID: types.UID("nodepool-uid")}})
	tag.AddSleeveObjectID(np)

	// Provisionable Pod (PodScheduled=False, Reason=Unschedulable)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pending", Namespace: "default", UID: types.UID("pod-uid")},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "pause"}}},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{
			Type:   corev1.PodScheduled,
			Status: corev1.ConditionFalse,
			Reason: corev1.PodReasonUnschedulable,
		}}},
	}
	tag.AddSleeveObjectID(pod)

	return []client.Object{nc, np, pod}, nil
}

func buildInitialKarpenterState(builder *tracecheck.ExplorerBuilder) tracecheck.StateNode {
	stateBuilder := builder.NewStateEventBuilder()
	objs, _ := newScenarioObjects()

	nc := objs[0]
	np := objs[1]
	pod := objs[2]

	// Trigger pod-related controllers at start.
	// NOTE: We explicitly enqueue the provisioner once to simulate the singleton reconcile loop
	// firing at least once in the DFS. This approximates the real ticker-driven trigger.
	podState := stateBuilder.AddTopLevelObject(pod, "state.pod", "provisioner.trigger.pod", "provisioner")
	poolState := stateBuilder.AddTopLevelObject(np, "state.nodepool")
	classState := stateBuilder.AddTopLevelObject(nc)

	state := tracecheck.MergeStateNodes(podState, poolState)
	return tracecheck.MergeStateNodes(state, classState)
}

func scenariosFromInputs(_ *tracecheck.ExplorerBuilder, _ []coverage.Input) ([]explore.Scenario, error) {
	return nil, fmt.Errorf("input to scenario conversion not implemented")
}
