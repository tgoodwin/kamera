package tracecheck

import (
	"fmt"
	"testing"

	"github.com/tgoodwin/kamera/pkg/snapshot"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func BenchmarkStateNodeHash(b *testing.B) {
	cases := []struct {
		name    string
		objects int
		pending int
	}{
		{"10obj_2pending", 10, 2},
		{"100obj_5pending", 100, 5},
		{"500obj_10pending", 500, 10},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			node := makeBenchmarkStateNode(tc.objects, tc.pending)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = node.Hash()
			}
		})
	}
}

func BenchmarkStateNodeOrderSensitiveHash(b *testing.B) {
	cases := []struct {
		name    string
		objects int
		pending int
	}{
		{"10obj_2pending", 10, 2},
		{"100obj_5pending", 100, 5},
		{"500obj_10pending", 500, 10},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			node := makeBenchmarkStateNode(tc.objects, tc.pending)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = node.OrderSensitiveHash()
			}
		})
	}
}

func BenchmarkExecutionHistoryUniqueKey(b *testing.B) {
	cases := []struct {
		name        string
		historySize int
		effectsPer  int
	}{
		{"len5_effect1", 5, 1},
		{"len20_effect2", 20, 2},
		{"len50_effect3", 50, 3},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			hist := makeBenchmarkHistory(tc.historySize, tc.effectsPer)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = hist.UniqueKey()
			}
		})
	}
}

// makeBenchmarkStateNode builds a deterministic StateNode for hashing benchmarks.
func makeBenchmarkStateNode(numObjects, numPending int) StateNode {
	contents := make(ObjectVersions, numObjects)
	for i := 0; i < numObjects; i++ {
		key := snapshot.NewCompositeKeyWithGroup("g", "Kind", "ns", fmt.Sprintf("obj-%d", i), fmt.Sprintf("obj-%d", i))
		contents[key] = snapshot.NewDefaultHash(fmt.Sprintf("hash-%d", i))
	}

	kindSeq := KindSequences{
		"g/Kind": int64(numObjects),
	}

	pending := make([]PendingReconcile, 0, numPending)
	for i := 0; i < numPending; i++ {
		pending = append(pending, PendingReconcile{
			ReconcilerID: fmt.Sprintf("reconciler-%d", i),
			Request: reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: "ns",
					Name:      fmt.Sprintf("obj-%d", i%numObjects),
				},
			},
		})
	}

	return StateNode{
		Contents:          StateSnapshot{contents: contents, KindSequences: kindSeq},
		PendingReconciles: pending,
	}
}

func makeBenchmarkHistory(length, effectsPer int) ExecutionHistory {
	hist := make(ExecutionHistory, 0, length)
	for i := 0; i < length; i++ {
		effects := make([]Effect, effectsPer)
		for j := 0; j < effectsPer; j++ {
			key := snapshot.NewCompositeKeyWithGroup("g", "Kind", "ns", fmt.Sprintf("obj-%d", j), fmt.Sprintf("obj-%d", j))
			effects[j] = Effect{
				Key:     key,
				OpType:  "CREATE",
				Version: snapshot.NewDefaultHash(fmt.Sprintf("hash-%d-%d", i, j)),
			}
		}
		hist = append(hist, &ReconcileResult{
			ControllerID: fmt.Sprintf("reconciler-%d", i%3),
			FrameID:      fmt.Sprintf("frame-%d", i),
			FrameType:    FrameTypeExplore,
			Changes: Changes{
				ObjectVersions: make(ObjectVersions),
				Effects:        effects,
			},
		})
	}
	return hist
}

func BenchmarkStateNodeClone(b *testing.B) {
	cases := []struct {
		name        string
		pending     int
		historySize int
	}{
		{"pending5_hist5", 5, 5},
		{"pending20_hist20", 20, 20},
		{"pending50_hist50", 50, 50},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			node := makeBenchmarkStateNode(10, tc.pending)
			node.ExecutionHistory = makeBenchmarkHistory(tc.historySize, 1)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = node.Clone()
			}
		})
	}
}
