package tracecheck

import (
	"context"
	"strings"
	"testing"

	foov1 "github.com/tgoodwin/kamera/pkg/test/integration/api/v1"
	"github.com/tgoodwin/kamera/pkg/test/integration/controller"
	"github.com/tgoodwin/kamera/pkg/util"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func runtimeScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(foov1.AddToScheme(scheme))
	return scheme
}

// Canonicalizes KindSequences for Foo resources (matching integration tests).
func canonicalizeKindSequences(seq KindSequences) KindSequences {
	if seq == nil {
		return nil
	}
	out := make(KindSequences, len(seq))
	for k, v := range seq {
		if strings.Contains(k, "/") {
			out[k] = v
			continue
		}
		out[util.CanonicalGroupKind(groupForTestKind(k), k)] = v
	}
	return out
}

func groupForTestKind(kind string) string {
	switch kind {
	case "Foo":
		return "webapp.discrete.events"
	default:
		return ""
	}
}

func runFooBarExplore(t *testing.T, opt OptimizationConfig) (*Explorer, *Result) {
	t.Helper()

	scheme := runtimeScheme(t)
	eb := NewExplorerBuilder(scheme)
	eb.WithMaxDepth(10)
	if opt.AnyEnabled() {
		eb.WithOptimizations(opt)
	} else {
		eb.WithoutOptimizations()
	}

	fooKind := "webapp.discrete.events/Foo"
	eb.WithReconciler("FooController", func(c ctrlclient.Client) Reconciler {
		return &controller.TestReconciler{
			Client: c,
			Scheme: scheme,
		}
	}).For(fooKind).Watches(fooKind, EnqueueRequestForObject()).PermuteOrder()

	eb.WithReconciler("BarController", func(c ctrlclient.Client) Reconciler {
		return &controller.TestReconciler{
			Client: c,
			Scheme: scheme,
		}
	}).For(fooKind).Watches(fooKind, EnqueueRequestForObject()).PermuteOrder()

	topLevelObj := &foov1.Foo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
			Labels: map[string]string{
				"tracey-uid":                       "foo",
				"discrete.events/sleeve-object-id": "foo-123",
			},
		},
		TypeMeta: metav1.TypeMeta{
			APIVersion: "webapp.discrete.events/v1",
			Kind:       "Foo",
		},
		Spec: foov1.FooSpec{
			Mode: "A",
		},
	}

	initialState := eb.GetStartStateFromObject(topLevelObj, "FooController", "BarController")
	initialState.Contents.KindSequences = canonicalizeKindSequences(initialState.Contents.KindSequences)
	explorer, err := eb.Build("standalone")
	if err != nil {
		t.Fatalf("build explorer: %v", err)
	}

	result := explorer.Explore(context.Background(), initialState)
	return explorer, result
}

func assertSingleConverged(t *testing.T, res *Result) {
	t.Helper()
	if len(res.ConvergedStates) != 1 {
		t.Fatalf("expected 1 converged state, got %d", len(res.ConvergedStates))
	}
}

func TestFooBarOptimizationsExactEffects(t *testing.T) {
	type expect struct {
		name                    string
		opt                     OptimizationConfig
		total, unique           int
		skippedPaths            int
		skippedOrders           int
		earlySkips              int
		cacheSkips              int
		skippedNoOpOrderings    int
		convergedPathCount      int
		convergedStateHashMatch bool
	}

	// Baseline stats observed for the Foo/Bar scenario:
	// - No optimizations: total=93, unique=9, paths=4
	//   Full permutation search: branch on every pending ordering at each step,
	//   revisit the same logical states under different histories, and keep all
	//   redundant no-op tail orderings. Yields four converged execution paths.
	// - Ordering pruning: total=29, unique=9, skippedOrders=6, paths=3
	//   Branch orderings only once per logical pending list; duplicate branching
	//   disappears, dropping the extra path that differed only by ordering.
	// - Cache prediction: total=37, unique=9, cacheSkips=7, paths=4
	//   Reconciles whose output was already enqueued for exploration get skipped;
	//   tree shape stays the same but several redundant steps vanish.
	// - Early convergence: total=15, unique=7, earlySkips=26, paths=1
	//   Once both reconcilers are known no-ops on the final state, all tail
	//   reorderings are pruned after the first converged hash is found.
	// - Completed-path dedup: total=70, unique=9, skippedPaths=23, paths=4
	//   After one full exploration of a (state, history) to convergence, later
	//   attempts to re-walk the same completion are skipped.
	// - All optimizations: total=10, unique=7, skippedOrders=3, earlySkips=4, cacheSkips=2, paths=1
	//   Each heuristic removes its slice of redundancy, leaving the minimal
	//   traversal to the single converged result.
	//
	// Why 93 total visits with no optimizations?
	// The Foo/Bar scenario has 4 meaningful state-changing reconciles. At every
	// state with Pending=[Foo,Bar] (there are six such points: initial + after
	// each state-changing step + the two no-op tails), the explorer enqueues both
	// permutations. DFS walks every enqueued node; the multiplicative 2!
	// branching across those six ordering points, plus the intermediate
	// in-flight states between permutations, yields 93 total visits over 9
	// unique logical states and 4 converged paths. The cases below pin the exact
	// counts for each heuristic.
	cases := []expect{
		// Full permutation search: branch on every pending ordering at each step,
		// revisit the same logical states under different histories, and keep all
		// redundant no-op tail orderings. Yields four converged execution paths.
		{
			name:                 "none",
			opt:                  OptimizationConfig{OnlyPermuteTriggered: true},
			total:                93,
			unique:               9,
			skippedPaths:         0,
			skippedOrders:        0,
			earlySkips:           0,
			cacheSkips:           0,
			skippedNoOpOrderings: 0,
			convergedPathCount:   4,
		},

		// Branch orderings only once per logical pending list; duplicate branching
		// disappears, dropping the extra path that differed only by ordering.
		{
			name:               "ordering",
			opt:                OptimizationConfig{OrderingPruning: true, OnlyPermuteTriggered: true},
			total:              29,
			unique:             9,
			skippedOrders:      6,
			convergedPathCount: 3,
		},

		// Reconciles whose output was already enqueued for exploration get skipped;
		// tree shape stays the same but several redundant steps vanish.
		{
			name:               "cache",
			opt:                OptimizationConfig{CachePrediction: true, OnlyPermuteTriggered: true},
			total:              37,
			unique:             9,
			cacheSkips:         7,
			convergedPathCount: 4,
		},

		// Once both reconcilers are known no-ops on the final state, all tail
		// reorderings are pruned after the first converged hash is found.
		{
			name:               "early",
			opt:                OptimizationConfig{EarlyConvergence: true, OnlyPermuteTriggered: true},
			total:              15,
			unique:             7,
			earlySkips:         26,
			convergedPathCount: 1,
		},

		// After one full exploration of a (state, history) to convergence, later
		// attempts to re-walk the same completion are skipped.
		{
			name:               "dedup",
			opt:                OptimizationConfig{CompletedPathDedup: true, OnlyPermuteTriggered: true},
			total:              70,
			unique:             9,
			skippedPaths:       23,
			convergedPathCount: 4,
		},

		// Each heuristic removes its slice of redundancy, leaving the minimal
		// traversal to the single converged result.
		{
			name: "all",
			opt: OptimizationConfig{
				EarlyConvergence:     true,
				CompletedPathDedup:   true,
				OrderingPruning:      true,
				CachePrediction:      true,
				OnlyPermuteTriggered: true,
			},
			total:              10,
			unique:             7,
			skippedOrders:      3,
			earlySkips:         4,
			cacheSkips:         2,
			convergedPathCount: 1,
		},
	}

	var baselineHash NodeHash

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			explorer, res := runFooBarExplore(t, tc.opt)
			assertSingleConverged(t, res)
			cs := res.ConvergedStates[0]

			if baselineHash == "" {
				baselineHash = cs.State.Hash()
			} else if cs.State.Hash() != baselineHash {
				t.Fatalf("converged state hash changed for %s: got %s want %s", tc.name, cs.State.Hash(), baselineHash)
			}

			stats := explorer.Stats()
			if stats.TotalNodeVisits != tc.total {
				t.Fatalf("%s: total node visits=%d, want %d", tc.name, stats.TotalNodeVisits, tc.total)
			}
			if stats.UniqueNodeVisits != tc.unique {
				t.Fatalf("%s: unique node visits=%d, want %d", tc.name, stats.UniqueNodeVisits, tc.unique)
			}
			if len(cs.Paths) != tc.convergedPathCount {
				t.Fatalf("%s: converged paths=%d, want %d", tc.name, len(cs.Paths), tc.convergedPathCount)
			}
			if stats.SkippedPaths != tc.skippedPaths {
				t.Fatalf("%s: skipped paths=%d, want %d", tc.name, stats.SkippedPaths, tc.skippedPaths)
			}
			if stats.SkippedOrderExpansions != tc.skippedOrders {
				t.Fatalf("%s: skipped order expansions=%d, want %d", tc.name, stats.SkippedOrderExpansions, tc.skippedOrders)
			}
			if stats.EarlyConvergence != tc.earlySkips {
				t.Fatalf("%s: early convergence skips=%d, want %d", tc.name, stats.EarlyConvergence, tc.earlySkips)
			}
			if stats.CachePredictedSkips != tc.cacheSkips {
				t.Fatalf("%s: cache predicted skips=%d, want %d", tc.name, stats.CachePredictedSkips, tc.cacheSkips)
			}
			if stats.SkippedNoOpOrderings != tc.skippedNoOpOrderings {
				t.Fatalf("%s: skipped no-op orderings=%d, want %d", tc.name, stats.SkippedNoOpOrderings, tc.skippedNoOpOrderings)
			}
		})
	}
}
