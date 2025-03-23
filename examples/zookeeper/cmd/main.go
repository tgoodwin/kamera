package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/samber/lo"
	appsv1 "github.com/tgoodwin/sleeve/examples/robinhood/api/v1"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/test/integration/controller"
	"github.com/tgoodwin/sleeve/pkg/tracecheck"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// var inFile = flag.String("logfile", "default.log", "path to the log file")
// var objectID = flag.String("objectID", "", "object ID to analyze")
// var reconcileID = flag.String("reconcileID", "", "object ID to analyze")
// var debug = flag.Bool("debug", false, "enable debug logging")
var searchDepth = flag.Int("search-depth", 10, "search depth for exploration")
var stalenessDepth = flag.Int("staleness-depth", 1, "staleness depth for exploration")
var outDir = flag.String("out-dir", "results", "output directory for results")

var scheme = runtime.NewScheme()

// CreateZookeeperObject creates a ZookeeperCluster unstructured object
func CreateZookeeperObject(name, namespace, uid string, size int64, deletionTimestamp *metav1.Time) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "zookeeper.pravega.io",
		Version: "v1beta1",
		Kind:    "ZookeeperCluster",
	})
	obj.SetName(name)
	obj.SetNamespace(namespace)
	obj.SetUID(types.UID(uid))
	obj.SetLabels(map[string]string{
		"tracey-uid":                       uid,
		"discrete.events/sleeve-object-id": uid,
	})
	obj.SetDeletionTimestamp(deletionTimestamp)

	_ = unstructured.SetNestedField(obj.Object, size, "spec", "size")

	return obj
}

// CreatePVCObject creates a PVC unstructured object
func CreatePVCObject(name, namespace, uid string, zkName string, ownerRef []metav1.OwnerReference, deletionTimestamp *metav1.Time) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "",
		Version: "v1",
		Kind:    "PersistentVolumeClaim",
	})
	obj.SetName(name)
	obj.SetNamespace(namespace)
	obj.SetUID(types.UID(uid))
	obj.SetLabels(map[string]string{
		"app":                              zkName, // This is the key label that connects to ZK
		"tracey-uid":                       uid,
		"discrete.events/sleeve-object-id": uid,
	})

	if ownerRef != nil {
		obj.SetOwnerReferences(ownerRef)
	}
	obj.SetDeletionTimestamp(deletionTimestamp)

	_ = unstructured.SetNestedField(obj.Object, "10Gi", "spec", "resources", "requests", "storage")
	_ = unstructured.SetNestedField(obj.Object, "standard", "spec", "storageClassName")

	return obj
}

func init() {
	flag.Parse()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(appsv1.AddToScheme(scheme))
}

func main() {
	eb := tracecheck.NewExplorerBuilder(scheme)
	eb.WithReconciler("ZookeeperReconciler", func(c tracecheck.Client) tracecheck.Reconciler {
		return &controller.ZookeeperReconciler{
			Client: c,
			Scheme: scheme,
		}
	})

	eb.WithResourceDep("ZookeeperCluster", "ZookeeperReconciler")
	eb.WithResourceDep("PersistentVolumeClaim", "ZookeeperReconciler")
	eb.AssignReconcilerToKind("ZookeeperReconciler", "ZookeeperCluster")

	emitter := event.NewDebugEmitter()
	eb.WithEmitter(emitter)

	stateBuilder := eb.NewStateEventBuilder()

	// Create a state builder to model our sequence of events

	// 1. First ZookeeperCluster is created
	zk1 := CreateZookeeperObject("zk-cluster", "default", "zk-old-uid", 3, nil)
	zk1OwnerRef := metav1.OwnerReference{
		APIVersion: "zookeeper.pravega.io/v1beta1",
		Kind:       "ZookeeperCluster",
		Name:       "zk-cluster",
		UID:        zk1.GetUID(),
	}
	stateBuilder.AddStateEvent("ZookeeperCluster", "zk-old-uid", zk1, event.CREATE, "ZookeeperReconciler")

	// 2. PVCs are created for the first ZK
	pvc1 := CreatePVCObject("zk-cluster-pvc-0", "default", "pvc-uid-1", "zk-cluster", []metav1.OwnerReference{zk1OwnerRef}, nil)
	stateBuilder.AddStateEvent("PersistentVolumeClaim", "pvc-uid-1", pvc1, event.CREATE, "ZookeeperReconciler")

	pvc2 := CreatePVCObject("zk-cluster-pvc-1", "default", "pvc-uid-2", "zk-cluster", []metav1.OwnerReference{zk1OwnerRef}, nil)
	stateBuilder.AddStateEvent("PersistentVolumeClaim", "pvc-uid-2", pvc2, event.CREATE, "ZookeeperReconciler")

	pvc3 := CreatePVCObject("zk-cluster-pvc-2", "default", "pvc-uid-3", "zk-cluster", []metav1.OwnerReference{zk1OwnerRef}, nil)
	stateBuilder.AddStateEvent("PersistentVolumeClaim", "pvc-uid-3", pvc3, event.CREATE, "ZookeeperReconciler")

	// 3. First ZK is marked for deletion
	deletionTime := metav1.NewTime(time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC))
	zk1WithDeletion := CreateZookeeperObject("zk-cluster", "default", "zk-old-uid", 3, &deletionTime)
	stateBuilder.AddStateEvent("ZookeeperCluster", "zk-old-uid", zk1WithDeletion, event.MARK_FOR_DELETION, "ZookeeperReconciler")

	// 4. As a result, PVCs are marked for deletion
	deletionTime = metav1.NewTime(time.Now())
	pvc1d := CreatePVCObject("zk-cluster-pvc-0", "default", "pvc-uid-1", "zk-cluster", []metav1.OwnerReference{zk1OwnerRef}, &deletionTime)
	pvc2d := CreatePVCObject("zk-cluster-pvc-1", "default", "pvc-uid-2", "zk-cluster", []metav1.OwnerReference{zk1OwnerRef}, &deletionTime)
	pvc3d := CreatePVCObject("zk-cluster-pvc-2", "default", "pvc-uid-3", "zk-cluster", []metav1.OwnerReference{zk1OwnerRef}, &deletionTime)

	stateBuilder.AddStateEvent("PersistentVolumeClaim", "pvc-uid-1", pvc1d, event.MARK_FOR_DELETION, "ZookeeperReconciler")
	stateBuilder.AddStateEvent("PersistentVolumeClaim", "pvc-uid-2", pvc2d, event.MARK_FOR_DELETION, "ZookeeperReconciler")
	stateBuilder.AddStateEvent("PersistentVolumeClaim", "pvc-uid-3", pvc3d, event.MARK_FOR_DELETION, "ZookeeperReconciler")

	// 5. PVCs are fully removed from cluster state (deleted)
	stateBuilder.AddStateEvent("PersistentVolumeClaim", "pvc-uid-1", pvc1d, event.REMOVE, "CleanupReconciler")
	stateBuilder.AddStateEvent("PersistentVolumeClaim", "pvc-uid-2", pvc2d, event.REMOVE, "CleanupReconciler")
	stateBuilder.AddStateEvent("PersistentVolumeClaim", "pvc-uid-3", pvc3d, event.REMOVE, "CleanupReconciler")

	// 6. First ZK is fully deleted
	stateBuilder.AddStateEvent("ZookeeperCluster", "zk-old-uid", zk1WithDeletion, event.REMOVE, "CleanupReconciler")

	// 6. New ZK with same name but different UID is created
	zk2 := CreateZookeeperObject("zk-cluster", "default", "zk-new-uid", 3, nil)
	zk2OwnerRef := metav1.OwnerReference{
		APIVersion: "zookeeper.pravega.io/v1beta1",
		Kind:       "ZookeeperCluster",
		Name:       "zk-cluster",
		UID:        zk2.GetUID(),
	}
	stateBuilder.AddStateEvent("ZookeeperCluster", "zk-new-uid", zk2, event.CREATE, "ZookeeperReconciler")

	// 7. New PVCs are created for the new ZK
	pvc4 := CreatePVCObject("zk-cluster-pvc-0", "default", "pvc-uid-4", "zk-cluster", []metav1.OwnerReference{zk2OwnerRef}, nil)
	stateBuilder.AddStateEvent("PersistentVolumeClaim", "pvc-uid-4", pvc4, event.CREATE, "ZookeeperReconciler")

	pvc5 := CreatePVCObject("zk-cluster-pvc-1", "default", "pvc-uid-5", "zk-cluster", []metav1.OwnerReference{zk2OwnerRef}, nil)
	stateBuilder.AddStateEvent("PersistentVolumeClaim", "pvc-uid-5", pvc5, event.CREATE, "ZookeeperReconciler")

	pvc6 := CreatePVCObject("zk-cluster-pvc-2", "default", "pvc-uid-6", "zk-cluster", []metav1.OwnerReference{zk2OwnerRef}, nil)
	stateBuilder.AddStateEvent("PersistentVolumeClaim", "pvc-uid-6", pvc6, event.CREATE, "ZookeeperReconciler")

	eb.ExploreStaleStates() // Enable staleness exploration
	eb.WithKindBounds("ZookeeperReconciler", tracecheck.KindBounds{
		"ZookeeperCluster":      *stalenessDepth,
		"PersistentVolumeClaim": 1,
	})
	eb.WithMaxDepth(*searchDepth) // tuned this experimentally

	explorer, err := eb.Build("standalone")
	if err != nil {
		panic(err)
	}

	// Build the state events
	initialState := stateBuilder.Build()
	initialState.Contents.Debug()

	initialState.PendingReconciles = []tracecheck.PendingReconcile{
		{
			ReconcilerID: "ZookeeperReconciler",
			Request: reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: "default",
					Name:      "zk-cluster",
				},
			},
		},
	}

	initialState.Contents = initialState.Contents.FixAt(
		tracecheck.KindSequences{"ZookeeperCluster": 7},
	)

	topHash := initialState.Hash()
	fmt.Println("initial state hash: ", topHash)

	// Set up a test logger
	// logger := zap.New(zap.UseDevMode(true))
	// tracecheck.SetLogger(logger)
	// log.SetLogger(logger)

	// Explore all possible execution paths
	result := explorer.Explore(context.Background(), initialState)

	fmt.Println("number of converged states: ", len(result.ConvergedStates))

	classifier := eb.NewStateClassifier()
	predicateBuilder := classifier.NewPredicateBuilder()
	pred := predicateBuilder.And(
		predicateBuilder.ObjectsCountOfKind("ZookeeperCluster", 1),
		predicateBuilder.ObjectsCountOfKind("PersistentVolumeClaim", 3),
	)

	// groups them by "shape"
	groupedBySignature := classifier.GroupBySignature(result.ConvergedStates)
	historiesBySignature := make(map[string][]tracecheck.ExecutionHistory)
	for sig, states := range groupedBySignature {
		fmt.Printf("signature: %s, number of states: %d\n", sig, len(states))
		historiesForSignature := make([]tracecheck.ExecutionHistory, 0)
		for _, state := range states {
			executions := state.State.ExecutionHistory
			historiesForSignature = append(historiesForSignature, executions)
		}
		historiesBySignature[sig] = historiesForSignature
	}

	for sig, histories := range historiesBySignature {
		fmt.Println("signature: ", sig)
		fmt.Println("number of histories: ", len(histories))
	}

	classified := classifier.ClassifyResults(result.ConvergedStates, pred)

	byClassificaition := lo.GroupBy(classified, func(c tracecheck.ClassifiedState) string {
		return c.Classification
	})

	for classification, states := range byClassificaition {
		signatures := lo.Map(states, func(c tracecheck.ClassifiedState, _ int) string {
			return c.Signature
		})
		uniqueSignatures := lo.Uniq(signatures)
		fmt.Printf("%s: %v\n", classification, uniqueSignatures)
	}

	for _, state := range classified {
		if strings.HasPrefix(state.Signature, "8meo") {
			fmt.Println("State Signature: ", state.Signature)
			fmt.Printf("%v\n", state.State.State.Contents.All())
		}
	}

	resultWriter := tracecheck.NewResultWriter(emitter)
	resultWriter.MaterializeClassified(classified, *outDir)
}
