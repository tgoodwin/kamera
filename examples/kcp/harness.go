package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"reflect"
	"unsafe"

	jsonpatch "github.com/evanphx/json-patch/v5"
	kcpcache "github.com/kcp-dev/apimachinery/v2/pkg/cache"
	kcpapiextfakeclient "github.com/kcp-dev/client-go/apiextensions/client/fake"
	kcpapiextinformers "github.com/kcp-dev/client-go/apiextensions/informers"
	kcpk8sinformers "github.com/kcp-dev/client-go/informers"
	kcpk8sfakeclient "github.com/kcp-dev/client-go/kubernetes/fake"
	kcptesting "github.com/kcp-dev/client-go/third_party/k8s.io/client-go/testing"
	"github.com/kcp-dev/kcp/pkg/indexers"
	apibinding "github.com/kcp-dev/kcp/pkg/reconciler/apis/apibinding"
	apibindingdeletion "github.com/kcp-dev/kcp/pkg/reconciler/apis/apibindingdeletion"
	apiexport "github.com/kcp-dev/kcp/pkg/reconciler/apis/apiexport"
	apiexportendpointslice "github.com/kcp-dev/kcp/pkg/reconciler/apis/apiexportendpointslice"
	apiexportendpointsliceurls "github.com/kcp-dev/kcp/pkg/reconciler/apis/apiexportendpointsliceurls"
	extraannotationsync "github.com/kcp-dev/kcp/pkg/reconciler/apis/extraannotationsync"
	logicalclustercleanup "github.com/kcp-dev/kcp/pkg/reconciler/apis/logicalclustercleanup"
	corelogicalcluster "github.com/kcp-dev/kcp/pkg/reconciler/core/logicalcluster"
	defaultapibinding "github.com/kcp-dev/kcp/pkg/reconciler/tenancy/defaultapibindinglifecycle"
	initialization "github.com/kcp-dev/kcp/pkg/reconciler/tenancy/initialization"
	"github.com/kcp-dev/logicalcluster/v3"
	kcpfakeclient "github.com/kcp-dev/sdk/client/clientset/versioned/cluster/fake"
	kcpinformers "github.com/kcp-dev/sdk/client/informers/externalversions"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema" //nolint:all
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/replay"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
)

type kcpStrategy struct {
	controllerID tracecheck.ReconcilerID
	scheme       *runtime.Scheme
	recorder     replay.EffectRecorder
}

type kcpHarness struct {
	scheme    *runtime.Scheme
	recorder  replay.EffectRecorder
	kcpClient *kcpfakeclient.ClusterClientset

	logicalClusterController             *corelogicalcluster.Controller
	apiBinderController                  *initialization.APIBinder
	defaultAPIBindingController          *defaultapibinding.DefaultAPIBindingController
	apiExportController                  unsafe.Pointer // apiexport.controller (unexported)
	logicalClusterCleanupController      unsafe.Pointer // logicalclustercleanup.controller (unexported)
	apiBindingReconciler                 unsafe.Pointer // apibinding.controller (unexported)
	apiBindingDeletionController         *apibindingdeletion.Controller
	extraAnnotationSyncController        unsafe.Pointer
	apiExportEndpointSliceController     unsafe.Pointer
	apiExportEndpointSliceURLsController unsafe.Pointer
}

type kcpHarnessContextKey struct{}

func newUpstreamKCPStrategy(controllerID tracecheck.ReconcilerID, scheme *runtime.Scheme, recorder replay.EffectRecorder) tracecheck.Strategy {
	return &kcpStrategy{
		controllerID: controllerID,
		scheme:       scheme,
		recorder:     recorder,
	}
}

func (s *kcpStrategy) PrepareState(ctx context.Context, state []runtime.Object) (context.Context, func(), error) {
	oldReader := rand.Reader
	rand.Reader = bytes.NewReader(bytes.Repeat([]byte{0x42}, 512))

	harness, stopInformers, err := newKCPHarness(s.scheme, s.recorder, state)
	if err != nil {
		rand.Reader = oldReader
		return nil, nil, err
	}

	ctx = context.WithValue(ctx, kcpHarnessContextKey{}, harness)
	return ctx, func() {
		stopInformers()
		rand.Reader = oldReader
	}, nil
}

func (s *kcpStrategy) ReconcileAtState(ctx context.Context, name types.NamespacedName) (reconcile.Result, error) {
	harness, err := harnessFromContext(ctx)
	if err != nil {
		return reconcile.Result{}, err
	}

	reactor := newKCPReactor(ctx, s.scheme, s.recorder, harness.kcpClient.Tracker())
	harness.kcpClient.PrependReactor("*", "*", reactor)

	key := kcpcache.ToClusterAwareKey(name.Namespace, "", name.Name)
	switch s.controllerID {
	case logicalClusterControllerID:
		requeue, err := logicalClusterProcess(harness.logicalClusterController, ctx, key)
		return reconcile.Result{Requeue: requeue}, err
	case apiBinderInitializerControllerID:
		return reconcile.Result{}, apiBinderProcess(harness.apiBinderController, ctx, key)
	case defaultAPIBindingLifecycleControllerID:
		return reconcile.Result{}, defaultAPIBindingProcess(harness.defaultAPIBindingController, ctx, key)
	case logicalClusterCleanupControllerID:
		return reconcile.Result{}, logicalClusterCleanupProcess(harness.logicalClusterCleanupController, ctx, key)
	case apiExportControllerID:
		return reconcile.Result{}, apiExportProcess(harness.apiExportController, ctx, key)
	case apiBindingReconcilerControllerID:
		requeue, err := apiBindingReconcilerProcess(harness.apiBindingReconciler, ctx, key)
		return reconcile.Result{Requeue: requeue}, err
	case apiBindingDeletionControllerID:
		return reconcile.Result{}, apiBindingDeletionProcess(harness.apiBindingDeletionController, ctx, key)
	case apiBindingAnnotationSyncControllerID:
		return reconcile.Result{}, extraAnnotationSyncProcess(harness.extraAnnotationSyncController, ctx, key)
	case apiExportEndpointSliceControllerID:
		return reconcile.Result{}, apiExportEndpointSliceProcess(harness.apiExportEndpointSliceController, ctx, key)
	case apiExportEndpointSliceURLsControllerID:
		requeue, err := apiExportEndpointSliceURLsProcess(harness.apiExportEndpointSliceURLsController, ctx, key)
		return reconcile.Result{Requeue: requeue}, err
	default:
		return reconcile.Result{}, fmt.Errorf("unsupported controller %q", s.controllerID)
	}
}

func newKCPHarness(scheme *runtime.Scheme, recorder replay.EffectRecorder, state []runtime.Object) (*kcpHarness, func(), error) {
	kcpObjects, err := splitObjectsForClients(scheme, state)
	if err != nil {
		return nil, nil, err
	}

	h := &kcpHarness{
		scheme:    scheme,
		recorder:  recorder,
		kcpClient: kcpfakeclient.NewClientset(kcpObjects...),
	}

	kcpFactory := kcpinformers.NewSharedInformerFactory(h.kcpClient, 0)

	logicalClusterInformer := kcpFactory.Core().V1alpha1().LogicalClusters()
	shardInformer := kcpFactory.Core().V1alpha1().Shards()
	workspaceTypeInformer := kcpFactory.Tenancy().V1alpha1().WorkspaceTypes()
	apiBindingInformer := kcpFactory.Apis().V1alpha2().APIBindings()
	apiExportInformer := kcpFactory.Apis().V1alpha2().APIExports()
	apiExportEndpointSliceInformer := kcpFactory.Apis().V1alpha1().APIExportEndpointSlices()
	partitionInformer := kcpFactory.Topology().V1alpha1().Partitions()

	// Install cluster index on ALL informers — required for Cluster(name).Lister().Get()
	// which is used by every controller's process() to fetch its primary resource.
	for _, inf := range []cache.SharedIndexInformer{
		logicalClusterInformer.Informer(),
		shardInformer.Informer(),
		workspaceTypeInformer.Informer(),
		apiBindingInformer.Informer(),
		apiExportInformer.Informer(),
		apiExportEndpointSliceInformer.Informer(),
		partitionInformer.Informer(),
	} {
		indexers.AddIfNotPresentOrDie(inf.GetIndexer(), cache.Indexers{
			kcpcache.ClusterIndexName:            kcpcache.ClusterIndexFunc,
			indexers.ByLogicalClusterPathAndName: indexers.IndexByLogicalClusterPathAndName,
		})
	}
	// Additional indexers for specific controllers
	indexers.AddIfNotPresentOrDie(apiBindingInformer.Informer().GetIndexer(), cache.Indexers{
		indexers.APIBindingsByAPIExport: indexers.IndexAPIBindingByAPIExport,
	})
	// Use controller-exported InstallIndexers for the endpoint slice controllers
	apiexportendpointslice.InstallIndexers(apiExportInformer, apiExportEndpointSliceInformer)
	apiexportendpointsliceurls.InstallIndexers(apiExportEndpointSliceInformer, apiExportEndpointSliceInformer, apiBindingInformer)

	// Region 2: API Binding controller (CRD creation)
	crdClient := kcpapiextfakeclient.NewClientset()
	crdFactory := kcpapiextinformers.NewSharedInformerFactory(crdClient, 0)
	crdInformer := crdFactory.Apiextensions().V1().CustomResourceDefinitions()
	indexers.AddIfNotPresentOrDie(crdInformer.Informer().GetIndexer(), cache.Indexers{
		kcpcache.ClusterIndexName:            kcpcache.ClusterIndexFunc,
		indexers.ByLogicalClusterPathAndName: indexers.IndexByLogicalClusterPathAndName,
	})
	apiResourceSchemaInformer := kcpFactory.Apis().V1alpha1().APIResourceSchemas()
	apiConversionInformer := kcpFactory.Apis().V1alpha1().APIConversions()
	indexers.AddIfNotPresentOrDie(apiResourceSchemaInformer.Informer().GetIndexer(), cache.Indexers{
		kcpcache.ClusterIndexName:            kcpcache.ClusterIndexFunc,
		indexers.ByLogicalClusterPathAndName: indexers.IndexByLogicalClusterPathAndName,
	})
	indexers.AddIfNotPresentOrDie(apiConversionInformer.Informer().GetIndexer(), cache.Indexers{
		kcpcache.ClusterIndexName:            kcpcache.ClusterIndexFunc,
		indexers.ByLogicalClusterPathAndName: indexers.IndexByLogicalClusterPathAndName,
	})
	apibinding.InstallIndexers(apiBindingInformer, apiExportInformer, apiExportInformer)
	apiexport.InstallIndexers(apiExportInformer)

	// Region 2/3: APIExport controller needs k8s client for Namespace/Secret
	k8sClient := kcpk8sfakeclient.NewSimpleClientset()
	k8sFactory := kcpk8sinformers.NewSharedInformerFactory(k8sClient, 0)
	namespaceInformer := k8sFactory.Core().V1().Namespaces()
	secretInformer := k8sFactory.Core().V1().Secrets()
	indexers.AddIfNotPresentOrDie(namespaceInformer.Informer().GetIndexer(), cache.Indexers{
		kcpcache.ClusterIndexName: kcpcache.ClusterIndexFunc,
	})
	indexers.AddIfNotPresentOrDie(secretInformer.Informer().GetIndexer(), cache.Indexers{
		kcpcache.ClusterIndexName: kcpcache.ClusterIndexFunc,
	})
	k8sClient.PrependWatchReactor("*", newBookmarkWatchReactor(map[schema.GroupVersionResource]func() runtime.Object{
		{Group: "", Version: "v1", Resource: "namespaces"}: func() runtime.Object {
			return bookmarkObj(&corev1.Namespace{})
		},
		{Group: "", Version: "v1", Resource: "secrets"}: func() runtime.Object {
			return bookmarkObj(&corev1.Secret{})
		},
	}))

	// KCP's custom reflector requires a typed BOOKMARK watch event for initial sync.
	crdClient.PrependWatchReactor("*", newBookmarkWatchReactor(map[schema.GroupVersionResource]func() runtime.Object{
		{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}: func() runtime.Object {
			return bookmarkObj(&apiextensionsv1.CustomResourceDefinition{})
		},
	}))
	// SMD bypass for CRD client
	crdClient.PrependReactor("create", "*", func(action kcptesting.Action) (bool, runtime.Object, error) {
		createAction := action.(kcptesting.CreateAction)
		obj := createAction.GetObject()
		if err := crdClient.Tracker().Cluster(action.GetCluster()).Create(
			createAction.GetResource(), obj, createAction.GetNamespace(),
		); err != nil {
			if addErr := crdClient.Tracker().Cluster(action.GetCluster()).Add(obj); addErr != nil {
				return true, nil, err
			}
		}
		return true, obj, nil
	})

	// Handle Create/Update actions that fail SMD type conversion by falling back to
	// simple tracker operations without managed fields tracking. The fake client's
	// NewFieldManagedObjectTracker requires OpenAPI schemas for all types, but KCP's
	// custom types (APIBinding, etc.) don't have schemas registered in the fake.
	h.kcpClient.PrependReactor("create", "*", func(action kcptesting.Action) (bool, runtime.Object, error) {
		createAction := action.(kcptesting.CreateAction)
		obj := createAction.GetObject()
		if err := h.kcpClient.Tracker().Cluster(action.GetCluster()).Create(
			createAction.GetResource(), obj, createAction.GetNamespace(),
		); err != nil {
			// The tracker's Create with managed fields may fail for custom types.
			// Fall back to Add which bypasses managed fields.
			if addErr := h.kcpClient.Tracker().Cluster(action.GetCluster()).Add(obj); addErr != nil {
				return true, nil, err // return original error
			}
		}
		return true, obj, nil
	})
	h.kcpClient.PrependReactor("update", "*", func(action kcptesting.Action) (bool, runtime.Object, error) {
		updateAction := action.(kcptesting.UpdateAction)
		obj := updateAction.GetObject()
		err := h.kcpClient.Tracker().Cluster(action.GetCluster()).Update(
			updateAction.GetResource(), obj, updateAction.GetNamespace(),
		)
		if err != nil {
			return true, nil, err
		}
		return true, obj, nil
	})

	// Handle ApplyPatchType (SSA) which the fake client's tracker doesn't support natively.
	// This reactor intercepts apply patches, performs a merge, and updates the tracker.
	h.kcpClient.PrependReactor("patch", "*", func(action kcptesting.Action) (bool, runtime.Object, error) {
		patchAction, ok := action.(kcptesting.PatchAction)
		if !ok || patchAction.GetPatchType() != types.ApplyPatchType {
			return false, nil, nil
		}
		obj, err := h.kcpClient.Tracker().Cluster(action.GetCluster()).Get(
			patchAction.GetResource(), patchAction.GetNamespace(), patchAction.GetName(),
		)
		if err != nil {
			return true, nil, err
		}
		patched, err := applyPatch(obj, patchAction.GetPatch(), types.MergePatchType)
		if err != nil {
			return true, nil, err
		}
		if err := h.kcpClient.Tracker().Cluster(action.GetCluster()).Update(
			patchAction.GetResource(), patched, patchAction.GetNamespace(),
		); err != nil {
			return true, nil, err
		}
		return true, patched, nil
	})

	stopCh := make(chan struct{})
	crdFactory.Start(stopCh)
	k8sFactory.Start(stopCh)
	kcpFactory.Start(stopCh)
	if err := ensureAllSynced(crdFactory.WaitForCacheSync(stopCh), k8sFactory.WaitForCacheSync(stopCh), kcpFactory.WaitForCacheSync(stopCh)); err != nil {
		close(stopCh)
		return nil, nil, err
	}

	logicalClusterController, err := corelogicalcluster.NewController(func() string { return rootShardExternal }, h.kcpClient, logicalClusterInformer)
	if err != nil {
		close(stopCh)
		return nil, nil, err
	}
	// In this single-shard harness, local and global informers are the same.
	apiBinderController, err := initialization.NewAPIBinder(
		h.kcpClient,
		logicalClusterInformer,
		workspaceTypeInformer, workspaceTypeInformer, // local + global
		apiBindingInformer,
		apiExportInformer, apiExportInformer, // local + global
	)
	if err != nil {
		close(stopCh)
		return nil, nil, err
	}

	defaultAPIBindingCtrl, err := defaultapibinding.NewDefaultAPIBindingController(
		h.kcpClient,
		logicalClusterInformer,
		workspaceTypeInformer, workspaceTypeInformer, // local + global
		apiBindingInformer,
		apiExportInformer, apiExportInformer, // local + global
	)
	if err != nil {
		close(stopCh)
		return nil, nil, err
	}
	extraAnnotationCtrl, err := extraannotationsync.NewController(
		h.kcpClient, apiExportInformer, apiBindingInformer,
	)
	if err != nil {
		close(stopCh)
		return nil, nil, err
	}
	apiExportEndpointSliceCtrl, err := apiexportendpointslice.NewController(
		apiExportEndpointSliceInformer, apiExportInformer,
		partitionInformer, h.kcpClient,
	)
	if err != nil {
		close(stopCh)
		return nil, nil, err
	}
	apiExportEndpointSliceURLsCtrl, err := apiexportendpointsliceurls.NewController(
		rootShardName,
		apiExportEndpointSliceInformer, apiBindingInformer,
		apiExportEndpointSliceInformer, shardInformer,
		apiExportInformer, h.kcpClient,
	)
	if err != nil {
		close(stopCh)
		return nil, nil, err
	}

	apiBindingDeletionCtrl := apibindingdeletion.NewController(
		&stubMetadataClient{},
		h.kcpClient,
		apiBindingInformer,
	)

	h.logicalClusterController = logicalClusterController
	apiBindingReconcilerCtrl, err := apibinding.NewController(
		crdClient,
		h.kcpClient,
		apiBindingInformer,
		apiExportInformer,
		apiResourceSchemaInformer,
		apiConversionInformer,
		logicalClusterInformer,
		apiExportInformer,         // global (same in single-shard)
		apiResourceSchemaInformer, // global (same)
		apiConversionInformer,     // global (same)
		crdInformer,
	)
	if err != nil {
		close(stopCh)
		return nil, nil, err
	}

	apiExportCtrl, err := apiexport.NewController(
		h.kcpClient,
		apiExportInformer,
		apiExportEndpointSliceInformer,
		shardInformer,
		k8sClient,
		namespaceInformer,
		secretInformer,
	)
	if err != nil {
		close(stopCh)
		return nil, nil, err
	}

	logicalClusterCleanupCtrl, err := logicalclustercleanup.NewController(
		h.kcpClient,
		logicalClusterInformer,
		crdInformer,
		apiBindingInformer,
	)
	if err != nil {
		close(stopCh)
		return nil, nil, err
	}

	h.apiBinderController = apiBinderController
	h.apiExportController = pointerTo(apiExportCtrl)
	h.logicalClusterCleanupController = pointerTo(logicalClusterCleanupCtrl)
	h.apiBindingReconciler = pointerTo(apiBindingReconcilerCtrl)
	h.apiBindingDeletionController = apiBindingDeletionCtrl
	h.defaultAPIBindingController = defaultAPIBindingCtrl
	h.extraAnnotationSyncController = pointerTo(extraAnnotationCtrl)
	h.apiExportEndpointSliceController = pointerTo(apiExportEndpointSliceCtrl)
	h.apiExportEndpointSliceURLsController = pointerTo(apiExportEndpointSliceURLsCtrl)

	return h, func() { close(stopCh) }, nil
}

func splitObjectsForClients(scheme *runtime.Scheme, state []runtime.Object) ([]runtime.Object, error) {
	kcpObjects := make([]runtime.Object, 0, len(state))
	for idx, obj := range state {
		typed, err := restoreTypedObject(scheme, obj)
		if err != nil {
			return nil, fmt.Errorf("convert state object %d: %w", idx, err)
		}
		kcpObjects = append(kcpObjects, typed)
	}
	return kcpObjects, nil
}

func restoreTypedObject(scheme *runtime.Scheme, obj runtime.Object) (client.Object, error) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		co, ok := obj.(client.Object)
		if !ok {
			return nil, fmt.Errorf("state object %T is not a client.Object", obj)
		}
		return co.DeepCopyObject().(client.Object), nil
	}

	copy := u.DeepCopy()
	cluster := traceClusterFor(copy)
	if needsClusterRequestKey(copy.GroupVersionKind()) {
		copy.SetNamespace("")
	}
	anns := copy.GetAnnotations()
	delete(anns, traceClusterNSKey)
	delete(anns, traceActualNSKey)
	copy.SetAnnotations(anns)

	typed, err := scheme.New(copy.GroupVersionKind())
	if err != nil {
		return nil, err
	}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(copy.Object, typed); err != nil {
		return nil, err
	}
	co, ok := typed.(client.Object)
	if !ok {
		return nil, fmt.Errorf("decoded %T is not a client.Object", typed)
	}

	if cluster != "" {
		if co.GetAnnotations() == nil {
			co.SetAnnotations(map[string]string{})
		}
		if co.GetAnnotations()[logicalcluster.AnnotationKey] == "" {
			co.GetAnnotations()[logicalcluster.AnnotationKey] = cluster
		}
	}
	co.GetObjectKind().SetGroupVersionKind(copy.GroupVersionKind())
	return co, nil
}

func newKCPReactor(ctx context.Context, scheme *runtime.Scheme, recorder replay.EffectRecorder, trackers ...kcptesting.ObjectTracker) kcptesting.ReactionFunc {
	return func(action kcptesting.Action) (bool, runtime.Object, error) {
		obj, op, err := objectForAction(action, trackers...)
		if err != nil || obj == nil || op == "" {
			return false, nil, nil
		}

		co, ok := obj.(client.Object)
		if !ok {
			return false, nil, nil
		}

		recorded, err := toRecordedObject(scheme, co, action.GetCluster())
		if err == nil {
			recorder.RecordEffect(ctx, recorded, op, nil, nil)
		}
		return false, nil, nil
	}
}

func objectForAction(action kcptesting.Action, trackers ...kcptesting.ObjectTracker) (runtime.Object, event.OperationType, error) {
	switch action.GetVerb() {
	case "create":
		createAction := action.(kcptesting.CreateAction)
		obj := createAction.GetObject()
		mutateServerSideObject(action.GetCluster(), obj)
		return obj, event.CREATE, nil
	case "update":
		updateAction := action.(kcptesting.UpdateAction)
		obj := updateAction.GetObject()
		mutateServerSideObject(action.GetCluster(), obj)
		return obj, event.UPDATE, nil
	case "patch":
		patchAction := action.(kcptesting.PatchAction)
		obj, err := lookupTrackedObject(trackers, action.GetCluster(), patchAction.GetResource(), patchAction.GetNamespace(), patchAction.GetName())
		if err != nil {
			return nil, "", err
		}
		patched, err := applyPatch(obj, patchAction.GetPatch(), patchAction.GetPatchType())
		if err != nil {
			return nil, "", err
		}
		mutateServerSideObject(action.GetCluster(), patched)
		return patched, event.PATCH, nil
	case "delete":
		deleteAction := action.(kcptesting.DeleteAction)
		obj, err := lookupTrackedObject(trackers, action.GetCluster(), deleteAction.GetResource(), deleteAction.GetNamespace(), deleteAction.GetName())
		if err != nil {
			return nil, "", err
		}
		return obj, event.MARK_FOR_DELETION, nil
	case "updatesubresource":
		updateAction, ok := action.(interface {
			GetSubresource() string
			GetObject() runtime.Object
		})
		if !ok || updateAction.GetSubresource() != "status" {
			return nil, "", nil
		}
		obj := updateAction.GetObject()
		mutateServerSideObject(action.GetCluster(), obj)
		return obj, event.UPDATE, nil
	default:
		return nil, "", nil
	}
}

func lookupTrackedObject(trackers []kcptesting.ObjectTracker, cluster logicalcluster.Path, gvr schema.GroupVersionResource, namespace, name string) (runtime.Object, error) {
	for _, tracker := range trackers {
		obj, err := tracker.Cluster(cluster).Get(gvr, namespace, name)
		if err == nil {
			return obj, nil
		}
		if apierrors.IsNotFound(err) {
			continue
		}
		return nil, err
	}
	return nil, apierrors.NewNotFound(gvr.GroupResource(), name)
}

func mutateServerSideObject(cluster logicalcluster.Path, obj runtime.Object) {
	co, ok := obj.(client.Object)
	if !ok || co == nil {
		return
	}
	if co.GetAnnotations() == nil {
		co.SetAnnotations(map[string]string{})
	}
	if co.GetAnnotations()[logicalcluster.AnnotationKey] == "" && cluster.String() != "" {
		co.GetAnnotations()[logicalcluster.AnnotationKey] = cluster.String()
	}
}

func toRecordedObject(scheme *runtime.Scheme, obj client.Object, cluster logicalcluster.Path) (client.Object, error) {
	copy, ok := obj.DeepCopyObject().(client.Object)
	if !ok {
		return nil, fmt.Errorf("copy %T is not client.Object", obj)
	}
	gvk := copy.GetObjectKind().GroupVersionKind()
	if gvk.Empty() {
		gvks, _, err := scheme.ObjectKinds(copy)
		if err == nil && len(gvks) > 0 {
			gvk = gvks[0]
			copy.GetObjectKind().SetGroupVersionKind(gvk)
		}
	}

	anns := copy.GetAnnotations()
	if anns == nil {
		anns = map[string]string{}
	}
	if cluster.String() != "" {
		anns[traceClusterNSKey] = cluster.String()
	}
	copy.SetAnnotations(anns)
	if needsClusterRequestKey(gvk) && cluster.String() != "" {
		copy.SetNamespace(cluster.String())
	}
	return copy, nil
}

func applyPatch(obj runtime.Object, patch []byte, patchType types.PatchType) (runtime.Object, error) {
	original, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}

	var modified []byte
	switch patchType {
	case types.JSONPatchType:
		decoded, err := jsonpatch.DecodePatch(patch)
		if err != nil {
			return nil, err
		}
		modified, err = decoded.Apply(original)
		if err != nil {
			return nil, err
		}
	default:
		modified, err = jsonpatch.MergePatch(original, patch)
		if err != nil {
			return nil, err
		}
	}

	targetType := reflect.TypeOf(obj)
	if targetType.Kind() != reflect.Pointer {
		return nil, fmt.Errorf("patched object %T is not a pointer", obj)
	}
	target, ok := reflect.New(targetType.Elem()).Interface().(runtime.Object)
	if !ok {
		return nil, fmt.Errorf("patched object %T is not a runtime.Object", obj)
	}
	if err := json.Unmarshal(modified, target); err != nil {
		return nil, err
	}
	target.GetObjectKind().SetGroupVersionKind(obj.GetObjectKind().GroupVersionKind())
	return target, nil
}

func ensureAllSynced(syncMaps ...map[reflect.Type]bool) error {
	for _, syncMap := range syncMaps {
		for typ, synced := range syncMap {
			if !synced {
				return fmt.Errorf("informer %v did not sync", typ)
			}
		}
	}
	return nil
}

func harnessFromContext(ctx context.Context) (*kcpHarness, error) {
	h, ok := ctx.Value(kcpHarnessContextKey{}).(*kcpHarness)
	if !ok || h == nil {
		return nil, fmt.Errorf("kcp harness missing from context")
	}
	return h, nil
}

func pointerTo(v any) unsafe.Pointer {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return nil
	}
	return unsafe.Pointer(rv.Pointer())
}
