package tracegen // Or a more general package name like 'clientmetrics'

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	clientRequestsTotal   *prometheus.CounterVec
	clientRequestDuration *prometheus.HistogramVec
)

func init() {
	clientRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "controller_client_requests_total",
			Help: "Total number of controller-runtime client operations.",
		},
		[]string{"reconciler", "operation", "kind", "namespace", "error"}, // Labels
	)
	clientRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "controller_client_request_duration_seconds",
			Help:    "Latency of controller-runtime client operations.",
			Buckets: prometheus.DefBuckets, // Default buckets
		},
		[]string{"reconciler", "operation", "kind", "namespace", "error"}, // Labels
	)
	metrics.Registry.MustRegister(clientRequestsTotal)
	metrics.Registry.MustRegister(clientRequestDuration)
}

// --- Helper function for metric recording ---
func recordMetrics(reconcilerID, operation string, gvk schema.GroupVersionKind, namespace string, start time.Time, err error) {
	duration := time.Since(start).Seconds()
	errorLabel := fmt.Sprintf("%t", err != nil) // "true" or "false"

	kindLabel := gvk.Kind
	if kindLabel == "" {
		kindLabel = "unknown"
	}
	nsLabel := namespace
	if nsLabel == "" {
		nsLabel = "cluster"
	}

	clientRequestDuration.WithLabelValues(reconcilerID, operation, kindLabel, nsLabel, errorLabel).Observe(duration)
	clientRequestsTotal.WithLabelValues(reconcilerID, operation, kindLabel, nsLabel, errorLabel).Inc()
}

// --- MetricsWrapper Definition ---

// MetricsWrapper wraps a controller-runtime client to record Prometheus metrics.
type MetricsWrapper struct {
	client.Client        // Embed the original client interface
	reconcilerID  string // Identifier for the controller using this client
}

// Ensure MetricsWrapper implements client.Client
var _ client.Client = (*MetricsWrapper)(nil)

// NewMetricsWrapper creates a new metrics-instrumented client wrapper.
func NewMetricsWrapper(wrapped client.Client, reconcilerID string) *MetricsWrapper {
	if reconcilerID == "" {
		reconcilerID = "unknown" // Avoid empty label
	}
	return &MetricsWrapper{
		Client:       wrapped,
		reconcilerID: reconcilerID,
	}
}

// --- Instrumented Methods ---

func (mw *MetricsWrapper) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) (err error) {
	start := time.Now()
	defer func() {
		gvk := obj.GetObjectKind().GroupVersionKind()
		ns := obj.GetNamespace()
		recordMetrics(mw.reconcilerID, "GET", gvk, ns, start, err)
	}()
	err = mw.Client.Get(ctx, key, obj, opts...)
	return err
}

func (mw *MetricsWrapper) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) (err error) {
	start := time.Now()
	gvk := list.GetObjectKind().GroupVersionKind()
	ns := "" // Namespace is hard to determine reliably from List args
	defer func() {
		recordMetrics(mw.reconcilerID, "LIST", gvk, ns, start, err)
	}()
	err = mw.Client.List(ctx, list, opts...)
	return err
}

func (mw *MetricsWrapper) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) (err error) {
	start := time.Now()
	defer func() {
		gvk := obj.GetObjectKind().GroupVersionKind()
		recordMetrics(mw.reconcilerID, "CREATE", gvk, obj.GetNamespace(), start, err)
	}()
	err = mw.Client.Create(ctx, obj, opts...)
	return err
}

func (mw *MetricsWrapper) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) (err error) {
	start := time.Now()
	gvk := obj.GetObjectKind().GroupVersionKind()
	ns := obj.GetNamespace()
	defer func() {
		recordMetrics(mw.reconcilerID, "DELETE", gvk, ns, start, err)
	}()
	err = mw.Client.Delete(ctx, obj, opts...)
	return err
}

func (mw *MetricsWrapper) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) (err error) {
	start := time.Now()
	gvk := obj.GetObjectKind().GroupVersionKind()
	ns := obj.GetNamespace()
	defer func() {
		recordMetrics(mw.reconcilerID, "UPDATE", gvk, ns, start, err)
	}()
	err = mw.Client.Update(ctx, obj, opts...)
	return err
}

func (mw *MetricsWrapper) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) (err error) {
	start := time.Now()
	gvk := obj.GetObjectKind().GroupVersionKind()
	ns := obj.GetNamespace()
	defer func() {
		recordMetrics(mw.reconcilerID, "PATCH", gvk, ns, start, err)
	}()
	err = mw.Client.Patch(ctx, obj, patch, opts...)
	return err
}

func (mw *MetricsWrapper) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) (err error) {
	start := time.Now()
	gvk := obj.GetObjectKind().GroupVersionKind()
	ns := obj.GetNamespace() // Namespace of the obj type being deleted from
	defer func() {
		recordMetrics(mw.reconcilerID, "DELETE_ALL_OF", gvk, ns, start, err)
	}()
	err = mw.Client.DeleteAllOf(ctx, obj, opts...)
	return err
}

// --- Status Subresource ---

// metricsStatusWriter wraps the original StatusWriter to ensure status operations
// are routed through the MetricsWrapper's instrumented Update/Patch methods.
type metricsStatusWriter struct {
	client.SubResourceWriter
	parentClient *MetricsWrapper // Reference back to the metrics wrapper
}

func (sw *metricsStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	// Call the parent MetricsWrapper's Update method
	return sw.SubResourceWriter.Update(ctx, obj, opts...)
}

func (sw *metricsStatusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	// Call the parent MetricsWrapper's Patch method
	return sw.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}

// Status returns an instrumented StatusWriter.
func (mw *MetricsWrapper) Status() client.StatusWriter {
	originalStatusWriter := mw.Client.Status()
	return &metricsStatusWriter{
		SubResourceWriter: originalStatusWriter,
		parentClient:      mw,
	}
}

// --- Pass-Through Methods ---

func (mw *MetricsWrapper) Scheme() *runtime.Scheme {
	return mw.Client.Scheme()
}

func (mw *MetricsWrapper) RESTMapper() meta.RESTMapper {
	return mw.Client.RESTMapper()
}

func (mw *MetricsWrapper) GroupVersionKindFor(obj runtime.Object) (schema.GroupVersionKind, error) {
	return mw.Client.GroupVersionKindFor(obj)
}

func (mw *MetricsWrapper) IsObjectNamespaced(obj runtime.Object) (bool, error) {
	return mw.Client.IsObjectNamespaced(obj)
}
