package kamera

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/go-logr/logr"
	"go.uber.org/zap"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	dynamicfakeclient "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
	testing "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
	filteredinformerfactory "knative.dev/pkg/client/injection/kube/informers/factory/filtered"
	"knative.dev/pkg/injection"
	dynamicclient "knative.dev/pkg/injection/clients/dynamicclient"
	dynamicfake "knative.dev/pkg/injection/clients/dynamicclient/fake"
	"knative.dev/pkg/reconciler"
	reconcilertesting "knative.dev/pkg/reconciler/testing"

	"reflect"

	kamerascheme "github.com/tgoodwin/kamera/examples/knative-serving/knative/scheme"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/replay"
	"github.com/tgoodwin/kamera/pkg/simclock"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/tgoodwin/kamera/pkg/tag"
	cachingv1alpha1 "knative.dev/caching/pkg/apis/caching/v1alpha1"
	fakecachingclient "knative.dev/caching/pkg/client/injection/client/fake"
	netv1alpha1 "knative.dev/networking/pkg/apis/networking/v1alpha1"
	fakenetworkingclient "knative.dev/networking/pkg/client/injection/client/fake"
	fakekubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	v1 "knative.dev/serving/pkg/apis/serving/v1"
	autoscalercfg "knative.dev/serving/pkg/autoscaler/config"
	"knative.dev/serving/pkg/autoscaler/scaling"
	fakeservingclient "knative.dev/serving/pkg/client/injection/client/fake"
	"knative.dev/serving/pkg/gc"
	"knative.dev/serving/pkg/reconciler/route/config"

	netcfg "knative.dev/networking/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/client"
	log "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	// Knative pkg imports
	// Knative controller plumbing
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/system"

	autoscalingv1alpha1 "knative.dev/serving/pkg/apis/autoscaling/v1alpha1"
	cfgmap "knative.dev/serving/pkg/apis/config"
	podscalableinformer "knative.dev/serving/pkg/client/injection/ducks/autoscaling/v1alpha1/podscalable"
	kparesources "knative.dev/serving/pkg/reconciler/autoscaling/kpa/resources"
)

// Ensure KnativeStrategy implements the Strategy interface
var _ tracecheck.Strategy = (*KnativeStrategy)(nil)

// ControllerFactory is a function that creates a new controller.
type ControllerFactory func(ctx context.Context, cmw configmap.Watcher) *controller.Impl

// KnativeStrategy implements the Strategy interface for Knative controllers.
type KnativeStrategy struct {
	factory   ControllerFactory
	recorder  replay.EffectRecorder
	selectors []string
	logger    logr.Logger
}

type fakeUniScaler struct {
	mu              sync.RWMutex
	desired         int32
	excessBC        int32
	activationScale int32
}

func newFakeUniScaler(decider *scaling.Decider) *fakeUniScaler {
	desired := decider.Spec.InitialScale
	// Store ActivationScale for use during activation
	activationScale := decider.Spec.ActivationScale
	return &fakeUniScaler{
		desired:         desired,
		excessBC:        0,
		activationScale: activationScale,
	}
}

func (f *fakeUniScaler) Scale(_ *zap.SugaredLogger, now time.Time) scaling.ScaleResult {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// For scale-to-zero simulation with no traffic: always return 0.
	// In real Knative, UniScaler uses metrics (concurrent requests) to determine desired scale.
	// For simulation with no traffic, we return 0 to trigger scale-down.
	//
	// Knative's KPA reconciler will:
	// 1. Apply bounds (min/max/InitialScale) in scale() before handleScaleToZero
	// 2. So if InitialScale=1, it will scale to 1 initially
	// 3. handleScaleToZero will enforce timing (60s stable window + 30s grace period)
	// 4. After timing is met, it will allow scaling to 0
	desired := int32(0)

	fmt.Printf("🔔 UNISCALER-SCALE: desired=%d, now=%v\n", desired, now)

	return scaling.ScaleResult{
		DesiredPodCount:     desired,
		ExcessBurstCapacity: f.excessBC,
		ScaleValid:          true,
	}
}

func (f *fakeUniScaler) Update(spec *scaling.DeciderSpec) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.desired = spec.InitialScale
	f.activationScale = spec.ActivationScale
}

// fakeDeciders wraps MultiScaler to ensure deciders are initialized with correct DesiredScale
// immediately, rather than waiting for the async ticker to update it.
type fakeDeciders struct {
	*scaling.MultiScaler
	uniScalerFactory func(*scaling.Decider) (scaling.UniScaler, error)
	logger           *zap.SugaredLogger
}

// Get overrides MultiScaler.Get to add logging
func (f *fakeDeciders) Get(ctx context.Context, namespace, name string) (*scaling.Decider, error) {
	fmt.Printf("🔔 FAKE-GET: decider=%s/%s\n", namespace, name)
	result, err := f.MultiScaler.Get(ctx, namespace, name)
	if err != nil {
		fmt.Printf("🔔 FAKE-GET-NOTFOUND: decider=%s/%s, err=%v\n", namespace, name, err)
	} else {
		fmt.Printf("🔔 FAKE-GET-FOUND: decider=%s/%s\n", namespace, name)
	}
	return result, err
}

// Create overrides MultiScaler.Create to immediately compute and set DesiredScale
// and register synchronous callbacks for tickers
func (f *fakeDeciders) Create(ctx context.Context, decider *scaling.Decider) (*scaling.Decider, error) {
	fmt.Printf("🔔 FAKE-CREATE: decider=%s/%s\n", decider.Namespace, decider.Name)

	// Create the decider using the underlying MultiScaler
	// This will call createScaler -> runScalerTicker, which creates a ticker
	result, err := f.MultiScaler.Create(ctx, decider)
	if err != nil {
		fmt.Printf("🔔 FAKE-CREATE-ERROR: decider=%s/%s, err=%v\n", decider.Namespace, decider.Name, err)
		return nil, err
	}
	fmt.Printf("🔔 FAKE-CREATE-SUCCESS: decider=%s/%s\n", decider.Namespace, decider.Name)

	// After Create, a ticker should have been created. We need to find it and register a callback.
	// Since we can't easily access the ticker that was created, we'll use a different approach:
	// We'll register callbacks for all tickers that match the interval (2s for KPA).
	// Actually, this is getting complex. Let's try a simpler approach: make the channel read
	// happen by ensuring the goroutine is running. But we can't ensure that.

	// Register a synchronous callback for the ticker that was just created.
	// The ticker should be the most recent one created by runScalerTicker.
	key := types.NamespacedName{Namespace: decider.Namespace, Name: decider.Name}
	registerTickerCallbackForDecider(f.MultiScaler, key)

	// Immediately compute the scale to set DesiredScale correctly
	// This simulates the ticker running immediately in our simulation context
	uniScaler, err := f.uniScalerFactory(result)
	if err != nil {
		return result, err
	}

	scaleResult := uniScaler.Scale(f.logger, time.Now())
	if scaleResult.ScaleValid {
		// Update the decider with the computed scale
		result.Status.DesiredScale = scaleResult.DesiredPodCount
		result.Status.ExcessBurstCapacity = scaleResult.ExcessBurstCapacity
		// Update the decider in the MultiScaler
		_, err = f.MultiScaler.Update(ctx, result)
		if err != nil {
			return result, err
		}
		// Return the updated decider
		return f.MultiScaler.Get(ctx, result.Namespace, result.Name)
	}

	return result, nil
}

// persistentStopCh is a channel that never closes, allowing tickers to persist across reconcile steps
var persistentStopCh = make(chan struct{})

// persistentMultiScaler is a singleton MultiScaler that persists across reconcile steps.
// This is necessary because controllers are recreated per reconcile step, but we need
// the MultiScaler (and its tickers) to persist so tickers can fire across steps.
var (
	persistentMultiScalerMu sync.Mutex
	persistentMultiScaler   kparesources.Deciders

	// persistentEnqueueWrapper is a singleton wrapper that persists across reconcile steps.
	// This ensures Watch() is only called once on the underlying MultiScaler.
	persistentEnqueueWrapper *enqueueCapturingDeciders

	// persistentDynamicClient tracks the persistent dynamic client that should be reused
	// across reconcile steps to ensure reactors are triggered.
	persistentDynamicClientMu           sync.Mutex
	persistentDynamicClient             *dynamicfakeclient.FakeDynamicClient
	persistentDynamicClientReactorAdded bool

	// pendingScaleChanges tracks deployment scale changes from KPA that need to be
	// propagated to the simulation's snapshot store.
	pendingScaleChangesMu sync.Mutex
	pendingScaleChanges   = make(map[types.NamespacedName]int32)
)

// RecordScaleChange records a pending scale change from the KPA dynamic client reactor.
// The KnativeStrategy will apply these changes to the simulation state after reconciliation.
func RecordScaleChange(namespace, name string, replicas int32) {
	pendingScaleChangesMu.Lock()
	defer pendingScaleChangesMu.Unlock()
	key := types.NamespacedName{Namespace: namespace, Name: name}
	pendingScaleChanges[key] = replicas
	fmt.Printf("🔔 SCALE-CHANGE-RECORDED: %s/%s -> replicas=%d\n", namespace, name, replicas)
}

// GetAndClearScaleChanges returns and clears all pending scale changes.
func GetAndClearScaleChanges() map[types.NamespacedName]int32 {
	pendingScaleChangesMu.Lock()
	defer pendingScaleChangesMu.Unlock()
	result := pendingScaleChanges
	pendingScaleChanges = make(map[types.NamespacedName]int32)
	return result
}

// NewFakeMultiScaler constructs a MultiScaler suitable for offline simulations.
// It wraps the MultiScaler to ensure deciders are initialized with correct DesiredScale
// immediately, rather than waiting for the async ticker. The fake UniScaler ensures that
// when InitialScale is 0, it returns at least 1 (or ActivationScale if set) to allow activation to proceed.
//
// Note: We use a persistent MultiScaler instance (singleton) instead of creating a new one
// each time, because controllers are recreated per reconcile step. This allows tickers to
// persist across reconcile steps and fire when depth advances.
func NewFakeMultiScaler(stopCh <-chan struct{}, logger *zap.SugaredLogger) kparesources.Deciders {
	persistentMultiScalerMu.Lock()
	defer persistentMultiScalerMu.Unlock()

	if persistentMultiScaler == nil {
		fmt.Printf("🔔 MULTISCALER-SINGLETON: Creating persistent MultiScaler\n")
		uniScalerFactory := func(decider *scaling.Decider) (scaling.UniScaler, error) {
			return newFakeUniScaler(decider), nil
		}
		// Use persistentStopCh instead of stopCh to allow tickers to persist across reconcile steps
		ms := scaling.NewMultiScaler(persistentStopCh, uniScalerFactory, logger)

		// Wrap the tickProvider to intercept ticker creation and register synchronous callbacks
		// We use reflection to access the private tickProvider field and replace it with our wrapper
		wrapMultiScalerTickerProvider(ms)

		persistentMultiScaler = &fakeDeciders{
			MultiScaler:      ms,
			uniScalerFactory: uniScalerFactory,
			logger:           logger,
		}
	} else {
		fmt.Printf("🔔 MULTISCALER-SINGLETON: Reusing existing MultiScaler\n")
	}

	return persistentMultiScaler
}

// mostRecentTicker tracks the most recently created ticker so we can register callbacks
var (
	mostRecentTicker   *simclock.Ticker
	mostRecentTickerMu sync.Mutex
)

// wrapMultiScalerTickerProvider uses unsafe to replace the MultiScaler's tickProvider
// with a wrapper that registers synchronous callbacks. When a ticker fires, we'll call
// tickScaler directly, which will then call Inform, triggering the Watch callback.
func wrapMultiScalerTickerProvider(ms *scaling.MultiScaler) {
	// Use reflection to find the tickProvider field offset
	msValue := reflect.ValueOf(ms).Elem()
	tickProviderField := msValue.FieldByName("tickProvider")
	if !tickProviderField.IsValid() {
		panic("MultiScaler.tickProvider field not found - reflection failed")
	}

	// Use unsafe to get a pointer to the field and set it
	// We need to get the address of the struct and add the field offset
	msPtr := unsafe.Pointer(ms)
	tickProviderPtr := unsafe.Pointer(uintptr(msPtr) + tickProviderField.UnsafeAddr() - msValue.UnsafeAddr())

	// Create a wrapper that tracks the most recent ticker
	wrappedProvider := func(d time.Duration) *simclock.Ticker {
		// Create the ticker directly (same as what the original tickProvider does)
		ticker := simclock.NewTicker(d)

		// Track the most recent ticker so we can register callbacks when Create is called
		mostRecentTickerMu.Lock()
		mostRecentTicker = ticker
		mostRecentTickerMu.Unlock()

		fmt.Printf("🔔 TICKER-PROVIDER-WRAP: Created ticker, interval=%v\n", d)
		return ticker
	}

	// Set the field using unsafe pointer
	*(*func(time.Duration) *simclock.Ticker)(tickProviderPtr) = wrappedProvider

	fmt.Printf("🔔 TICKER-PROVIDER-WRAP: Wrapped MultiScaler tickProvider\n")
}

// registerTickerCallbackForDecider registers a synchronous callback for the ticker
// associated with the given decider. When the ticker fires, it will call tickScaler
// synchronously, which will then call Inform, triggering the Watch callback.
func registerTickerCallbackForDecider(ms *scaling.MultiScaler, key types.NamespacedName) {
	// Get the most recent ticker (should be the one created for this decider)
	mostRecentTickerMu.Lock()
	ticker := mostRecentTicker
	mostRecentTickerMu.Unlock()

	if ticker == nil {
		fmt.Printf("🔔 CALLBACK-REGISTER: No recent ticker found for key=%s, skipping\n", key)
		return
	}

	// Use reflection to access the MultiScaler's internal scalers map
	msValue := reflect.ValueOf(ms).Elem()
	scalersField := msValue.FieldByName("scalers")
	if !scalersField.IsValid() {
		fmt.Printf("🔔 CALLBACK-REGISTER: scalers field not found, skipping callback registration\n")
		return
	}

	// Can't call .Interface() on unexported field, so use unsafe to get a pointer to it
	msPtr := unsafe.Pointer(ms)
	scalersPtr := unsafe.Pointer(uintptr(msPtr) + scalersField.UnsafeAddr() - msValue.UnsafeAddr())

	// Get the scaler for this key by using reflection on the unsafe pointer
	scalersMapValue := reflect.NewAt(scalersField.Type(), scalersPtr).Elem()
	scalerValue := scalersMapValue.MapIndex(reflect.ValueOf(key))
	if !scalerValue.IsValid() {
		fmt.Printf("🔔 CALLBACK-REGISTER: scaler not found for key=%s, skipping\n", key)
		return
	}

	// The scaler is a *scalerRunner. We need to access its fields.
	// Can't call .Elem() on unexported type, so we need to work with the reflect.Value directly
	// The scalerValue is a pointer to scalerRunner, so we need to dereference it
	if scalerValue.Kind() != reflect.Ptr {
		fmt.Printf("🔔 CALLBACK-REGISTER: scalerValue is not a pointer, got kind=%v\n", scalerValue.Kind())
		return
	}

	runnerValue := scalerValue.Elem()
	scalerField := runnerValue.FieldByName("scaler")
	if !scalerField.IsValid() {
		fmt.Printf("🔔 CALLBACK-REGISTER: scaler field not found in runner\n")
		return
	}

	// Can't call .Interface() on unexported field, so use unsafe
	runnerPtr := unsafe.Pointer(scalerValue.Pointer())
	scalerPtr := unsafe.Pointer(uintptr(runnerPtr) + scalerField.UnsafeAddr() - runnerValue.UnsafeAddr())
	scalerValuePtr := reflect.NewAt(scalerField.Type(), scalerPtr).Elem()

	// Get the scaler (UniScaler interface)
	scaler := scalerValuePtr.Interface().(scaling.UniScaler)

	// Use reflection to call the private tickScaler method
	// tickScaler signature: func (m *MultiScaler) tickScaler(scaler UniScaler, runner *scalerRunner, metricKey types.NamespacedName)
	tickScalerMethod := reflect.ValueOf(ms).MethodByName("tickScaler")
	if !tickScalerMethod.IsValid() {
		// tickScaler is private, so we can't call it via MethodByName
		// We need to use a different approach: call it via the unexported method
		// Actually, we can't call private methods via reflection easily.
		// Let's try a different approach: make Inform get called directly.

		// Actually, the simplest approach is to register a callback that calls Inform
		// when the ticker fires. But we need to know when to call Inform (when scale changes).
		// That requires calling tickScaler, which we can't do easily.

		// Let's try yet another approach: Register a callback that manually calls Scale
		// and then Inform if the scale changed. This mimics what tickScaler does.
		fmt.Printf("🔔 CALLBACK-REGISTER: tickScaler is private, using workaround\n")

		// Register a callback that calls Scale, updates decider status, and Inform
		callback := func() {
			now := simclock.Now()
			fmt.Printf("🔔 CALLBACK-TICK: key=%s, now=%v\n", key, now)

			// Call Scale (similar to what tickScaler does)
			scaleResult := scaler.Scale(nil, now) // logger is nil for now

			if !scaleResult.ScaleValid {
				fmt.Printf("🔔 CALLBACK-TICK: Scale result invalid for key=%s\n", key)
				return
			}

			// Get the current decider to update its status
			// We use a background context since this is called from a ticker callback
			ctx := context.Background()
			decider, err := ms.Get(ctx, key.Namespace, key.Name)
			if err != nil {
				fmt.Printf("🔔 CALLBACK-TICK: Failed to get decider for key=%s, err=%v\n", key, err)
				return
			}

			// Check if scale changed
			oldScale := decider.Status.DesiredScale
			newScale := scaleResult.DesiredPodCount
			scaleChanged := oldScale != newScale

			fmt.Printf("🔔 CALLBACK-TICK: decider=%s, oldScale=%d, newScale=%d, changed=%v\n", key, oldScale, newScale, scaleChanged)

			// Update decider status (like tickScaler does via updateLatestScale)
			decider.Status.DesiredScale = newScale
			decider.Status.ExcessBurstCapacity = scaleResult.ExcessBurstCapacity

			// Persist the update
			_, err = ms.Update(ctx, decider)
			if err != nil {
				fmt.Printf("🔔 CALLBACK-TICK: Failed to update decider for key=%s, err=%v\n", key, err)
				return
			}

			// Always inform to trigger reconcile, even if scale didn't change.
			// This ensures KPA keeps reconciling to check handleScaleToZero timing.
			// In real Knative, tickScaler calls Inform whenever scale changes OR when
			// ExcessBurstCapacity sign changes. For scale-to-zero, we need periodic
			// reconciles to check if enough time has passed.
			fmt.Printf("🔔 CALLBACK-TICK: Calling Inform for key=%s (scaleChanged=%v, oldScale=%d, newScale=%d)\n", key, scaleChanged, oldScale, newScale)
			ms.Inform(key)
		}

		simclock.RegisterTickerCallback(ticker, callback)
		fmt.Printf("🔔 CALLBACK-REGISTER: Registered callback for ticker, key=%s\n", key)
		return
	}

	// If we could call tickScaler, we would do:
	// tickScalerMethod.Call([]reflect.Value{
	//     reflect.ValueOf(scaler),
	//     scalerValue,
	//     reflect.ValueOf(key),
	// })
	// But since it's private, we can't do this easily.

	fmt.Printf("🔔 CALLBACK-REGISTER: Callback registration attempted for key=%s\n", key)
}

// enqueueCapturingDeciders wraps a Deciders implementation to capture Watch callback invocations
type enqueueCapturingDeciders struct {
	kparesources.Deciders
	reconcilerID string

	// watchRegistered tracks whether we've already called Watch() on the underlying MultiScaler.
	// Since MultiScaler.Watch() can only be called once, we need to make this idempotent.
	watchRegistered bool
	watchMu         sync.Mutex

	// currentCallback stores the current controller callback so we can call it when Inform is invoked
	currentCallback func(types.NamespacedName)
}

func (e *enqueueCapturingDeciders) Watch(callback func(types.NamespacedName)) {
	e.watchMu.Lock()
	defer e.watchMu.Unlock()

	// Store the current callback so we can call it when Inform is invoked
	e.currentCallback = callback

	fmt.Printf("🔔 WATCH-REGISTER: reconcilerID=%s, alreadyRegistered=%v\n", e.reconcilerID, e.watchRegistered)

	// Only call the underlying Watch() once, since MultiScaler doesn't support multiple calls
	if !e.watchRegistered {
		wrappedCallback := func(key types.NamespacedName) {
			fmt.Printf("🔔 WATCH-CALLBACK: reconcilerID=%s, key=%s\n", e.reconcilerID, key)

			// Call the current controller callback (impl.EnqueueKey) - this enqueues in the controller's workqueue
			e.watchMu.Lock()
			cb := e.currentCallback
			e.watchMu.Unlock()

			if cb != nil {
				cb(key)
			}

			// Add to the global async enqueue collector.
			// The collector is automatically cleared after each Get() call in determineNewPendingReconciles.
			collector := tracecheck.GetGlobalAsyncEnqueueCollector()
			fmt.Printf("🔔 WATCH-ADD: reconcilerID=%s, key=%s, adding to global collector\n", e.reconcilerID, key)
			collector.Add(e.reconcilerID, key)
		}
		e.Deciders.Watch(wrappedCallback)
		e.watchRegistered = true
		fmt.Printf("🔔 WATCH-REGISTER: Successfully registered Watch callback\n")
	} else {
		fmt.Printf("🔔 WATCH-REGISTER: Watch already registered, just updating callback reference\n")
	}
}

// NewEnqueueCapturingDeciders creates a Deciders wrapper that captures Watch callback invocations.
// Since the underlying MultiScaler is a singleton, we also use a singleton wrapper to ensure
// Watch() is only called once.
// The wrapper uses the global async enqueue collector, which is automatically cleared after each Get() call.
func NewEnqueueCapturingDeciders(base kparesources.Deciders, reconcilerID string) kparesources.Deciders {
	fmt.Printf("🔔 NEW-ENQUEUE-CAPTURING: reconcilerID=%s, base_type=%T\n", reconcilerID, base)

	persistentMultiScalerMu.Lock()
	defer persistentMultiScalerMu.Unlock()

	// If we already have a persistent wrapper, reuse it.
	// The context will be updated before SetDepth in takeReconcileStep.
	if persistentEnqueueWrapper != nil {
		fmt.Printf("🔔 NEW-ENQUEUE-CAPTURING: Reusing persistent wrapper\n")
		return persistentEnqueueWrapper
	}

	// Create a new wrapper and make it persistent
	wrapper := &enqueueCapturingDeciders{
		Deciders:        base,
		reconcilerID:    reconcilerID,
		watchRegistered: false,
	}
	persistentEnqueueWrapper = wrapper
	fmt.Printf("🔔 NEW-ENQUEUE-CAPTURING: Created new persistent wrapper\n")
	return wrapper
}

// Get forwards to the base Deciders implementation
func (e *enqueueCapturingDeciders) Get(ctx context.Context, namespace, name string) (*scaling.Decider, error) {
	fmt.Printf("🔔 ENQUEUE-GET: reconcilerID=%s, decider=%s/%s, base_type=%T\n", e.reconcilerID, namespace, name, e.Deciders)
	return e.Deciders.Get(ctx, namespace, name)
}

// Create forwards to the base Deciders implementation
func (e *enqueueCapturingDeciders) Create(ctx context.Context, decider *scaling.Decider) (*scaling.Decider, error) {
	fmt.Printf("🔔 ENQUEUE-CREATE: reconcilerID=%s, decider=%s/%s, base_type=%T\n", e.reconcilerID, decider.Namespace, decider.Name, e.Deciders)
	return e.Deciders.Create(ctx, decider)
}

// NewKnativeStrategy creates a new KnativeStrategy for a given controller factory.
func NewKnativeStrategy(factory ControllerFactory, recorder replay.EffectRecorder, selectors ...string) (*KnativeStrategy, error) {
	if factory == nil {
		return nil, fmt.Errorf("controller factory cannot be nil")
	}

	cmw := configmap.NewStaticWatcher(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cfgmap.FeaturesConfigName,
			Namespace: system.Namespace(),
		},
		Data: map[string]string{},
	}, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cfgmap.DefaultsConfigName,
			Namespace: system.Namespace(),
		},
		Data: map[string]string{},
	}, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      autoscalercfg.ConfigName,
			Namespace: system.Namespace(),
		},
		Data: map[string]string{
			// Shorten stable-window from default 60s to 6s (minimum allowed)
			// to speed up scale-to-zero in simulation
			"stable-window": "6s",
			// Also shorten scale-to-zero-grace-period from default 30s to 6s
			"scale-to-zero-grace-period": "6s",
			// Set TBC to -1 to skip activator probe (activator always in path)
			// This is needed because the simulation doesn't have network access
			"target-burst-capacity": "-1",
		},
	},
		// added the following for route reconciler
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      config.DomainConfigName,
				Namespace: system.Namespace(),
			},
			Data: map[string]string{
				"test-domain.dev": "",
				"prod-domain.com": "selector:\n  app: prod",
			},
		}, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      netcfg.ConfigMapName,
				Namespace: system.Namespace(),
			},
			Data: map[string]string{},
		}, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      gc.ConfigName,
				Namespace: system.Namespace(),
			},
			Data: map[string]string{},
		}, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      cfgmap.FeaturesConfigName,
				Namespace: system.Namespace(),
			},
			Data: map[string]string{},
		},
		// added for revision informer
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "config-observability",
				Namespace: system.Namespace(),
			},
			Data: map[string]string{},
		},
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "config-deployment",
				Namespace: system.Namespace(),
			},
			Data: map[string]string{
				"queue-sidecar-image": "gcr.io/knative-releases/knative.dev/serving/cmd/queue@sha256:abc123",
			},
		},
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "config-logging",
				Namespace: system.Namespace(),
			},
			Data: map[string]string{},
		},

		// added for certificate reconciler
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "config-certmanager",
				Namespace: system.Namespace(),
			},
			Data: map[string]string{},
		},
	)

	partial := func(ctx context.Context, _ configmap.Watcher) *controller.Impl {
		return factory(ctx, cmw)
	}

	return &KnativeStrategy{
		factory:   partial,
		selectors: selectors,
		recorder:  recorder,
		logger:    log.Log.WithName("knative-strategy"),
	}, nil
}

// SetLogger overrides the default logger used by the strategy. Call this before PrepareState.
func (ks *KnativeStrategy) SetLogger(logger logr.Logger) {
	if logger.GetSink() == nil {
		return
	}
	ks.logger = logger
}

// PrepareState sets up the fake clients and informers for the reconciler under test.
func (ks *KnativeStrategy) PrepareState(ctx context.Context, state []runtime.Object) (context.Context, func(), error) {
	ctx = log.IntoContext(ctx, ks.logger)
	ctx, cancel, err := setupClientState(ctx, state, ks.selectors...)
	if err != nil {
		return nil, cancel, err
	}
	ctx = log.IntoContext(ctx, ks.logger)
	// Provide a no-op event recorder to avoid spinning up the default broadcaster goroutine.
	ctx = controller.WithEventRecorder(ctx, record.NewFakeRecorder(100))
	return ctx, cancel, nil
}

// newReactor creates a new reactor function that intercepts client actions,
// records them as effects, and uses the provided trackers to fetch object states.
func newReactor(ctx context.Context, recorder replay.EffectRecorder, trackers ...testing.ObjectTracker) testing.ReactionFunc {
	baseLogger := log.FromContext(ctx).WithName("fake-reactor")

	return func(action testing.Action) (handled bool, ret runtime.Object, err error) {
		var obj runtime.Object
		var op event.OperationType
		resource := action.GetResource().Resource
		verb := action.GetVerb()
		logger := baseLogger.WithValues(
			"verb", verb,
			"resource", resource,
			"namespace", action.GetNamespace(),
		)

		// lookup iterates through all provided trackers to find the object.
		lookup := func(res schema.GroupVersionResource, ns, name string) (runtime.Object, error) {
			for _, tracker := range trackers {
				obj, err := tracker.Get(res, ns, name)
				if err == nil {
					return obj, nil
				}
			}
			// If we didn't find it in any tracker, return a generic error.
			return nil, fmt.Errorf("object %s/%s not found in any tracker for resource %v", ns, name, res)
		}

		switch action.GetVerb() {
		case "get":
			a := action.(testing.GetAction)
			obj, err = lookup(a.GetResource(), a.GetNamespace(), a.GetName())
			op = event.GET
		case "list":
			a := action.(testing.ListAction)
			gvr := a.GetResource()
			gvk := schema.GroupVersionKind{
				Group:   gvr.Group,
				Version: gvr.Version,
				Kind:    listKindForResource(gvr.Resource),
			}
			ul := &unstructured.Unstructured{}
			ul.SetGroupVersionKind(gvk)
			obj = ul
			op = event.LIST
		case "create":
			a := action.(testing.CreateAction)
			obj = a.GetObject()
			op = event.CREATE
		case "update":
			a := action.(testing.UpdateAction)
			obj = a.GetObject()
			op = event.UPDATE
		case "delete":
			a := action.(testing.DeleteAction)
			obj, err = lookup(a.GetResource(), a.GetNamespace(), a.GetName())
			op = event.MARK_FOR_DELETION
		case "patch":
			a := action.(testing.PatchAction)
			obj, err = lookup(a.GetResource(), a.GetNamespace(), a.GetName())
			op = event.PATCH
		case "updatesubresource":
			// Handle status updates which use UpdateSubresourceAction
			if updateSubAction, ok := action.(interface {
				GetSubresource() string
				GetObject() runtime.Object
				GetNamespace() string
			}); ok {
				subresource := updateSubAction.GetSubresource()
				if subresource == "status" {
					obj = updateSubAction.GetObject()
					op = event.UPDATE
				} else {
					logger.V(1).Info("updatesubresource with non-status subresource", "subresource", subresource, "resource", resource)
				}
			} else {
				panic("updatesubresource action type assertion failed - not supposed to happen")
			}
		default:
			// Log unhandled verbs to help debug missing status updates
			if action.GetVerb() != "watch" && action.GetVerb() != "deletecollection" {
				panic("unhandled action type: " + strings.Join([]string{action.GetVerb(), action.GetResource().Resource}, " "))
			}
			return false, nil, nil
		}

		if err == nil && obj != nil {
			if co, ok := obj.(client.Object); ok {
				if _, isEvent := co.(*corev1.Event); isEvent {
					logger.V(1).Info("skipping event recording", "operation", op, "name", co.GetName())
					return false, nil, err
				}
				ensureGVK(co)
				if tag.GetSleeveObjectID(co) == "" {
					tag.AddSleeveObjectID(co)
				}
				logger.V(2).Info("recording effect",
					"operation", op,
					"name", co.GetName(),
					"kind", co.GetObjectKind().GroupVersionKind().Kind,
				)
				recorder.RecordEffect(ctx, co, op, nil)
			} else {
				logger.V(1).Info("object does not implement client.Object", "operation", op, "type", fmt.Sprintf("%T", obj))
			}
		} else if err != nil {
			logger.V(1).Info("failed to resolve object for action", "error", err)
		}

		// Return false to let the default reactor handle the action.
		return false, nil, err
	}
}

func syncPodScalableInformer(ctx context.Context, dep *appsv1.Deployment, op event.OperationType, logger logr.Logger) {
	if err := ctx.Err(); err != nil {
		// TODO(tg/debug): remove once informer startup issues are resolved; this is a defensive guard
		logger.WithValues("stage", "podscalable-context").Info("skipping podscalable sync due to context error", "contextErr", err)
		return
	}

	factory := podscalableinformer.Get(ctx)
	gvr := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}

	inf, _, err := factory.Get(ctx, gvr)
	if err != nil {
		logger.WithValues("stage", "podscalable-get").Error(err, "failed to get podscalable informer")
		return
	}

	ps := makePodScalableFromDeployment(dep)
	indexer := inf.GetIndexer()

	switch op {
	case event.CREATE:
		if err := indexer.Add(ps); err != nil && !apierrs.IsAlreadyExists(err) {
			logger.WithValues("stage", "podscalable-add").Error(err, "failed to add podscalable to informer")
		} else {
			logger.WithValues("stage", "podscalable-add", "keys", indexer.ListKeys()).Info("added podscalable to informer")
		}
	case event.UPDATE:
		if err := indexer.Update(ps); err != nil {
			logger.WithValues("stage", "podscalable-update").Error(err, "failed to update podscalable in informer")
		} else {
			logger.WithValues("stage", "podscalable-update", "keys", indexer.ListKeys()).Info("updated podscalable in informer")
		}
	case event.MARK_FOR_DELETION:
		if err := indexer.Delete(ps); err != nil && !apierrs.IsNotFound(err) {
			logger.WithValues("stage", "podscalable-delete").Error(err, "failed to delete podscalable from informer")
		} else {
			logger.WithValues("stage", "podscalable-delete", "keys", indexer.ListKeys()).Info("deleted podscalable from informer")
		}
	}

	_, lister, err := factory.Get(ctx, gvr)
	if err == nil {
		if obj, getErr := lister.ByNamespace(dep.Namespace).Get(dep.Name); getErr == nil {
			logger.WithValues("stage", "podscalable-lister").Info("podscalable visible to lister", "object", obj)
		} else {
			logger.WithValues("stage", "podscalable-lister").Error(getErr, "podscalable not visible to lister")
		}
	} else {
		logger.WithValues("stage", "podscalable-lister").Error(err, "failed to retrieve lister")
	}
}

func makePodScalableFromDeployment(dep *appsv1.Deployment) *autoscalingv1alpha1.PodScalable {
	ps := &autoscalingv1alpha1.PodScalable{
		TypeMeta: metav1.TypeMeta{
			APIVersion: autoscalingv1alpha1.SchemeGroupVersion.String(),
			Kind:       "PodScalable",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        dep.Name,
			Namespace:   dep.Namespace,
			Labels:      dep.Labels,
			Annotations: dep.Annotations,
		},
		Status: autoscalingv1alpha1.PodScalableStatus{
			Replicas: dep.Status.Replicas,
		},
	}

	if dep.Spec.Replicas != nil {
		rep := *dep.Spec.Replicas
		ps.Spec.Replicas = &rep
	}
	if dep.Spec.Selector != nil {
		ps.Spec.Selector = dep.Spec.Selector.DeepCopy()
	}
	ps.Spec.Template = *dep.Spec.Template.DeepCopy()
	return ps
}

// seedDeploymentToDynamicClient adds a deployment to the dynamic fake client.
// This is necessary because the KPA scaler uses the dynamic client to patch deployments
// for scale operations, but deployments created via the typed kubeclient don't automatically
// appear in the dynamic client's store.
func seedDeploymentToDynamicClient(ctx context.Context, dep *appsv1.Deployment) error {
	dynamicClient := dynamicfake.Get(ctx)

	// Convert the deployment to unstructured
	unstructuredObj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(dep)
	if err != nil {
		return fmt.Errorf("failed to convert deployment to unstructured: %w", err)
	}

	u := &unstructured.Unstructured{Object: unstructuredObj}
	u.SetGroupVersionKind(appsv1.SchemeGroupVersion.WithKind("Deployment"))

	gvr := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	_, err = dynamicClient.Resource(gvr).Namespace(dep.Namespace).Create(ctx, u, metav1.CreateOptions{})
	if err != nil && !apierrs.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create deployment in dynamic client: %w", err)
	}

	return nil
}

// ReconcileAtState invokes the reconciler for a given state.
func (ks *KnativeStrategy) ReconcileAtState(ctx context.Context, nsName types.NamespacedName) (reconcile.Result, error) {
	servingClient := fakeservingclient.Get(ctx)
	kubeClient := fakekubeclient.Get(ctx)
	cachingClient := fakecachingclient.Get(ctx)
	networkingClient := fakenetworkingclient.Get(ctx)

	logger := log.FromContext(ctx).WithName("reconcile").WithValues("key", nsName.String())

	// Create a reactor and attach it to both clients to intercept and record actions.
	reactor := newReactor(ctx, ks.recorder,
		servingClient.Tracker(),
		kubeClient.Tracker(),
		cachingClient.Tracker(),
		networkingClient.Tracker())

	// Add the reactor to both clients.
	servingClient.PrependReactor("*", "*", reactor)
	kubeClient.PrependReactor("*", "*", reactor)
	cachingClient.PrependReactor("*", "*", reactor)
	networkingClient.PrependReactor("*", "*", reactor)

	// must re-initialize the controller each time to reset its informer state
	ctrl := ks.factory(ctx, nil)
	logger = logger.WithValues("reconciler", ctrl.Name)
	if la, ok := ctrl.Reconciler.(reconciler.LeaderAware); ok {
		la.Promote(reconciler.UniversalBucket(), func(reconciler.Bucket, types.NamespacedName) {})
	} else {
		logger.Error(fmt.Errorf("not leader-aware"), "reconcile aborted")
		return reconcile.Result{}, fmt.Errorf("Reconciler is not leader-aware")
	}

	key := nsName.String()
	err := ctrl.Reconciler.Reconcile(ctx, key)

	if err != nil {
		errMsg := err.Error()
		if apierrs.IsNotFound(err) || strings.Contains(errMsg, " not found") {
			logger.Info("transient not found; requeueing", "error", err)
			return reconcile.Result{Requeue: true}, nil
		}

		requeue, requeueAfter := controller.IsRequeueKey(err)
		if !requeue {
			logger.Error(err, "reconcile failed")
			return reconcile.Result{}, err // Return actual error if it's not a requeue request
		}

		logger.Info("reconcile completed", "requeue", true, "requeueAfter", requeueAfter)
		return reconcile.Result{Requeue: true, RequeueAfter: requeueAfter}, nil
	}

	logger.Info("reconcile completed", "requeue", false, "requeueAfter", 0)

	// Apply any pending scale changes from the KPA's dynamic client patch operations.
	// These need to be recorded via the effect recorder so they're visible to the simulation.
	applyPendingScaleChanges(ctx, ks.recorder)

	return reconcile.Result{}, nil
}

// applyPendingScaleChanges applies any pending deployment scale changes to the simulation state.
// This bridges the gap between the KPA's dynamic client (which patches deployments directly)
// and the simulation's snapshot store (which the DeploymentController reads from).
// The recorder parameter is unused but kept for signature compatibility.
func applyPendingScaleChanges(ctx context.Context, _ replay.EffectRecorder) {
	scaleChanges := GetAndClearScaleChanges()
	if len(scaleChanges) == 0 {
		return
	}

	kubeClient := fakekubeclient.Get(ctx)
	for key, replicas := range scaleChanges {
		fmt.Printf("🔔 APPLY-SCALE-CHANGE: applying %s/%s -> replicas=%d to simulation state\n", key.Namespace, key.Name, replicas)

		// Get the deployment from the typed client
		dep, err := kubeClient.AppsV1().Deployments(key.Namespace).Get(ctx, key.Name, metav1.GetOptions{})
		if err != nil {
			fmt.Printf("🔔 APPLY-SCALE-CHANGE: failed to get deployment: %v\n", err)
			continue
		}

		// Update the replicas
		dep.Spec.Replicas = &replicas

		// Update via the kubeclient - the reactor will intercept this and record the effect
		_, err = kubeClient.AppsV1().Deployments(key.Namespace).Update(ctx, dep, metav1.UpdateOptions{})
		if err != nil {
			fmt.Printf("🔔 APPLY-SCALE-CHANGE: failed to update deployment: %v\n", err)
			continue
		}

		fmt.Printf("🔔 APPLY-SCALE-CHANGE: successfully recorded deployment scale change\n")
	}
}

// setupDeploymentPatchSync adds a reactor to the dynamic client that syncs deployment patches
// to the typed kubeclient. This is necessary because the KPA uses the dynamic client for scale
// operations, but the DeploymentController reads from the typed kubeclient's store.
func setupDeploymentPatchSync(ctx context.Context, dynamicFakeClient *dynamicfakeclient.FakeDynamicClient) {
	persistentDynamicClientMu.Lock()
	defer persistentDynamicClientMu.Unlock()

	if persistentDynamicClientReactorAdded {
		fmt.Printf("🔔 SETUP-PATCH-SYNC: Reactor already added, skipping\n")
		return
	}
	persistentDynamicClientReactorAdded = true

	fmt.Printf("🔔 SETUP-PATCH-SYNC: Adding deployment patch reactor to dynamic client (first time), client=%p\n", dynamicFakeClient)

	// Add a catch-all reactor to see what operations are happening
	dynamicFakeClient.PrependReactor("*", "*", func(action testing.Action) (bool, runtime.Object, error) {
		fmt.Printf("🔔 DYNAMIC-CLIENT-ACTION: verb=%s, resource=%s, namespace=%s\n",
			action.GetVerb(), action.GetResource().Resource, action.GetNamespace())
		return false, nil, nil // Don't handle, let it fall through
	})

	// Add a reactor for Patch operations on deployments.
	// The KPA uses the dynamic client to patch deployment replicas for scale operations.
	// We intercept this and update the deployment in the dynamic client's store.
	gvr := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	dynamicFakeClient.PrependReactor("patch", "deployments", func(action testing.Action) (bool, runtime.Object, error) {
		patchAction, ok := action.(testing.PatchAction)
		if !ok {
			return false, nil, nil
		}

		ns := patchAction.GetNamespace()
		name := patchAction.GetName()
		patch := patchAction.GetPatch()

		fmt.Printf("🔔 DYNAMIC-PATCH-REACTOR: patching deployment %s/%s with patch: %s\n", ns, name, string(patch))

		// Get the deployment from the dynamic client's store
		unstructuredDep, err := dynamicFakeClient.Tracker().Get(gvr, ns, name)
		if err != nil {
			fmt.Printf("🔔 DYNAMIC-PATCH-REACTOR: failed to get deployment from dynamic client: %v\n", err)
			return false, nil, err
		}

		u, ok := unstructuredDep.(*unstructured.Unstructured)
		if !ok {
			fmt.Printf("🔔 DYNAMIC-PATCH-REACTOR: unexpected type: %T\n", unstructuredDep)
			return false, nil, fmt.Errorf("unexpected type: %T", unstructuredDep)
		}

		// Apply the JSON patch to the deployment
		// The patch format is: [{"op":"replace","path":"/spec/replicas","value":0}]
		var patchOps []map[string]interface{}
		if err := json.Unmarshal(patch, &patchOps); err != nil {
			fmt.Printf("🔔 DYNAMIC-PATCH-REACTOR: failed to unmarshal patch: %v\n", err)
			return false, nil, err
		}

		for _, op := range patchOps {
			path, _ := op["path"].(string)
			if path == "/spec/replicas" {
				value, _ := op["value"].(float64)
				replicas := int64(value)
				if err := unstructured.SetNestedField(u.Object, replicas, "spec", "replicas"); err != nil {
					fmt.Printf("🔔 DYNAMIC-PATCH-REACTOR: failed to set replicas: %v\n", err)
					return false, nil, err
				}
				fmt.Printf("🔔 DYNAMIC-PATCH-REACTOR: setting replicas to %d\n", replicas)
				// Record this scale change so the KnativeStrategy can propagate it to the simulation state
				RecordScaleChange(ns, name, int32(replicas))
			}
		}

		// Update the deployment in the dynamic client's tracker
		if err := dynamicFakeClient.Tracker().Update(gvr, u, ns); err != nil {
			fmt.Printf("🔔 DYNAMIC-PATCH-REACTOR: failed to update deployment in dynamic client: %v\n", err)
			return false, nil, err
		}

		fmt.Printf("🔔 DYNAMIC-PATCH-REACTOR: successfully updated deployment scale in dynamic client\n")

		return true, u, nil
	})
}

func setupClientState(ctx context.Context, state []runtime.Object, selectors ...string) (context.Context, func(), error) {
	ctx, cancel := context.WithCancel(ctx)
	ctx = filteredinformerfactory.WithSelectors(ctx, selectors...)
	ctx = injection.WithConfig(ctx, &rest.Config{})

	// Set up fake informers first. This creates fake clients including a dynamic client.
	ctx, informers := injection.Fake.SetupInformers(ctx, &rest.Config{})

	// Use a persistent dynamic client so reactors are triggered across reconcile steps.
	// The dynamic client is used by KPA for scale operations, and we need to add reactors
	// to sync patches back to the typed kubeclient.
	// IMPORTANT: We inject the persistent dynamic client AFTER SetupInformers so it overrides
	// the fake dynamic client that SetupInformers creates.
	persistentDynamicClientMu.Lock()
	if persistentDynamicClient == nil {
		fmt.Printf("🔔 SETUP-CLIENT-STATE: Creating persistent dynamic client\n")
		persistentDynamicClient = dynamicfakeclient.NewSimpleDynamicClient(kamerascheme.Default)
	} else {
		fmt.Printf("🔔 SETUP-CLIENT-STATE: Reusing persistent dynamic client\n")
	}
	// Inject the persistent dynamic client into the context, overriding any client from SetupInformers
	ctx = context.WithValue(ctx, dynamicclient.Key{}, persistentDynamicClient)
	persistentDynamicClientMu.Unlock()

	// Add a reactor to sync dynamic client deployment patches to the typed kubeclient.
	// This is necessary because the KPA uses the dynamic client to patch deployments for scale
	// operations, but our DeploymentController reads from the typed kubeclient.
	setupDeploymentPatchSync(ctx, persistentDynamicClient)

	logger := log.FromContext(ctx).WithName("setup")
	type informerMeta struct {
		typeName string
		informer controller.Informer
	}
	metas := make([]informerMeta, len(informers))

	for idx, informer := range informers {
		typeName := fmt.Sprintf("%T", informer)
		logger.Info("registered informer", "index", idx, "type", typeName)
		metas[idx] = informerMeta{typeName: typeName, informer: informer}
	}

	if err := insertObjects(ctx, state); err != nil {
		return nil, cancel, err
	}

	if err := ensureSystemResources(ctx); err != nil {
		return nil, cancel, err
	}

	waitInformers, err := reconcilertesting.RunAndSyncInformers(ctx, informers...)
	if err != nil {
		// Log which informer failed to sync for easier debugging.
		for idx, meta := range metas {
			logger.Error(err, "informer sync status", "index", idx, "type", meta.typeName, "synced", meta.informer.HasSynced())
		}
		logger.Error(err, "RunAndSyncInformers failed")
		cancel()
		return nil, nil, fmt.Errorf("failed to sync informers: %w", err)
	}

	return ctx, func() {
		cancel()
		waitInformers()
	}, nil
}

func insertObjects(ctx context.Context, objs []runtime.Object) error {
	servingclient := fakeservingclient.Get(ctx)
	kubeclient := fakekubeclient.Get(ctx)
	cachingclient := fakecachingclient.Get(ctx)
	networkingclient := fakenetworkingclient.Get(ctx)

	// i am sorry for the following code
	for _, obj := range objs {
		if u, ok := obj.(*unstructured.Unstructured); ok {
			typed, err := convertUnstructured(u)
			if err != nil {
				return fmt.Errorf("failed to convert unstructured %s: %w", u.GroupVersionKind().String(), err)
			}
			obj = typed
		}

		switch o := obj.(type) {
		case *v1.Service:
			if _, err := servingclient.ServingV1().Services(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create service: %w", err)
			}
		case *v1.Route:
			if _, err := servingclient.ServingV1().Routes(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create route: %w", err)
			}
		case *v1.Configuration:
			if _, err := servingclient.ServingV1().Configurations(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create configuration: %w", err)
			}
		case *v1.Revision:
			if _, err := servingclient.ServingV1().Revisions(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create revision: %w", err)
			}
		case *autoscalingv1alpha1.PodAutoscaler:
			if _, err := servingclient.AutoscalingV1alpha1().PodAutoscalers(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create podautoscaler: %w", err)
			}
		case *autoscalingv1alpha1.Metric:
			if _, err := servingclient.AutoscalingV1alpha1().Metrics(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create metric: %w", err)
			}
		case *netv1alpha1.ServerlessService:
			if _, err := networkingclient.NetworkingV1alpha1().ServerlessServices(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create serverlessservice: %w", err)
			}
		case *netv1alpha1.Ingress:
			if _, err := networkingclient.NetworkingV1alpha1().Ingresses(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create knative ingress: %w", err)
			}
		case *corev1.ConfigMap:
			if _, err := kubeclient.CoreV1().ConfigMaps(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create configmap: %w", err)
			}
		case *corev1.Secret:
			if _, err := kubeclient.CoreV1().Secrets(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create secret: %w", err)
			}
		case *corev1.ServiceAccount:
			if _, err := kubeclient.CoreV1().ServiceAccounts(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create serviceaccount: %w", err)
			}
		case *corev1.Pod:
			if _, err := kubeclient.CoreV1().Pods(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create pod: %w", err)
			}
		case *corev1.Endpoints:
			if _, err := kubeclient.CoreV1().Endpoints(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create endpointss: %w", err)
			}
		case *corev1.Service:
			if _, err := kubeclient.CoreV1().Services(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create service: %w", err)
			}
		case *appsv1.Deployment:
			if _, err := kubeclient.AppsV1().Deployments(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create deployment: %w", err)
			}
			ensureGVK(o)
			// Also seed the deployment to the dynamic client so applyScale can find it
			if err := seedDeploymentToDynamicClient(ctx, o); err != nil {
				return fmt.Errorf("failed to seed deployment to dynamic client: %w", err)
			}
			logger := log.FromContext(ctx).WithName("seed").WithValues("resource", "deployments", "namespace", o.Namespace, "name", o.Name)
			syncPodScalableInformer(ctx, o, event.CREATE, logger)
		case *appsv1.ReplicaSet:
			if _, err := kubeclient.AppsV1().ReplicaSets(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create replicaset: %w", err)
			}
		case *cachingv1alpha1.Image:
			if _, err := cachingclient.CachingV1alpha1().Images(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create image: %w", err)
			}
		case *networkingv1.Ingress:
			if _, err := kubeclient.NetworkingV1().Ingresses(o.Namespace).Create(ctx, o, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create ingress: %w", err)
			}
			ensureGVK(o)
		default:
			return fmt.Errorf("unsupported type %T", o)
		}
	}
	return nil

}

func ensureSystemResources(ctx context.Context) error {
	kubeclient := fakekubeclient.Get(ctx)
	ns := system.Namespace()

	activatorSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "activator-service",
			Namespace: ns,
			Labels: map[string]string{
				"app": "activator",
			},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"app": "activator",
			},
			Ports: []corev1.ServicePort{
				{Name: "http", Port: 80, TargetPort: intstr.FromInt(8012)},
				{Name: "http2", Port: 81, TargetPort: intstr.FromInt(8013)},
				{Name: "https", Port: 443, TargetPort: intstr.FromInt(8112)},
				{Name: "http-metrics", Port: 9090, TargetPort: intstr.FromInt(9090)},
				{Name: "http-profiling", Port: 8008, TargetPort: intstr.FromInt(8008)},
			},
		},
	}
	ensureGVK(activatorSvc)
	if _, err := kubeclient.CoreV1().Services(ns).Create(ctx, activatorSvc, metav1.CreateOptions{}); err != nil && !apierrs.IsAlreadyExists(err) {
		return fmt.Errorf("failed to seed activator service: %w", err)
	}

	activatorEndpoints := &corev1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "activator-service",
			Namespace: ns,
		},
		Subsets: []corev1.EndpointSubset{{
			Addresses: []corev1.EndpointAddress{{
				IP:       "10.0.0.1",
				Hostname: "activator-0",
			}},
			Ports: []corev1.EndpointPort{
				{Name: "http", Port: 8012, Protocol: corev1.ProtocolTCP},
				{Name: "http2", Port: 8013, Protocol: corev1.ProtocolTCP},
				{Name: "https", Port: 8112, Protocol: corev1.ProtocolTCP},
				{Name: "http-metrics", Port: 9090, Protocol: corev1.ProtocolTCP},
				{Name: "http-profiling", Port: 8008, Protocol: corev1.ProtocolTCP},
			},
		}},
	}
	ensureGVK(activatorEndpoints)
	if _, err := kubeclient.CoreV1().Endpoints(ns).Create(ctx, activatorEndpoints, metav1.CreateOptions{}); err != nil && !apierrs.IsAlreadyExists(err) {
		return fmt.Errorf("failed to seed activator endpoints: %w", err)
	}

	return nil
}

var kindToGVK = map[string]schema.GroupVersionKind{
	"Deployment":        appsv1.SchemeGroupVersion.WithKind("Deployment"),
	"ReplicaSet":        appsv1.SchemeGroupVersion.WithKind("ReplicaSet"),
	"Image":             cachingv1alpha1.SchemeGroupVersion.WithKind("Image"),
	"PodAutoscaler":     autoscalingv1alpha1.SchemeGroupVersion.WithKind("PodAutoscaler"),
	"Metric":            autoscalingv1alpha1.SchemeGroupVersion.WithKind("Metric"),
	"Configuration":     v1.SchemeGroupVersion.WithKind("Configuration"),
	"Revision":          v1.SchemeGroupVersion.WithKind("Revision"),
	"Route":             v1.SchemeGroupVersion.WithKind("Route"),
	"Service":           v1.SchemeGroupVersion.WithKind("Service"),
	"ServerlessService": netv1alpha1.SchemeGroupVersion.WithKind("ServerlessService"),
	"Ingress":           networkingv1.SchemeGroupVersion.WithKind("Ingress"),
}

var resourceToListKind = map[string]string{
	"deployments":        "DeploymentList",
	"replicasets":        "ReplicaSetList",
	"images":             "ImageList",
	"podautoscalers":     "PodAutoscalerList",
	"metrics":            "MetricList",
	"configurations":     "ConfigurationList",
	"revisions":          "RevisionList",
	"routes":             "RouteList",
	"services":           "ServiceList",
	"serverlessservices": "ServerlessServiceList",
	"pods":               "PodList",
	"endpoints":          "EndpointsList",
	"configmaps":         "ConfigMapList",
	"secrets":            "SecretList",
	"serviceaccounts":    "ServiceAccountList",
	"ingresses":          "IngressList",
}

func ensureGVK(obj client.Object) {
	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Kind != "" && gvk.Version != "" {
		return
	}
	if gvks, _, err := kamerascheme.Default.ObjectKinds(obj); err == nil && len(gvks) > 0 {
		for _, candidate := range gvks {
			if candidate.Kind != "" && candidate.Version != "" {
				obj.GetObjectKind().SetGroupVersionKind(candidate)
				return
			}
		}
	}
	switch o := obj.(type) {
	case *corev1.ConfigMap:
		o.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("ConfigMap"))
	case *corev1.Secret:
		o.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Secret"))
	case *corev1.ServiceAccount:
		o.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("ServiceAccount"))
	case *corev1.Pod:
		o.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Pod"))
	case *corev1.Endpoints:
		o.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Endpoints"))
	case *corev1.Service:
		o.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Service"))
	case *networkingv1.Ingress:
		o.SetGroupVersionKind(networkingv1.SchemeGroupVersion.WithKind("Ingress"))
	case *netv1alpha1.Ingress:
		o.SetGroupVersionKind(netv1alpha1.SchemeGroupVersion.WithKind("Ingress"))
	case *appsv1.Deployment:
		o.SetGroupVersionKind(appsv1.SchemeGroupVersion.WithKind("Deployment"))
	case *appsv1.ReplicaSet:
		o.SetGroupVersionKind(appsv1.SchemeGroupVersion.WithKind("ReplicaSet"))
	case *v1.Service:
		o.SetGroupVersionKind(v1.SchemeGroupVersion.WithKind("Service"))
	case *v1.Route:
		o.SetGroupVersionKind(v1.SchemeGroupVersion.WithKind("Route"))
	case *v1.Configuration:
		o.SetGroupVersionKind(v1.SchemeGroupVersion.WithKind("Configuration"))
	case *v1.Revision:
		o.SetGroupVersionKind(v1.SchemeGroupVersion.WithKind("Revision"))
	case *autoscalingv1alpha1.PodAutoscaler:
		o.SetGroupVersionKind(autoscalingv1alpha1.SchemeGroupVersion.WithKind("PodAutoscaler"))
	case *autoscalingv1alpha1.Metric:
		o.SetGroupVersionKind(autoscalingv1alpha1.SchemeGroupVersion.WithKind("Metric"))
	case *cachingv1alpha1.Image:
		o.SetGroupVersionKind(cachingv1alpha1.SchemeGroupVersion.WithKind("Image"))
	}
}

func convertUnstructured(u *unstructured.Unstructured) (runtime.Object, error) {
	gvk := u.GroupVersionKind()
	if gvk.Empty() {
		if kind := u.GetKind(); kind != "" {
			if apiVersion := u.GetAPIVersion(); apiVersion != "" {
				if gv, err := schema.ParseGroupVersion(apiVersion); err == nil {
					gvk = gv.WithKind(kind)
				}
			}
		}
	}
	// Only apply a fallback mapping when we have neither group nor kind.
	// Otherwise we risk remapping core kinds (e.g., core/v1 Service) to a
	// different API group (e.g., serving.knative.dev/v1 Service).
	if gvk.Group == "" && gvk.Kind == "" && u.GetKind() != "" {
		if mapped, ok := kindToGVK[u.GetKind()]; ok {
			gvk = mapped
		}
	}
	if gvk.Empty() {
		return nil, fmt.Errorf("object has no GroupVersionKind")
	}

	obj, err := kamerascheme.Default.New(gvk)
	if err != nil {
		return nil, fmt.Errorf("creating typed object for %s: %w", gvk.String(), err)
	}

	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, obj); err != nil {
		return nil, fmt.Errorf("converting from unstructured: %w", err)
	}

	if accessor, err := meta.Accessor(obj); err == nil {
		accessor.SetNamespace(u.GetNamespace())
		accessor.SetName(u.GetName())
		accessor.SetResourceVersion(u.GetResourceVersion())
	}
	obj.GetObjectKind().SetGroupVersionKind(gvk)

	return obj, nil
}

func listKindForResource(resource string) string {
	if kind, ok := resourceToListKind[resource]; ok {
		return kind
	}
	if resource == "" {
		return "List"
	}
	return strings.ToUpper(resource[:1]) + resource[1:] + "List"
}
