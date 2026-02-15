# Kamera

**Note:** This project is a research artifact and is under active development. Its APIs and functionalities are subject to change and it is not yet recommended for production use.

`kamera` is a toolkit for observing, analyzing, and verifying the behavior of the Kubernetes control plane. It is designed specifically for controllers built with [controller-runtime](https://github.com/kubernetes-sigs/controller-runtime), providing targeted instrumentation to capture the behaviors of individual controllers as well as the interactions between them.

The primary goal of Kamera is to help platform developers understand complex interactions within the control plane by capturing detailed execution traces, enabling offline analysis and functional replay. To ensure control plane reliability, Kamera also employs implementation-level model checking and simulation testing of control plane deployments, enabling developers to proactively catch problematic behaviors that only manifest under certain conditions before deploying their code.

## Try it out first!

Kick the tires with a [Knative Serving](https://knative.dev/docs/serving/) example. It wires the Knative Serving control plane up to Kamera and kicks off a simulation test which lets you inspect how Knative reconciles a `serving.knative.dev/v1/Service` across different interleavings.

```bash
cd examples/knative-serving
# first run: fetch deps
go mod tidy
# launch the explorer + interactive inspector UI
go run .
```
> Tip: this process can take a couple minutes, but you can let it run for ~30s and then ctrl-C out to view incremental results!

## Getting Started


1. **Install Kamera as a module dependency** (e.g., `go get github.com/tgoodwin/kamera@latest`).

    ```go
    import (
        "github.com/tgoodwin/kamera/pkg/explore"
        "github.com/tgoodwin/kamera/pkg/tracecheck"
        myapiv1 "github.com/yourorg/yourproject/api/v1" // replace with your module path
        "sigs.k8s.io/controller-runtime/pkg/client"
        "k8s.io/apimachinery/pkg/runtime"
        metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    )
    // ...
    ```

2. **Initialize a scheme with your APIs.**

    ```go
    scheme := runtime.NewScheme()
    myapiv1.AddToScheme(scheme) // register your CRDs
    corev1.AddToScheme(scheme) // register any related resource dependencies
    ```

3. **Create an ExplorerBuilder.** It lets you register reconciler implementations with the appropriate resource dependencies and lets you tune exploration parameters.

    ```go
    eb := tracecheck.NewExplorerBuilder(scheme)
    eb.WithMaxDepth(100) // optional
    ```

    Exploration defaults are conservative and opt-in:
    - No reconcile-order permutation is applied unless you explicitly set it in the explore config (for example: `cfg := eb.Config(); cfg.PermuteOrder["FooController"] = true; eb.SetConfig(cfg)`).
    - No staleness/perturbation injection is applied unless you explicitly configure it (for example with `WithPerturbations(...)`).
    - `OnlyPermuteTriggered` only affects permutation scope when at least one reconciler has permutation enabled; otherwise it has no effect.

4. **Register each controller-runtime reconciler.** Supply a factory that accepts a controller-runtime `client.Client`. The returned `ReconcilerBuilder` lets you chain `.For()` (primary resource) and `.Watches()` registrations. For non controller-runtime implementations, see [below](#using-non-controller-runtime-controllers).

    ```go
    eb.WithReconciler("FooController", func(c client.Client) tracecheck.Reconciler {
        return &fooctrl.FooReconciler{Client: c, Scheme: scheme}
    }).For("mygroup.example.com/Foo")

    eb.WithReconciler("BarController", func(c client.Client) tracecheck.Reconciler {
        return &barctrl.BarReconciler{Client: c, Scheme: scheme}
    }).For("mygroup.example.com/Bar")
    ```

5. **Describe controller dependencies and ownership.** Use `.For()` on the reconciler builder to declare primaries (controller-runtime `For()` semantics). Add explicit watches with `.Watches(kind, mapper)` on the reconciler builder when you need custom trigger mappings (controller-runtime `Watches()` semantics). `WithResourceDep` is deprecated; prefer explicit `.For()`/`.Watches()` declarations.

    ```go
    const fooKind = "mygroup.example.com/FooResource"
    const barKind = "mygroup.example.com/Bar"
    eb.WithReconciler("FooController", func(c client.Client) tracecheck.Reconciler {
        return &fooctrl.FooReconciler{Client: c, Scheme: scheme}
    }).For(fooKind)

    eb.WithReconciler("BarController", func(c client.Client) tracecheck.Reconciler {
        return &barctrl.BarReconciler{Client: c, Scheme: scheme}
    }).For(barKind)

    // Optional: explicit watch mapping if names/owners aren’t enough
    // .Watches("othergroup/Other", func(u *unstructured.Unstructured) []reconcile.Request { ... })
    ```

6. **Seed the initial cluster state.** Construct the objects you want in your starting cluster, then use the state builder helpers to create a `StateNode` that includes your top-level objects and the initial pending reconciles.

    ```go
    fooObj := &myapiv1.FooResource{
        ObjectMeta: metav1.ObjectMeta{
            Namespace: "default",
            Name:      "example-foo",
        },
        Spec: myapiv1.FooResourceSpec{
            Mode:     "alpha",
            Replicas: 2,
        },
    }
    fooObj.SetGroupVersionKind(myapiv1.GroupVersion.WithKind("FooResource"))

    sb := eb.NewStateEventBuilder()
    // add an object along with the initial pending reconciles
    initialState := sb.AddTopLevelObject(fooObj, "FooController", "BarController")
    ```

7. **Build and run with the Runner (recommended).** Runner wires the explorer to the inspector UI and handles restart requests using the shared version manager.

    ```go
    explorer, err := eb.Build()
    if err != nil {
        log.Fatal(err)
    }
    runner, err := explore.NewRunner(eb)
    if err != nil {
        log.Fatal(err)
    }

    if err := runner.Run(context.Background(), initialState); err != nil {
        log.Fatal(err)
    }
    ```

    `Runner` honors the standard `-interactive` and `-dump-output` flags (see `pkg/explore/flags.go`) so you can disable the inspector or persist results when scripting.

That’s enough to start evaluating how your controllers interact across different interleavings.

### Using non-controller-runtime controllers

If your controllers aren’t built with `controller-runtime`, that's fine! Kamera exposes a `tracecheck.Strategy` interface to support alternative controller structures via an adapter layer. Implement this interface and register it with the builder:

```go
eb.WithStrategy("MyCustomResourceController", func(recorder replay.EffectRecorder) tracecheck.Strategy {
    return &MyStrategyImpl{
        Recorder: recorder,
        // ...inject whatever else your reconciler needs...
    }
}).For("mygroup.example.com/MyCustomResource")
```

`WithStrategy` receives a `replay.EffectRecorder` so your custom strategy can record controller actions like the controller-runtime strategy does automatically. You'll use the effect recorder to implement your own strategy for capturing controller actions. Everything else (state tracking, pending reconcile management, and path exploration) works the same, which makes it straightforward to mix and match controller-runtime reconcilers with bespoke logic in the same Explorer setup.

### Inspecting exploration results

Kamera ships with a terminal inspector that lets you interactively browse converged states, execution paths, and per-step effects. After running an exploration you can launch it inline:

```go
result := explorer.Explore(context.Background(), initialState)
states := result.ConvergedStates
resolver := explorer.VersionManager()
if _, err := interactive.RunStateInspectorTUIView(states, resolver, true, tracecheck.ExploreConfig{}); err != nil {
    log.Fatal(err)
}
```

You can also save a snapshot for later review:

```go
if err := interactive.SaveInspectorDump(states, resolver, "inspector_dump.json"); err != nil {
    log.Fatal(err)
}
```

Dump files can be reopened at any time via `go run ./cmd/kamera inspect exploration inspector_dump.json`, which restores the same UI. The inspector provides keyboard shortcuts (shown in the status bar) to switch between states, examine individual reconcile steps, and export dumps from within the UI.

### Unified CLI entrypoint

Kamera commands are available behind a single entrypoint:

```bash
go run ./cmd/kamera --help
go run ./cmd/kamera inspect exploration <dump.jsonl>
go run ./cmd/kamera determinize ./...
go run ./cmd/kamera generate --graph <graph.json> --input-map <input-map.json> --out <inputs.json>
go run ./cmd/kamera analyze report <dump.jsonl>
```

### Using Kamera in test suites

Kamera can be run with `go test`, so you can easily cover multi-controller reconciliation flows in your test suites without relying on heavy integration test infrastructure. You can use Kamera to assert that these flows converge deterministically and that your domain-specific invariants hold across all executions.

```go
func TestWidgetControllerConverges(t *testing.T) {
    scheme := runtime.NewScheme()
    _ = myapiv1.AddToScheme(scheme)

    eb := tracecheck.NewExplorerBuilder(scheme)
    const widgetKind = "apps.example.com/Widget"
    eb.WithReconciler("WidgetController", func(c client.Client) tracecheck.Reconciler {
        return &widgetctrl.WidgetReconciler{Client: c, Scheme: scheme}
    }).For(widgetKind)

    // configure a max depth based on the complexity of your use case (# of reconciler invocations involved in the flow under test)
    eb.WithMaxDepth(100)

    widget := &myapiv1.Widget{
        ObjectMeta: metav1.ObjectMeta{
            Namespace: "default",
            Name:      "demo",
        },
        Spec: myapiv1.WidgetSpec{}, // seed your CR spec
    }
    widget.SetGroupVersionKind(myapiv1.GroupVersion.WithKind("Widget"))

    // construct an initial state as input and configure a controller to reconcile it
    initial := eb.NewStateEventBuilder().AddTopLevelObject(widget, "WidgetController")
    explorer, err := eb.Build()
    if err != nil {
        t.Fatalf("build explorer: %v", err)
    }

    result := explorer.Explore(context.Background(), initial)
    // assert that the state converges and does so deterministically
    if len(result.ConvergedStates) != 1 {
        t.Fatalf("expected 1 converged state, got %d", len(result.ConvergedStates))
    }
    endState := result.ConvergedStates[0]

    // assert any desired invariants specific to your use case
    // e.g. all pods must have a unique name
    seenPods := map[string]struct{}{}
    for _, obj := range explorer.Objects(endState) {
        if obj.GetKind() != "Pod" {
            continue
        }
        name := obj.GetName()
        if _, exists := seenPods[name]; exists {
            t.Fatalf("encountered non-unique pod name %q", name)
        }
        seenPods[name] = struct{}{}
    }
}
```


## License

This project is licensed under the terms of the [MIT License](LICENSE).
