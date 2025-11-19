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
        "github.com/tgoodwin/kamera/pkg/tracecheck"
        "sigs.k8s.io/controller-runtime/pkg/client"
        "k8s.io/apimachinery/pkg/runtime"
        corev1 "k8s.io/api/core/v1"
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

4. **Register each controller-runtime reconciler.** Supply a factory that accepts a controller-runtime `client.Client`. For alternative controller implementations, see [below](#using-non-controller-runtime-controllers).

    ```go
    eb.WithReconciler("FooController", func(c client.Client) tracecheck.Reconciler {
        return &fooctrl.FooReconciler{Client: c, Scheme: scheme}
    })
    eb.WithReconciler("BarController", func(c client.Client) tracecheck.Reconciler {
        return &barctrl.BarReconciler{Client: c, Scheme: scheme}
    })
    ```

5. **Describe controller dependencies and ownership.** `WithResourceDep` declares which reconcilers subscribe to watch/read a kind, while `AssignReconcilerToKind` identifies the primary owners that should be triggered when objects of that kind change. These dependencies determine which controllers will be queued to reconcile in response to resource state changes during the exploration process.

    ```go
    const fooKind = "mygroup.example.com/Foo"
    const barKind = "mygroup.example.com/Bar"
    eb.AssignReconcilerToKind("FooController", fooKind)          // FooController owns Foo resources
    eb.AssignReconcilerToKind("BarController", barKind)          // BarController  owns Bar resources
    eb.WithResourceDep(fooKind, "FooController", "BarController") // both controllers watch Foo objects
    ```

6. **Seed the initial cluster state.** Use the state builder helpers to create a `StateNode` that includes your top-level objects and the initial pending reconciles for that state.

    ```go
    sb := eb.NewStateEventBuilder()
    // add an object along with the initial pending reconciles
    initialState := sb.AddTopLevelObject(fooObj, "FooController", "BarController")
    ```

7. **Build and run the explorer.**

    ```go
    explorer, err := eb.Build()
    if err != nil {
        log.Fatal(err)
    }
    result := explorer.Explore(context.Background(), initialState)
    for _, converged := range result.ConvergedStates {
        fmt.Printf("paths to converged state: %d\n", len(converged.Paths))
    }
    ```

That’s enough to start evaluating how your controllers interact across different interleavings.

### Using non-controller-runtime controllers

If your controllers aren’t built with `controller-runtime`, implement the `tracecheck.Strategy` interface (the same contract Kamera uses internally) and register it with the builder:

```go
eb.WithStrategy("MyCustomController", func(recorder replay.EffectRecorder) tracecheck.Strategy {
    return &MyStrategyImpl{
        Recorder: recorder,
        // ...inject whatever else your reconciler needs...
    }
})
eb.WithResourceDep("mygroup.example.com/Foo", "MyCustomController")
```

`WithStrategy` receives a `replay.EffectRecorder` so your custom strategy can record controller actions like the controller-runtime strategy does automatically. You'll use the effect recorder to implement your own write set recording. Everything else (state tracking, pending reconcile management, and path exploration) works the same, which makes it straightforward to mix and match controller-runtime reconcilers with bespoke logic in the same Explorer setup.

### Inspecting exploration results

Kamera ships with a terminal inspector that lets you interactively browse converged states, execution paths, and per-step effects. After running an exploration you can launch it inline:

```go
result := explorer.Explore(context.Background(), initialState)
states := result.ConvergedStates
if err := interactive.RunStateInspectorTUIView(states, true); err != nil {
    log.Fatal(err)
}
```

You can also save a snapshot for later review:

```go
if err := interactive.SaveInspectorDump(states, "camera_inspector_dump.json"); err != nil {
    log.Fatal(err)
}
```

Dump files can be reopened at any time via `go run ./cmd/inspect --dump camera_inspector_dump.json`, which restores the same UI. The inspector provides keyboard shortcuts (shown in the status bar) to switch between states, examine individual reconcile steps, and export dumps from within the UI.

## License

This project is licensed under the terms of the [MIT License](LICENSE).
