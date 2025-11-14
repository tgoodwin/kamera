# Kamera

**Note:** This project is a research artifact and is under active development. Its APIs and functionalities are subject to change and it is not yet recommended for production use.

`kamera` is a toolkit for observing, analyzing, and verifying the behavior of the Kubernetes control plane. It is designed specifically for control plane components (controllers) built with the `controller-runtime` framework, providing targeted instrumentation to capture the behaviors of individual controllers and the interactions between them.

The primary goal of Kamera is to help platform developers understand complex interactions within the control plane by capturing detailed execution traces, enabling offline analysis and functional replay. To ensure control plane reliability, Kamera also employs implementation-level model checking and simulation testing of control plane deployments, enabling developers to proactively catch problematic behaviors that only manifest under certain conditions before deploying their code.

## Core Capabilities

Kamera provides a set of tools to:

*   **Trace Generation:** Instrument `controller-runtime` based controllers to capture fine-grained execution traces in a minimally invasive manner.
*   **System Replay:** Replay captured scenarios to reproduce and debug issues in the control plane.
*   **Trace Analysis:** Analyze traces to understand the causal relationships between controller reconciliations  and how data consistency affects reconciliation outcomes.
*   **Simulation Testing:** Systematically explore the state space of possible reconciliation executions to verify control plane convergence properties under different event orderings and data consistency scenarios.


## Getting Started


1. **Install Kamera as a module dependency** (e.g., `go get github.com/tgoodwin/kamera@latest`).

2. **Initialize a scheme with your APIs.**

    ```go
    scheme := runtime.NewScheme()
    utilruntime.Must(clientgoscheme.AddToScheme(scheme))
    utilruntime.Must(myapiv1.AddToScheme(scheme)) // register your CRDs
    ```

3. **Create an ExplorerBuilder.** It automatically wires a default in-memory emitter, so no extra plumbing is needed unless you want to stream traces elsewhere.

    ```go
    eb := tracecheck.NewExplorerBuilder(scheme)
    eb.WithMaxDepth(100) // optional
    ```

4. **Register each controller-runtime reconciler.** Supply a factory that accepts the Kamera client interface. Instantiate the reconciler with the scheme from earlier.

    ```go
    eb.WithReconciler("FooController", func(c tracecheck.Client) tracecheck.Reconciler {
        return &fooctrl.FooReconciler{Client: c, Scheme: scheme}
    })
    eb.WithReconciler("BarController", func(c tracecheck.Client) tracecheck.Reconciler {
        return &barctrl.BarReconciler{Client: c, Scheme: scheme}
    })
    ```

5. **Describe controller dependencies and ownership.** `WithResourceDep` declares which reconcilers subscribe to watch/read a kind, while `AssignReconcilerToKind` identifies the primary owners that should be triggered when objects of that kind change.

    ```go
    const fooKind = "mygroup.example.com/Foo"
    const barKind = "mygroup.example.com/Bar"
    eb.AssignReconcilerToKind("FooController", fooKind)          // FooController owns Foo resources
    eb.AssignReconcilerToKind("BarController", barKind)          // BarController  owns Bar resources
    eb.WithResourceDep(fooKind, "FooController", "BarController") // both controllers watch Foo objects
    ```

6. **Seed the initial cluster state.** Use the state builder helpers to create a `StateNode` that includes your top-level objects and pending reconciles.

    ```go
    sb := eb.NewStateEventBuilder()
    initial := sb.AddTopLevelObject(fooObj, "FooController", "BarController")
    ```

7. **Build and run the explorer.**

    ```go
    explorer, err := eb.Build()
    if err != nil {
        log.Fatal(err)
    }
    result := explorer.Explore(context.Background(), initial)
    for _, converged := range result.ConvergedStates {
        fmt.Printf("paths to converged state: %d\n", len(converged.Paths))
    }
    ```

That’s enough to start evaluating how your controllers interact across different interleavings.

### Using non-controller-runtime controllers

If your controllers aren’t built with `controller-runtime`, implement the `tracecheck.Strategy` interface (the same contract Kamera uses internally) and register it with the builder:

```go
eb.WithStrategy("MyCustomController", func(recorder replay.EffectRecorder) tracecheck.Strategy {
    return &MyStrategy{
        Recorder: recorder,
        // ...inject whatever your reconciler needs...
    }
})
eb.WithResourceDep("mygroup.example.com/Foo", "MyCustomController")
```

`WithStrategy` receives the effect recorder so your implementation can publish write sets just like the controller-runtime-based `tracecheck.Client` does automatically. You'll use the effect recorder to implement your own write set recording. Everything else—state tracking, pending reconcile management, and path exploration—works exactly the same, which makes it straightforward to mix and match controller-runtime reconcilers with bespoke logic in the same Explorer setup.

### Inspecting exploration results

Kamera ships with a terminal inspector that lets you interactively browse converged states, execution paths, and per-step effects. After running an exploration you can launch it inline:

```go
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
