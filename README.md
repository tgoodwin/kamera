# Kamera

Kamera is a testing system for Kubernetes custom control planes, which compose
multiple controllers to automate infrastructure management. It uses partial
simulation—executing real controller code against a lightweight model of the
Kubernetes runtime—to make it practical to explore multi-controller reconcile
orderings, stale controller views, and faults without deploying a live cluster.

In the SOSP 2026 evaluation, Kamera found 31 new bugs across five popular
open-source custom control planes and achieved a 1,781× speedup over
cluster-based controller testing. See the
[full index of bug findings](artifact/bug-findings.md), including their public
reports and current status.

> [!IMPORTANT]
> **SOSP 2026 artifact evaluators:** use the `sosp-ae` branch and start with
> the [artifact evaluation guide](artifact/README.md). It provides the shortest
> successful path through the badge checks, reproduction scripts, expected
> outputs, and runtime estimates.

<!--## Reproducing the Table 6 Sieve baselines-->
<!---->
<!--The standard artifact workflow runs the 11 Table 6 cases in Kamera with-->
<!--`./artifact/run-table6.sh`. Evaluators who also want to rerun the comparison-->
<!--with Sieve itself can use the checked-in wrapper around Sieve's real-->
<!--kind-cluster reproducer.-->
<!---->
<!--This optional workflow additionally requires Docker, kubectl, Helm 3, kind-->
<!--0.13.0, Go, Python, `jq`, a configured `GOPATH` and `KUBECONFIG`, and enough-->
<!--resources to run the target controllers in a local cluster. Use a dedicated-->
<!--host: Sieve creates and deletes a cluster named `kind`. The validated Python-->
<!--and container-image setup differs between Linux x86-64 and Apple Silicon, so-->
<!--complete the platform-specific preparation in the-->
<!--[Sieve baseline guide](artifact/sieve/README.md) before running the wrapper.-->
<!---->
<!--The general process is:-->
<!---->
<!--1. Clone `sieve-project/sieve` and check out the pinned revision-->
<!--   `6c97abeb79e644fa5eda889a2c174b2436dbc264`.-->
<!--2. Create Sieve's Python environment, then complete the platform-specific-->
<!--   image setup from the guide:-->
<!---->
<!--   ```bash-->
<!--   ./artifact/setup-sieve-python.sh /absolute/path/to/sieve-->
<!--   ```-->
<!---->
<!--   The baseline runner automatically uses the resulting virtual environment.-->
<!--3. Return to the Kamera repository root and validate the environment with one-->
<!--   RabbitMQ row:-->
<!---->
<!--   ```bash-->
<!--   ./artifact/run-sieve-baselines.sh \-->
<!--     --only rmq/intermediate-state-1 \-->
<!--     /absolute/path/to/sieve-->
<!--   ```-->
<!---->
<!--4. Run all 11 Table 6 rows:-->
<!---->
<!--   ```bash-->
<!--   ./artifact/run-sieve-baselines.sh /absolute/path/to/sieve-->
<!--   ```-->
<!---->
<!--The full run covers four ZooKeeper, four RabbitMQ, and three Cassandra bugs.-->
<!--It preserves every Sieve result and writes a combined `table6-sieve.tsv` under-->
<!--`artifact-results/`; a successful row has `reproduced=True` in Sieve's oracle-->
<!--output. Budget approximately 1.5–2 hours, with additional time possible when-->
<!--old amd64 workload images run under Apple Silicon emulation.-->
<!---->
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

    `Runner` honors the standard `-interactive`, `-perturb`, `-output`, and `-emit-stats` flags (see `pkg/explore/flags.go`) so you can disable the inspector, skip closed-loop analysis reruns (`--perturb=false`), or persist results when scripting. When `--emit-stats` is enabled, the dump written via `--output` includes a top-level `stats` section.

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

Dump files can be reopened at any time via `go run ./cmd/kamera inspect exploration inspector_dump.jsonl`, which restores the same UI. The inspector provides keyboard shortcuts (shown in the status bar) to switch between states, examine individual reconcile steps, and export dumps from within the UI. If the run used `--emit-stats`, that same dump file also carries top-level exploration stats.

### Unified CLI entrypoint

Kamera commands are available behind a single entrypoint:

```bash
go run ./cmd/kamera --help
go run ./cmd/kamera inspect exploration <dump.jsonl>
go run ./cmd/kamera determinize ./...
go run ./cmd/kamera generate --graph <graph.json> --input-map <input-map.json> --out <inputs.json>
go run ./cmd/kamera analyze report <dump.jsonl>
```

`inputs.json` uses a top-level JSON array (`[]coverage.Input`). Each entry must include:
- `name`: non-empty scenario name (unique within the file)
- `objects`: at least one Kubernetes object with `apiVersion` and `kind`
- `pending` (optional): each entry must set `controllerId` and `key.name` (`key.namespace` may be empty for cluster-scoped keys)

`coverage.LoadInputs` validates these constraints and returns index-specific errors for invalid files.

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
