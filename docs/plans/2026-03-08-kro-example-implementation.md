# KRO Example Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a new `examples/kro` standalone example that wires a documented KRO application flow into Kamera, ships public-docs-derived inputs, and explains the harness boundaries clearly.

**Architecture:** Build a small standalone Go module under `examples/kro` that follows the existing example pattern: a thin `main.go`, a scenario/harness package, focused tests, and JSON/YAML inputs. Use the official KRO docs as the source of truth for the `ResourceGraphDefinition` and instance inputs, but keep the harness explicit about the main approximation: Kamera will statically wire one generated KRO instance kind instead of booting KRO’s full dynamic controller manager.

**Tech Stack:** Go, Kamera `pkg/explore`/`pkg/tracecheck`, Kubernetes API types, unstructured objects, documented KRO YAML examples.

### Task 1: Establish the KRO example skeleton

**Files:**
- Create: `examples/kro/go.mod`
- Create: `examples/kro/main.go`
- Create: `examples/kro/README.md`
- Create: `examples/kro/inputs.json`
- Create: `examples/kro/application-rgd.yaml`
- Create: `examples/kro/application-instance.yaml`

**Step 1: Add the standalone module**

Write `examples/kro/go.mod` with a local `replace github.com/tgoodwin/kamera => ../..` so the example can build against this checkout.

**Step 2: Add the thin entrypoint**

Write `examples/kro/main.go` to mirror the existing examples:
- parse flags
- build the KRO explorer builder
- optionally load `--explore-config`
- run either interactive single-scenario mode or batch `--inputs` mode

**Step 3: Add the doc-derived fixtures**

Seed:
- `application-rgd.yaml` from KRO’s quick-start `Application` example
- `application-instance.yaml` from the corresponding instance example
- `inputs.json` with scenario cases derived from those docs

**Step 4: Commit**

```bash
git add examples/kro/go.mod examples/kro/main.go examples/kro/README.md examples/kro/inputs.json examples/kro/application-rgd.yaml examples/kro/application-instance.yaml
git commit -m "add kro example skeleton"
```

### Task 2: Write failing harness tests first

**Files:**
- Create: `examples/kro/scenario_test.go`
- Test: `examples/kro/scenario_test.go`

**Step 1: Write the failing tests**

Add tests that check:
- `scenariosFromInputs` rejects a nil builder
- `scenariosFromInputs` rejects empty inputs
- doc-derived inputs seed the RGD and generated instance correctly
- KRO-specific input translation preserves tuning and expected user actions

**Step 2: Run the targeted tests and verify RED**

Run:

```bash
cd examples/kro
go test ./... -run 'TestScenariosFromInputs|TestBuild'
```

Expected: FAIL because `scenario.go` and helper constructors do not exist yet.

**Step 3: Commit**

```bash
git add examples/kro/scenario_test.go
git commit -m "add kro harness tests"
```

### Task 3: Implement the KRO harness and adapters

**Files:**
- Create: `examples/kro/scenario.go`
- Create: `examples/kro/adapter_dynamic_client.go`
- Create: `examples/kro/adapter_clientset.go`
- Create: `examples/kro/graph_application.go`
- Modify: `examples/kro/main.go`

**Step 1: Add the explorer builder**

Implement `newKROExplorerBuilder()` and `buildInitialKROState(...)` following the Crossplane example structure.

**Step 2: Add the KRO controller wiring**

Register:
- an RGD reconciler/adapter that turns the documented `Application` RGD into generated API state for the harness
- an instance reconciler/adapter for the generated `Application` kind

**Step 3: Add the replay-aware adapters**

Create a small adapter layer so KRO-style dynamic operations read/write through the same underlying replay state Kamera records.

**Step 4: Add the static graph for the documented RGD**

Encode the quick-start `Application` resource graph explicitly:
- `deployment`
- `service`
- conditional `ingress`

Keep the static graph and README honest about this approximation.

**Step 5: Re-run the targeted tests and verify GREEN**

Run:

```bash
cd examples/kro
go test ./... -run 'TestScenariosFromInputs|TestBuild'
```

Expected: PASS.

**Step 6: Refactor**

Extract any shared helper code needed to keep the harness readable without broadening scope beyond the documented quick-start flow.

**Step 7: Commit**

```bash
git add examples/kro/main.go examples/kro/scenario.go examples/kro/adapter_dynamic_client.go examples/kro/adapter_clientset.go examples/kro/graph_application.go
git commit -m "wire kro application flow into kamera"
```

### Task 4: Finish the README and example inputs

**Files:**
- Modify: `examples/kro/README.md`
- Modify: `examples/kro/inputs.json`
- Modify: `examples/kro/application-rgd.yaml`
- Modify: `examples/kro/application-instance.yaml`

**Step 1: Document the source inputs**

Explain which official KRO docs/examples the example mirrors.

**Step 2: Document the harness boundaries**

Spell out what is real vs approximated:
- real Kamera exploration flow
- documented KRO API shapes
- static generated-kind wiring instead of full dynamic-manager boot

**Step 3: Document run commands**

Add:
- `go run .`
- `go run . --inputs ...`
- expected output locations and batch behavior

**Step 4: Commit**

```bash
git add examples/kro/README.md examples/kro/inputs.json examples/kro/application-rgd.yaml examples/kro/application-instance.yaml
git commit -m "document kro example behavior"
```

### Task 5: Verify the example end to end

**Files:**
- Verify only

**Step 1: Run formatting**

Run:

```bash
gofmt -w examples/kro/*.go
```

**Step 2: Run example tests**

Run:

```bash
cd examples/kro
go test ./...
```

**Step 3: Run one batch command**

Run:

```bash
cd examples/kro
go run . --inputs ./inputs.json --output /tmp/kro-dumps --interactive=false
```

Expected: the harness loads doc-derived scenarios and emits per-scenario dump output without crashing.

**Step 4: Record remaining gaps**

If any unsupported KRO behavior is deferred, add a `bd` follow-up issue linked to this work.

**Step 5: Commit**

```bash
git add docs/plans/2026-03-08-kro-example-implementation.md examples/kro
git commit -m "finish kro example verification"
```
