# Crossplane FunctionRevision Real-Flow Wiring Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan.

**Goal:** Wire the CompositionRevision reconciler and seed a real FunctionRevision so pipeline validation succeeds without stubs.

**Architecture:** Add the CompositionRevision controller to the example, seed a FunctionRevision with composition capability, and switch to a real logger/recorder so errors surface. Keep all changes localized to `examples/crossplane` and scheme registration.

**Tech Stack:** Go, Crossplane controllers, Kamera tracecheck.

**Note:** User explicitly requested bypassing TDD for `examples/crossplane`, so test-first steps are replaced with build verification.

### Task 1: Seed a real FunctionRevision in the example

**Files:**
- Modify: `examples/crossplane/scheme.go`
- Modify: `examples/crossplane/scenario.go`

**Step 1: Update scheme to register FunctionRevision types**

Edit `examples/crossplane/scheme.go` to add `pkg/v1` to the scheme.

**Step 2: Add FunctionRevision builder helper**

In `examples/crossplane/scenario.go`, add a `buildFunctionRevision()` helper that returns a cluster-scoped FunctionRevision with:
- `spec.desiredState = Active`
- `metadata.labels[pkgv1.LabelParentPackage] = stubFunctionName`
- `status.capabilities` including `pkgmetav1.FunctionCapabilityComposition`

**Step 3: Add FunctionRevision to initial state**

Update `buildInitialCrossplaneState` to add the FunctionRevision as a top-level object and merge it with the Composition/XR state.

**Step 4: Build verification**

Run:

```bash
go build ./...
```

from `examples/crossplane`.

### Task 2: Wire the CompositionRevision reconciler

**Files:**
- Modify: `examples/crossplane/scenario.go`

**Step 1: Add revision reconciler to the explorer builder**

Import `internal/controller/apiextensions/revision` and register a reconciler for `apiextensions.crossplane.io/CompositionRevision` using `revision.NewReconciler`.

**Step 2: Reuse logger/recorder for revision**

Ensure the revision reconciler uses the shared logger/recorder (added in Task 3).

**Step 3: Build verification**

Run:

```bash
go build ./...
```

from `examples/crossplane`.

### Task 3: Surface errors in logs/events

**Files:**
- Modify: `examples/crossplane/scenario.go`
- Modify: `examples/crossplane/README.md`

**Step 1: Replace Nop logger with a real logger**

Use `github.com/tgoodwin/kamera/pkg/util/logger` to create a `logging.Logger` (Debug level) and pass it to Composition, CompositionRevision, and Composite reconcilers.

**Step 2: Add a simple event recorder**

Add a small `event.Recorder` implementation that logs events via the logger (include type, reason, message, object, and annotations), and pass it to reconcilers.

**Step 3: Document missing parts**

Update the README to mention that FunctionRevisions are seeded (no package manager) and that the example uses a local log recorder to surface events.

**Step 4: Build verification**

Run:

```bash
go build ./...
```

from `examples/crossplane`.

**Step 5: Commit**

```bash
git add examples/crossplane/scheme.go examples/crossplane/scenario.go examples/crossplane/README.md
git commit -m "wire crossplane function revision pipeline" 
```
