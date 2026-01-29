# Kubernetes Operator Analysis Guidelines

When generating static dependency graphs for Kubernetes controllers, follow this five-phase approach: **Discovery**, **Verification**, **Topology**, **Interaction**, and **Resolution**.

## Phase 1: Controller Discovery (The "Where")

Do not assume a standard directory structure or stop at the first match. Codebases often split core and extension controllers across multiple directories.

1.  **Exhaustive Search:**
    *   Use `find . -name "*controller.go"` and `find . -name "*reconciler.go"` to locate *all* potential control loops, not just those in top-level directories.
    *   *Anti-Pattern:* Do not assume all controllers live in one `pkg/reconciler` folder. Check `internal/`, `pkg/`, and `vendor/` (if customized).
2.  **Search Heuristics:**
    *   **Standard Kubebuilder/Operator-SDK:** `internal/controller/` or `controllers/`.
    *   **Knative-style:** `pkg/reconciler/`.
    *   **Legacy/Custom:** `pkg/controllers/` or `cmd/`.
3.  **File Identification:**
    *   Files named `controller.go` often contain setup/wiring (e.g., `SetupWithManager`, `NewController`).
    *   Files named `reconciler.go` often contain business logic (`Reconcile`).

## Phase 2: Verification (The "Truth")

Cross-reference discovered files against the binary entrypoints to ensure no active controller is missed. This is critical for finding "leaf" or peripheral controllers (e.g., StateStores, Metrics) that might be located in separate packages.

1.  **Entrypoint Check:**
    *   Locate `cmd/` or `main.go` files.
    *   Read the `main` function to see which controllers are actually registered with the Manager (e.g., `controller.New(...)`, `reconciler.SetupWithManager(mgr)`).
    *   *Correction Rule:* If `main.go` registers a controller (e.g., `BucketStateStoreReconciler`) that was missed in Phase 1, explicitly locate and analyze that controller's source code.

## Phase 3: Topology Extraction (The "When")

Determine what triggers the controller. This defines the **Watches** edges. Locate the setup function identified in Phases 1 & 2.

1.  **Primary Trigger:**
    *   Look for `.For(&Type{})` or `controller.For(&Type{})`.
    *   *Graph Edge:* `Controller -> Watches (Primary) -> Resource`
2.  **Owned Resources (Garbage Collection triggers):**
    *   Look for `.Owns(&Type{})`.
    *   *Graph Edge:* `Controller -> Watches (Secondary) -> Resource`
3.  **Event Handlers (Cross-resource triggers):**
    *   Look for `.Watches(&Type{}, ...)` or `.Watches(source.Kind(...))`.
    *   *Graph Edge:* `Controller -> Watches (Secondary) -> Resource`

## Phase 4: Interaction Analysis (The "What")

Analyze the `Reconcile` function. **Crucial:** Do not stop at the top-level function. You must perform **Deep Traversal** of private helper methods (e.g., `reconcileResources`, `updateStatus`, `syncDeployment`) to find the actual client calls.

1.  **Read Detection:**
    *   **Keywords:** `client.Get`, `client.List`, `lister.Get`.
    *   *Context:* If the target object is *not* the Primary Trigger object, this is a **Read** dependency.
    *   *Graph Edge:* `Controller -> Reads -> Resource`
2.  **Write Detection:**
    *   **Keywords:** `client.Create`, `client.Update`, `client.Patch`, `client.Delete`.
    *   **Targeting:**
        *   **Spec:** `client.Update(ctx, obj)` (usually implies a Spec or Label update).
        *   **Status:** `client.Status().Update(...)` or `client.Status().Patch(...)`.
    *   **Dynamic Clients:** Watch for `unstructured.Unstructured` usage and `SetGroupVersionKind`. Use variable names, GVK construction logic, and context to infer the target Resource (e.g., "Promise-defined CR").
    *   *Graph Edge:* `Controller -> Writes (Target: Spec/Status) -> Resource`
3.  **Implicit/Chained Writes:**
    *   If a controller creates a `Job` or `Work` object that executes a pipeline, explicitly model the creation of that intermediate object.

## Phase 5: Type Resolution (The "Who")

Map Go types to their full GVKs.

1.  **Locate Definitions:**
    *   Find the imports used in the controller (e.g., `v1 "github.com/org/repo/api/v1"`).
    *   Navigate to that package directory (usually `api/` or `pkg/apis/`).
2.  **Extract Group/Version:**
    *   Look for `groupversion_info.go`, `doc.go`, or `register.go`.
    *   Look for `GroupName = "..."` or `GroupVersion = ...`.
    *   *Knative Exception:* Check for `InternalGroupName` vs `GroupName` (public).
3.  **Standard K8s Types:**
    *   Map `appsv1.Deployment` -> `apps/v1/Deployment`.
    *   Map `corev1.Service` -> `core/v1/Service`.
    *   Map `batchv1.Job` -> `batch/v1/Job`.

## Common Patterns & Pitfalls

*   **Facet Controllers (Karpenter Pattern):** If a project splits a logical controller into many small sub-reconcilers (e.g., `Launch`, `Termination`, `Health`), **aggregate them** into a single Controller node in the graph to avoid noise, unless they run as totally separate processes.
*   **Deep Helpers (Knative Pattern):** Key resource creation often happens in helper files (e.g., `reconcile_resources.go`). Ensure analysis includes these files.
*   **Status vs. Spec:** Distinguishing `Update` (Spec) from `Status().Update` (Status) is critical for loop detection.
*   **Recorder:** `recorder.Event(...)` calls are useful for debugging but usually do not constitute a structural graph dependency.

## Example LLM Prompt Structure

To request this analysis in the future, use a prompt like this:

> "Analyze [Project Path].
> 1. **Discovery:** Use `find . -name "*controller.go"` to locate all controllers.
> 2. **Verification:** Check `cmd/` main files to ensure you haven't missed any registered controllers.
> 3. **Topology:** For each controller, identify Primary (`For`) and Secondary (`Watches`, `Owns`) triggers.
> 4. **Interaction:** Analyze `Reconcile` and *all called helper functions* to find `client.Get/List` (Reads) and `client.Create/Update/Patch/Delete` (Writes).
> 5. **Resolution:** Map all Go types to full GVKs.
> Output a JSON graph with nodes (Controllers, Resources) and edges (Watches, Reads, Writes)."