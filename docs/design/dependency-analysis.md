# Kubernetes Operator Analysis Guidelines

When generating static dependency graphs for Kubernetes controllers, follow this four-phase approach: **Discovery**, **Topology**, **Interaction**, and **Resolution**.

## Phase 1: Controller Discovery (The "Where")

Do not assume a standard directory structure. Use `ls -F` recursively to locate the control logic.

1.  **Search Heuristics:**
    *   **Standard Kubebuilder/Operator-SDK:** Look in `internal/controller/` or `controllers/`.
    *   **Knative-style:** Look in `pkg/reconciler/`.
    *   **Legacy/Custom:** Look in `pkg/controllers/` or `cmd/`.
2.  **File Identification:**
    *   Look for files named `controller.go` (often contains setup logic) and `reconciler.go` (often contains business logic).
    *   If split, `controller.go` usually defines **Watches**, while `reconciler.go` defines **Reads/Writes**.

## Phase 2: Topology Extraction (The "When")

Determine what triggers the controller. This defines the **Watches** edges. Locate the setup function (often named `SetupWithManager`, `Register`, `NewController`, or `addKnownTypes`).

1.  **Primary Trigger:**
    *   Look for `.For(&Type{})` or `controller.For(&Type{})`.
    *   *Graph Edge:* `Controller -> Watches (Primary) -> Resource`
2.  **Owned Resources (Garbage Collection triggers):**
    *   Look for `.Owns(&Type{})`.
    *   *Graph Edge:* `Controller -> Watches (Secondary) -> Resource`
3.  **Event Handlers (Cross-resource triggers):**
    *   Look for `.Watches(&Type{}, handler...)`.
    *   *Graph Edge:* `Controller -> Watches (Secondary) -> Resource`

## Phase 3: Interaction Analysis (The "What")

Analyze the `Reconcile` function (or `ReconcileKind` in Knative) to determine **Read** and **Write** edges. Scan the code for Kubernetes Client usage.

1.  **Read Detection:**
    *   **Keywords:** `client.Get`, `client.List`, `lister.Get`.
    *   *Context:* If the target object is *not* the Primary Trigger object, this is a **Read** dependency.
    *   *Graph Edge:* `Controller -> Reads -> Resource`
2.  **Write Detection:**
    *   **Keywords:** `client.Create`, `client.Update`, `client.Patch`, `client.Delete`.
    *   **Targeting:**
        *   **Spec:** `client.Update(ctx, obj)` (usually implies a Spec or Label update).
        *   **Status:** `client.Status().Update(...)` or `client.Status().Patch(...)`.
    *   *Graph Edge:* `Controller -> Writes (Target: Spec/Status) -> Resource`

## Phase 4: Type Resolution (The "Who")

Code uses Go types (e.g., `&v1alpha1.Promise{}`), but the graph requires GVKs (e.g., `platform.kratix.io/v1alpha1/Promise`).

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
*   **Dynamic Clients:** If `unstructured.Unstructured` is used, the GVK is often dynamic. Look for where `SetGroupVersionKind` is called to infer the target, or label the node generically (e.g., "Promise-defined CR") if strictly dynamic.
*   **Status vs. Spec:** Distinguishing `Update` (Spec) from `Status().Update` (Status) is critical for loop detection.
*   **Recorder:** `recorder.Event(...)` calls are useful for debugging but usually do not constitute a structural graph dependency unless they trigger another controller (rare).

## Example LLM Prompt Structure

To request this analysis in the future, use a prompt like this:

> "Analyze [Project Path]. Locate the controllers in `internal/controller` or `pkg/reconciler`. For each controller, identify:
> 1. The Primary Watch (`For`).
> 2. Secondary Watches (`Owns`, `Watches`).
> 3. Resources Read via client (`Get`, `List`).
> 4. Resources Written via client (`Create`, `Update`, `Status().Update`).
> Map Go types to their full GVKs. Output a JSON graph with nodes (Controllers, Resources) and edges (Watches, Reads, Writes)."
