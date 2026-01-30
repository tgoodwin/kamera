# Coverage Strategy

## Objective
To systematically generate inputs and exploration configurations that maximize the likelihood of exposing bugs (race conditions, stale read sensitivities, logic errors) in complex Kubernetes control planes. This strategy aims to be generalizable across different project domains (Knative, Crossplane, Karpenter, etc.).

## Core Components

The strategy relies on three primary components:

### 1. The Dependency Graph (Static View)
We model the control plane as a directed bipartite graph:
*   **Nodes** are either **Controllers** (Agents) or **Resources** (GVKs).
    *   *Decision:* We do *not* split nodes into `Spec`/`Status` sub-nodes to avoid exploding the graph size.
*   **Edges** represent directed interactions, heavily annotated with attributes to capture the nuance needed for bug finding:
    *   **Reconciles** ($C \to R$): The primary event source.
    *   **Watches** ($C \to R$): Secondary triggers.
    *   **Owns** ($R_{parent} \to R_{child}$): Lifecycle and GC relationships.
    *   **Reads** ($C \to R$): Annotated with `Target: Spec|Status`.
    *   **Writes** ($C \to R$): Annotated with `Target: Spec|Status`.

*Goal:* Automatically infer this graph (via LLM or static analysis) to identify "Interaction Hotspots."

As of 1/26/26 I am using an LLM to do this analysis and produce a DOT-format graph, which can be manually verified against the project source code.

### 2. Interaction Hotspots (Semantics-First)
We identify hotspot classes based on interaction semantics (what can go wrong),
using graph predicates as detection rules. These categories are *not* mutually
exclusive; a subgraph can be tagged by multiple hotspot classes.

#### Multi-Writer Contention
*   **Pattern:** Two distinct controllers write the same resource.
*   **Graph predicate:** $\exists C_1 \neq C_2, R$ s.t. $C_1 \xrightarrow{writes} R$ and $C_2 \xrightarrow{writes} R$.
    *   Optional refinement: both target `Status`, or both target `Spec`.
    *   Ownership variant: two different parents own (or write) the same child.
*   **Bug pattern:** Thrashing, last-writer-wins, status fights.

#### Missing Trigger / Stale Read
*   **Pattern:** A controller reads a resource but does not watch it.
*   **Graph predicate:** $C \xrightarrow{reads} R$ and **no** $C \xrightarrow{watches} R$ edge.
    *   Escalation: another controller writes that resource ($C_2 \xrightarrow{writes} R$).
*   **Bug pattern:** Stale configs, missed reconciles, non-convergence.

#### Fan-out with Converging Writes (Order Sensitivity)
*   **Pattern:** One upstream resource triggers multiple controllers whose effects converge.
*   **Graph predicate:** $R_{start}$ watches/reconciles into $C_1$ and $C_2$, and both write the same downstream resource $R_{end}$ (or write resources that own/compose into $R_{end}$).
*   **Bug pattern:** Races and ordering sensitivity across controllers; different final states.

#### Aggregation / Join Controller
*   **Pattern:** A controller reads many resources to compute one output.
*   **Graph predicate:** $|\{R_i : C \xrightarrow{reads} R_i\}| > 1$ and $C \xrightarrow{writes} R_{out}$.
*   **Bug pattern:** Partial updates, inconsistent snapshots, dropped inputs.

#### Feedback Cycle
*   **Pattern:** Writes feed back into triggers in a cycle (self or multi-controller).
*   **Graph predicate:** A cycle exists where a write by $C_1$ causes a watch/reconcile that eventually causes a write back to a resource that triggers $C_1$.
*   **Bug pattern:** Oscillation, infinite reconciles, non-convergence.

#### Hotspot Attributes → Scenario Construction Hints
Each hotspot instance carries lightweight `attributes` to guide scenario construction. These are *hints*, not hard requirements. The goal is to decide (a) which objects to include, (b) which controllers to place in the initial pending set, and (c) which explore parameters to tune (permutations, staleness).

**Multi‑Writer (`target=spec|status|any`)**
* **Resources:** include the shared resource `R`.
* **Pending reconciles:** at least the writer controllers.
* **Explore:** set `PermutationScope` to those writers; if `target` is `spec` or `status`, bias mutations toward that surface.

**Missing Trigger (`missing_trigger=true|false`, `writers=...`)**
* **Resources:** include `R` plus any listed writers’ output resources.
* **Pending reconciles:** the reader controller; if `writers` present, include writers too.
* **Explore:** inject `StaleReads` for `R` (reader side). If writers are present, permute writer vs reader ordering.

**Fan‑Out Converging Writes (`converges_via=direct|owns`)**
* **Resources:** include the trigger `R_start` and downstream `R_end`. If `owns`, include the parent/child relationship target.
* **Pending reconciles:** both fan‑out controllers (the converging pair).
* **Explore:** use `PermutationScope` across the converging controllers; `converges_via=owns` suggests adding parent/child objects to emphasize ordering effects.

**Aggregation / Join (`inputs=...`, `outputs=...`)**
* **Resources:** include all `inputs` and at least one `output`.
* **Pending reconciles:** the aggregator controller.
* **Explore:** staleness injection per input; partial‑update cases (subset of inputs present/updated).

**Feedback Cycle (`cycle_size=N`)**
* **Resources:** include cycle nodes and their triggering edges.
* **Pending reconciles:** at least one controller in the cycle; optionally all controllers in the cycle if the goal is to expose oscillation quickly.
* **Explore:** increase `MaxDepth` (or detect loops early); use permutations within the cycle to expose order sensitivity.

### 3. Dimensions of Variation
To test these interaction hotspots, we define "Dimensions" along which we can sweep.

#### A. Structural Dimensions (Shape)
Variations in the initial object graph $S_{init}$.
*   **Cardinality:** 1 vs N instances of a resource.
*   **Depth:** Length of dependency chains.
*   **Overlap:** Disjoint vs. Overlapping selectors/names.

#### B. Spec Dimensions (Config Values)
Parametric fuzzing of valid CR specs.
*   **Enumerations:** `mode: Proxy | Serve`, `protocol: HTTP | GRPC`.
*   **Boundaries:** `replicas: 0 | 1 | MaxInt`.
*   **Presence:** Optional fields set vs unset.

#### C. Temporal Dimensions
In addition to generating the *input state*; we generate an *exploration strategy* for that state.
*   **Order Permutation:** If the topology introduces risks of race condition, we strictly permute the scheduling order of the intermediate controllers.
*   **Staleness Injection:** If the System Graph shows $C_1$ reads $R$ which $C_2$ writes, we explicitly inject stale views of $R$ into $C_1$'s Reconcile loop.

## The Generation Framework

### Phase 1: Discovery
Scan the `ExplorerBuilder` or `Scheme` to build the **System Graph**.
*   *Input:* `ExplorerBuilder` (contains Reconcilers + Watches).
*   *Output:* A `ControllerGraph` object.

### Phase 2: Planning
Identify the **Interaction Topologies** to test.
*   *Input:* `ControllerGraph` + User constraints.
*   *Output:* A list of abstract `TestPlans`.
    *   *Example:* "Test Shared Child pattern between ServiceReconciler and RouteReconciler."

### Phase 3: Instantiation
Concretize the `TestPlan` into a Kamera `StateNode` and `ExploreConfig` by sampling from **Spec Dimensions**.
*   *Input:* `TestPlan`.
*   *Output*:
    1.  `InitialState`: The set of Kubernetes objects that will be reconciled by the system.
    2.  `Config`: Tuned `MaxDepth`, specific `Reorder` sets, or `StaleRead` policies.

## Universal Oracle & Failure Classification

Instead of writing manual assertions for every scenario, we define properties of the system execution that indicate failure.

### 1. The Instability/Liveness Bug (Convergence Failure)
*   **Signal:** `AbortedStates` > 0 with `Reason: MaxDepth`.
*   **Meaning:** The system entered a loop or a dead-end retry cycle that exceeded the step budget. It failed to stabilize within a reasonable timeframe (simulated time or step count).
*   **Implication:** A "Feedback Loop" topology might be oscillating, or a "Dependency Chain" is stuck waiting for a condition that never becomes true.

### 2. The Race Condition (Nondeterminism)
*   **Signal:** `len(ConvergedStates)` > 1.
*   **Meaning:** The final state of the cluster depends on the order in which controllers ran or the precise timing of events.
*   **Implication:** A "Diamond" or "Shared Child" topology has a critical section that is not properly guarded. Kubernetes controllers must be eventually consistent; multiple converged states violate this contract.

### 3. The Crash (Panic/Error)
*   **Signal:** `AbortedStates` > 0 with `Reason: Panic` or `Error`.
*   **Meaning:** Unhandled nil pointer dereference, type assertion failure, or explicit error return that stops reconciliation.

### 4. The "Stuck" State (Logic Error)
*   **Signal:** `len(ConvergedStates)` == 1 AND `Result.PendingReconciles` is empty, but some expected "goal" is unmet.
*   **Meaning:** The system thinks it is done, but the desired outcome hasn't happened.
*   **Mechanism:** When the `ScenarioEngine` generates a test case (e.g., "Scale to Zero"), it attaches a **Domain Assertion**.
    *   *What is it:* A predicate over the resulting state
    *   *Example:* `Check(func(root) { return root.Status.Replicas == 0 })`
    *   *Check:* `Check(func(root) { return root.Status.Conditions['Ready'].Status == True })`
    *   If the system converges (stops reconciling) but this check returns false, we classify it as a "Stuck State" bug.

## Search Strategy & Optimization

The Cartesian product of all Dimensions (Topology × Structure × Spec × Temporal) could be infinite, so we must efficiently sample this space.

### 1. Semantics-Aware Fuzzing
Instead of random bit-flipping, we use Schema awareness (OpenAPI/CRD validation rules):
*   **Boundary Value Analysis:** For integers (like `replicas`), we test `-1`, `0`, `1`, `MAX`.
*   **Enum Exhaustion:** We explicitly test all defined enum values.
*   **Required vs Optional:** We generate variants with only required fields vs. all optional fields populated.

### 2. Pairwise (Combinatorial) Testing
Most bugs are triggered by the interaction of just two parameters (e.g., "Protocol=GRPC" AND "Scale=0"). We can use pairwise testing algorithms (like All-Pairs) to cover all 2-way combinations of Spec Dimensions with significantly fewer test cases than exhaustive testing.

### 3. Topology-Guided Scoping
We filter the test generation based on the Topology being tested.
*   If testing the **Dependency Chain** ($A
ightarrow B$), we lock the parameters of $C$ and $D$ to known stable defaults.
*   We only inject **Staleness** or **Order Permutations** for the controllers specifically involved in the identified Topology Instance.

## Proposed Workflow

1.  **Analyze:**
    ```bash
    kamera coverage analyze ./cmd/controller-manager
    > Discovered 12 Controllers, 8 Resources.
    > Identified 3 "Diamond" patterns, 2 "Shared Child" patterns.
    ```

2.  **Plan:**
    ```bash
    kamera coverage plan --topology=Diamond
    > Generated 3 Test Plans:
    > 1. Service -> [Config, Route] -> ServiceStatus
    > ...
    ```

3.  **Generate:**
    ```bash
    kamera coverage gen --plan=1 --strategy=pairwise
    > Generated 15 Test Manifests (manifests/scenario_*.yaml)
    ```

4.  **Run:**
    ```bash
    kamera run manifests/
    > Executing 15 scenarios...
    > [FAIL] scenario_04: Race Condition detected (2 converged states).
    > [PASS] scenario_05
    > ...
    ```

## Integration with Kamera Model Checker

The `ScenarioEngine` produces a `TestManifest`:

```go
type TestManifest struct {
    Name         string
    InitialState tracecheck.StateNode
    // Tuning the model checker for this specific scenario
    Options      tracecheck.ExploreOptions
    // Optional domain-specific invariant check
    Invariant    func(tracecheck.StateNode) error
}

type ExploreOptions struct {
    // Focus ordering permutations only on these controllers
    PermutationScope []string
    // Inject staleness for these specific GVK reads
    StaleReads       []schema.GroupVersionKind
}
```
