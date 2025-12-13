
**Sieve: Automatic Reliability Testing for Cluster Management Controllers**

**Paper:** *Automatic Reliability Testing For Cluster Management Controllers* (OSDI '22) 1

Sieve is a tool designed to automatically test the reliability of Kubernetes controllers by identifying "buggy corners" in their logic. It operates by analyzing a **Reference Trace** (a recording of a successful, normal execution) and generating **Test Plans** based on three specific fault patterns.

## **1\. Intermediate State (Crash Consistency)**

Goal: to verify if a controller can recover safely when it crashes in the middle of a multi-step reconciliation loop, preventing atomicity violations.

**The Strategy**
* **Identification:** Sieve identifies reconciliation loops in the reference trace where the controller issues multiple distinct state updates ($U_1, U_2, ..., U_n$).

* **Plan Generation:** It generates a test plan to crash the controller precisely after update $U_i$ but before $U_{i+1}$.

* **Verification:** Upon restart, the controller observes the "intermediate state" (partial updates applied) and must reconcile it correctly.

Mechanism (Fault Injection)
Sieve does not rely on imprecise external timing. Instead, it uses Automated Instrumentation:

* **Build-Time Injection:** Sieve modifies the source code of the controller’s client library (e.g., client-go) during the build process.

* **The Crash Hook:** It inserts a hook into state-changing functions (e.g., Update, Create). When the controller executes the target update $U_i$, this hook reports to the Sieve coordinator (an external service).

* **Trigger:** Once the update is confirmed successful, the injected hook forces the controller process to crash immediately (e.g., via panic), ensuring the crash happens exactly between  two updates.

---

**2\. Stale State (Time Travel)**

Goal
To verify if a controller acts correctly when its local view of the cluster lags behind the actual cluster state (e.g., acting on cached data that has since changed).

**The Strategy**

* **Identification:** Sieve looks for a causal pair $(N, U)$ where notification $N$ triggered action $U$.

* **Conflict Detection:** It scans for a subsequent notification $N'$ that conflicts with the action $U$.

* **Plan Generation:** The test coordinator pauses the controller’s view at $N$, waits for the cluster to progress to $N'$, and then reconnects the controller to a stale API server so it "time travels" back to seeing $N$.

**Heuristics & Conflict Definition**

* **Destructive Updates Only:** For the action $U$, Sieve currently **only considers deletions**. It ignores stale value updates, focusing on irreversible data loss.

* **Conflict Logic:** A conflict exists if executing the old deletion ($U$) against the new state ($N'$) would cause a state change (i.e., destroying a live object).

* **Offline Simulation (Pruning):** Sieve filters false positives by "simulating" the conflict offline. For example, if $U$ is "Delete Object X" and $N'$ implies Object X is already deleted, Sieve realizes that applying $U$ after $N'$ is a no-op and prunes the test plan.

---

**3\. Unobserved State (State Skipping)**

Goal
To verify if the controller is correctly Level-Triggered (reacts to the current state) rather than Edge-Triggered (relying on seeing every specific transition).

**The Strategy**

* **Identification:** Sieve scans for a sequence of notifications $(N, N')$ involving the same object where $N'$ **cancels or overwrites** the effect of $N$.

* **Plan Generation:** The coordinator blocks notification $N$ from reaching the controller. It unblocks the controller only after $N'$ has arrived. The controller wakes up seeing only $N'$, completely missing the state $N$.

**Heuristics & Scope**

* **Definition of Cancel:** $N'$ cancels $N$ if it overwrites the attribute changed in $N$ or deletes the object entirely (making $N$ obsolete). The notion of "overwrite" is not semantics-aware, rather, two subsequent state changes to a resource imply the second update "overwrites" the first regardless of the actual changes produced by the update.

* **Scope:** Unlike the Stale State rule, this strategy is not limited to deletions. It applies to any attribute change with respect to a given object (e.g., `attribute=A` -> `attribute=B`) provided the controller cared about the intermediate state where `attribute=A`. In practice, Sieve only applied this technique to resource deletion scenarios (e.g. preventing a controller from observing a `deletionTimestamp` before the object is fully removed from the cluster state).

* **Causality Constraint:** Sieve only generates a plan for skipping $N$ if the reference trace shows the controller **reacted** to $N$ (issued a causally related update). If the controller ignored $N$ in the baseline, Sieve assumes skipping it is safe and prunes the test plan.
