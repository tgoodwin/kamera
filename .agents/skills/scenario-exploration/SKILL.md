---
name: scenario-exploration
description: Analyze Kamera scenario runs to extract grounded bug findings. Use this after running a workflow scenario to systematically check convergence, compare orderings, and produce evidence-backed bug reports. Enforces the distinction between observed trace evidence and inferred hypotheses.
metadata:
  short-description: Analyze scenario dumps and produce evidence-grounded bug reports
---

# Scenario Exploration Skill

Use this skill to analyze the results of a Kamera scenario run. The output is
an evidence-grounded bug report that clearly distinguishes what was **observed
in the trace** from what was **inferred from code analysis**.

## Prerequisites

- A completed scenario run with output in a dump directory
- The workflow JSON that produced it
- Access to `campaign-metrics` and `inspect exploration` CLI tools

## Run Modes

Every harness run is one of three modes, controlled by flags:

| Mode | Flags | Behavior |
|------|-------|----------|
| **Reference** | `--closed-loop=false --no-perturbations` | Single run with all perturbations stripped. Use to get a clean baseline from a JSON that already has `permuteControllers` configured. |
| **Exploration** | `--closed-loop=false` (default when `--closed-loop` absent) | Single run with the JSON config applied as-is, including any `permuteControllers`, `stalenessIntervals`, etc. This is the primary agent-driven mode. |
| **Closed-loop** | `--closed-loop=true` (default) | Runs a reference phase (perturbations stripped) followed by auto-generated perturbation plans derived from the reference trace. |

> **Agent workflow:** For hypothesis-driven re-exploration (Phase 6), use **Exploration mode** (`--closed-loop=false`).
> Configure the perturbations in the JSON variant file and run it directly. Use **Reference mode**
> (`--closed-loop=false --no-perturbations`) when you want a baseline trace from the same JSON file without
> manually removing the perturbation fields.

## Core Principle: Evidence Before Conclusions

Every claim in a bug report must be traceable to one of two sources:

1. **Trace evidence** -- something directly observable in the dump file
   (a specific step, state hash, controller ID, effect, error message)
2. **Code analysis** -- reasoning about source code behavior that was NOT
   observed in the trace

These two categories must NEVER be mixed without explicit labeling. A finding
that says "ordering X succeeds" must either point to a trace path where it
succeeded, or be clearly marked as "[inferred from code]" with links to the source code.

**Why this matters:** Kamera's value is that it produces concrete execution
traces, not speculation. A report that mixes observed and inferred findings
undermines trust in the tool. Readers should be able to verify every claim
by inspecting the dump.

## Phase 1: Convergence Assessment

Run campaign-metrics on every jsonl file in the output directory:

```bash
go run ./cmd/kamera analyze campaign-metrics <output-dir>/*.jsonl
```

Record, for each invocation (reference, rerun, staleness phases):

| Metric | Value |
|--------|-------|
| Unique node visits | |
| Total node visits | |
| Unique resource states | |
| Converged states | |
| Aborted states | |
| Max-depth aborted states | |

### Convergence decision tree

```
All states converged?
├── Yes → proceed to Phase 2 (compare converged states)
└── No
    ├── Some converged, some aborted → proceed to Phase 2 with partial data
    └── Zero converged
        ├── All max-depth aborted → double maxDepth, re-run (up to 2-3x)
        └── All non-max-depth aborted → investigate why and flag to user
```

**Cycling detection:** If `total node visits / unique node visits > 3`, the
exploration is cycling. Increasing depth will not help. Diagnose the cycle:
- Unconditional API writes (known pattern)
- Error re-enqueue loops
- Feedback cycles between controllers

## Phase 2: State Comparison

If there are multiple converged states, compare them:

```bash
go run ./cmd/kamera analyze diff <output-dir>/*.jsonl
```

Key questions:
1. How many distinct converged states exist?
2. If multiple: which objects differ between them?
3. Which controller last wrote the differing objects?

If all converged states are identical, no ordering-dependent divergence exists
for this scenario configuration.

## Phase 3: Ordering Analysis

This is the core of Kamera's value: determining whether different controller
orderings produce different outcomes.

### What constitutes an ordering-dependent bug?

An ordering-dependent bug exists when:
- **Two or more orderings are explored** (via controller permutation)
- **At least one ordering produces a different outcome** than another

"Different outcome" means one of:
- Different converged state (objects have different final values)
- One ordering converges, another errors or cycles
- One ordering produces an error, another succeeds

### Grounding ordering claims in evidence

For every ordering discussed in the report, cite the specific trace path:

```
Ordering: [ControllerA, ControllerB, ControllerC]
Path: state[0].paths[0]
Steps: 0 (ControllerA) → 1 (ControllerB) → 2 (ControllerC)
Outcome: converged at step 2, final state hash abc123
```

or:

```
Ordering: [ControllerC, ControllerA, ControllerB]
Path: state[0].paths[1]
Steps: 0 (ControllerC) → error: "no compatible revisions found"
Outcome: error-terminated at step 0
```

### If an ordering was NOT explored

If you believe a particular ordering would produce a different outcome but it
does not appear in the trace, you MUST label this as an inference:

> **[Inferred from code, not observed in trace]:** If ControllerB ran before
> ControllerA, the revision would be validated before selection, avoiding the
> error. This ordering was not explored because [reason].

Common reasons an ordering wasn't explored:
- The reference run didn't converge, so no rerun was generated
- The controller wasn't triggered (no watch registration for the event)
- Max depth was reached before the ordering could complete
- The scenario tuning didn't include the controller in `permuteControllers`

## Phase 4: Bug Report Construction

### Required sections

Every bug report must include:

#### 1. Summary
One paragraph. What was found and how it relates to the bug patterns Kamera is designed to detect.

#### 2. Evidence (trace-grounded)
What Kamera actually showed. Include:
- Campaign metrics (verbatim, in code blocks)
- Specific trace paths that demonstrate the finding
- State hashes, step numbers, controller IDs, error messages
- For ordering bugs: at least two paths with different outcomes

#### 3. Mechanism (code analysis)
How the bug works at the source code level. Include:
- File and line references in the controller source
- The specific assumption that is violated
- The code path that leads to the incorrect behavior

**This section must be clearly separated from Evidence.** It is interpretation
of the code, not something the trace proved.

#### 4. Unverified hypotheses
Anything you believe to be true but could not observe in the trace. Each must
include:
- The hypothesis
- Why it could not be verified (e.g., reference didn't converge, ordering not
  explored, max depth reached)
- What would need to change to verify it (e.g., fix Bug #1 to enable
  convergence, add a watch registration, increase depth)

#### 5. Production impact
How this manifests in a real cluster. Be specific about:
- Whether controller-runtime's backoff/retry resolves it (transient vs permanent)
- What the user-visible symptom is (error events, status conditions, latency)
- What the blast radius is (single resource vs all resources of a kind)

#### 6. Suggested fix
Concrete code change, with the caveat that it's based on code analysis and
has not been tested.

### Severity rubric

| Severity | Criteria |
|----------|----------|
| P0 | Silent data corruption or security issue. No error visible to user. |
| P1 | Permanent failure requiring manual intervention. |
| P2 | Transient failure that self-resolves but causes unnecessary latency/churn. |
| P3 | Cosmetic or efficiency issue (unnecessary API calls, extra reconcile loops). |

### What NOT to do

- Do not claim "Kamera proved X" if X was not observed in the trace
- Do not claim an ordering "succeeds" if no trace path shows that ordering
  reaching a converged state
- Do not mix trace evidence and code analysis in the same paragraph without
  labeling which is which
- Do not describe a bug as "confirmed" unless a trace path demonstrates the
  incorrect behavior (an error message, a wrong object state, or divergent
  converged states)
- Do not assume that a scenario with zero converged states has no findings --
  error patterns, cycling behavior, and partial traces are all evidence

## Phase 5: Re-run Comparison

When re-running a scenario after code changes, append a new dated section
rather than overwriting. The section must include:

1. **What changed in the codebase** since the previous run (relevant commits)
2. **Campaign metrics comparison** (table showing before/after)
3. **Whether previous findings still hold** -- explicitly, for each finding
4. **New findings** from the deeper/different exploration
5. **Previously-reported issues now resolved** by the code changes

## Phase 6: Hypothesis-Driven Re-Exploration

This phase is optional. Enter it when Phase 4 produces **unverified hypotheses that can be
tested by modifying scenario tuning** — as opposed to hypotheses that require code fixes
before they can be verified.

> **Context:** The Kamera pipeline has an automatic perturbation stage (`parallel_runner.go`
> planFn) that generates staleness/ordering reruns programmatically from reference runs. This
> phase gives an agent a manual, targeted path to the same configuration surface: use it when
> you have a specific hypothesis the automatic stage didn't cover, or when you want to probe
> a targeted race condition directly.

### Step 1: Classify each unverified hypothesis

For each unverified hypothesis from Phase 4, determine which perturbation type can test it:

| Hypothesis type | Perturbation |
|----------------|--------------|
| Ordering X before Y produces different outcome | `permuteControllers` — **required base**; specifies which controllers to permute |
| Race only relevant after a specific event fires (e.g., CREATE of a revision) | `permuteAfterEvent` — optional scoping modifier |
| Race only relevant within a bounded depth range | `permuteDepthRange` — optional scoping modifier |
| Controller C sees stale data for kind K during window [A, B) | `stalenessIntervals` |
| User action fires at the wrong point in the execution | `userActionReadyDepths` |
| State space not fully sampled (non-determinism suspected) | `search.monteCarlo` |

> **Permutation scoping:** `permuteControllers` is required to enable any ordering permutation —
> it specifies *which* controllers are permuted. `permuteAfterEvent` and `permuteDepthRange` are
> optional scoping modifiers that narrow *when* permutations are active. They compose with AND
> logic: when both are set, permutations activate only when the trigger event has fired AND the
> current depth is within range. Omitting both means permutation is always active (default).

If a hypothesis requires a code change before it's testable (e.g., a prerequisite bug must
be fixed first), label it **blocked** and skip it.

### Pre-flight checklist (run before any scenario)

Before running or re-running a scenario, verify:

1. **Are all relevant controllers in `permuteControllers`?** If the hypothesis involves
   controller C running before controller D, both must be listed. Missing a controller
   is the most common reason an ordering is never explored. Note: `permuteAfterEvent`
   and `permuteDepthRange` only scope *when* permutations are active — they have no
   effect if the relevant controllers are absent from `permuteControllers`.
2. **Does the user action fire?** If `userInputs` is non-empty but `userActionReadyDepths`
   is missing, the action fires at convergence. If the reference run cycles, the action
   never fires. Add `"userActionReadyDepths": {"0": 0}` to fire immediately.
3. **Is the staleness window active?** Check that `staleAt` < initial KindSequence <
   `catchUpAt` for the target kind. If the initial KindSequence is already >= `catchUpAt`,
   the staleness window is expired before the run starts.
4. **Is `maxDepth` sufficient?** If all states are max-depth aborted with low cycling
   ratio (< 3x), increase depth. If cycling ratio is high, depth won't help.

### Step 2: Read KindSequences from the reference trace

`staleAt`/`catchUpAt` values in `stalenessIntervals` are **KindSequence values** — per-kind
write counters, not step numbers. Each step in the dump records `KindSeqBefore` and
`KindSeqAfter` for every kind.

Find when a specific kind's sequence advances (i.e., a write occurred):

```bash
# Replace GROUP/Kind with e.g. "apiextensions.crossplane.io/Composition"
jq '.states[0].paths[0][] | select(.KindSeqAfter["GROUP/Kind"] > .KindSeqBefore["GROUP/Kind"]) | {controllerId, KindSeqBefore, KindSeqAfter}' dump.jsonl
```

Set `staleAt` to the KindSeq value at the **start** of the window you want to make stale,
and `catchUpAt` to the KindSeq value where staleness should resolve.

- `lag: -1`: reconciler is frozen at the `staleAt` snapshot
- `lag: N`: reconciler sees `frontier - N` (sliding window)

For `userActionReadyDepths`, `{"0": N}` fires user action 0 at DFS depth N. Read the step
index from the reference trace at which the action should fire to produce the hypothesized
race.

For `permuteAfterEvent`, find the event that opens the race window:

```bash
# Find all CREATE effects and their kinds
jq '.states[0].paths[0][] | .effects[]? | select(.opType == "CREATE") | {opType, kind: .key.identityKey}' dump.jsonl
```

Set `opType` and `kind` (canonical `group/Kind` format) to the event that should trigger
permutation. Permutations are suppressed before this event and active after it.

For `permuteDepthRange`, identify the depth range where the race occurs:

```bash
# Find which depths produce the interesting divergence
jq '.states[0].paths[0][] | {depth: .depth, controllerId, effectCount: (.effects | length)}' dump.jsonl
```

Set `min` and `max` to bracket the interesting depths. This avoids wasting exploration
budget on deterministic early steps or cycling noise at deeper depths.

Find valid controller IDs from the dump:

```bash
jq '.states[0].paths[0][].controllerId' dump.jsonl | sort -u
```

### Step 3: Create a hypothesis variant file

**Do not modify the original scenario file.** Create a new variant:

```
<scenario-name>-hypothesis-1.json
<scenario-name>-hypothesis-2.json
```

Edit the `tuning` section. All perturbation fields are supported:

```json
"tuning": {
  "maxDepth": 200,
  "permuteControllers": ["ControllerA", "ControllerB"],
  "permuteAfterEvent": {
    "opType": "CREATE",
    "kind": "GROUP/Kind"
  },
  "permuteDepthRange": {"min": 1, "max": 10},
  "stalenessIntervals": [
    {
      "reconciler": "ControllerA",
      "kind": "GROUP/Kind",
      "staleAt": 3,
      "catchUpAt": 7,
      "lag": -1
    }
  ],
  "userActionReadyDepths": {"0": 5},
  "search": {
    "mode": "monte_carlo",
    "monteCarlo": {"seed": 42, "trials": 20}
  }
}
```

**`permuteControllers`** (required to enable ordering permutation): list of controller IDs
whose scheduling order is permuted during exploration. Without this, the exploration follows
a single deterministic order.

**Optional scoping modifiers** (all default to unconstrained; AND logic when combined):

- `permuteAfterEvent`: Only start permuting after an effect matching `opType` + `kind`
  is observed on the current exploration path. Implemented as a one-shot boolean (`permuteTriggered`)
  on each `StateNode` — once fired, it is inherited by all descendant states and included in
  state serialization so deduplication is trigger-aware. Use when the race window only opens
  after a specific event (e.g., "only permute after a CompositionRevision is created").

- `permuteDepthRange`: Only permute within depths `[min, max]` (inclusive). Acts as an
  off-switch for permutation outside the interesting depth range. Use when the race occurs in
  a known depth window and you want to avoid wasting exploration budget on uninteresting early
  steps or cycling noise at deeper depths.

- `permuteAfterEvent` + `permuteDepthRange` (combined): Permute only when BOTH the trigger
  event has fired AND the current depth is within range. Useful when the race window is both
  event-gated and depth-bounded (e.g., "start permuting after the CompositionRevision is created,
  but stop at depth 15 before cycling begins").

**Planned:** `permuteForSteps` — (not yet implemented) a relative window alternative to `permuteDepthRange.max`. After
`permuteAfterEvent` fires, permute for at most N additional steps, then stop. This is more robust
than an absolute depth bound when the trigger event's depth varies across branches.

### Step 4: Re-run and compare

```bash
# Exploration mode: run with perturbations as configured in the hypothesis JSON
go run <path/to/harness> --closed-loop=false --inputs <hypothesis-variant.json> --output <new-dir> --interactive=false

# Reference mode: run the same JSON with perturbations stripped (for baseline comparison)
go run <path/to/harness> --closed-loop=false --no-perturbations --inputs <hypothesis-variant.json> --output <ref-dir> --interactive=false
```

Then compare:

```bash
# Check convergence
go run ./cmd/kamera analyze campaign-metrics <new-dir>/*.jsonl

# Compare converged states
go run ./cmd/kamera analyze diff <new-dir>/*.jsonl

# Run last-write analysis if states differ
go run ./cmd/kamera analyze report <new-dir>/*.jsonl
```

### Step 5: Update the bug report

For each hypothesis, record the outcome explicitly:
- **Confirmed:** cite the trace path and state hash that demonstrates the bug
- **Refuted:** cite the trace evidence showing the expected behavior holds
- **Inconclusive:** state what prevented verification (e.g., still hitting max depth,
  hypothesis requires a prerequisite fix)

Iterate per-hypothesis until each one is resolved. There is no fixed limit on the number
of hypotheses or iterations.

## Quick Reference: Useful Commands

```bash
# Convergence check (always run first)
go run ./cmd/kamera analyze campaign-metrics <dump.jsonl>

# DAG visualization
go run ./cmd/kamera inspect exploration <dump.jsonl> --interactive=false

# Converged state comparison
go run ./cmd/kamera analyze diff <dump.jsonl>

# Last-write analysis for differing objects
go run ./cmd/kamera analyze report <dump.jsonl>

# Inspect a specific step
jq '.states[0].paths[0][N]' <dump.jsonl>

# Find all controllers that ran in a path
jq '.states[0].paths[0][].controllerId' <dump.jsonl>

# Get the final converged state objects
jq '.states[0].state.contents.objects' <dump.jsonl>
```
