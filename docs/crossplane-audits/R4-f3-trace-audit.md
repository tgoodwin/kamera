# R-4: F3 hypothesis-1 terminal-state XWidget condition trace audit

**Status:** ✅ AUDITED — **F3 SHIFTS, does not retract.** All 4 unique XWidget terminal hashes encode `Synced=False / reason=ReconcileError` with the original `errSelectComp` message ("cannot select Composition: no compatible Compositions found"). The bug behavior persists; the hash shift is explained by added `metadata` fields (`generation`, `resourceVersion`) that hash differently across paths but carry the same error condition.

**Threats addressed:** [F3-A1, F3-A2, F3-A3](../upstream-updates/AUDIT-threats-to-validity.md#f3-7222--already-retraction-candidate-threats-to-that-claim) (covers F3-Audit-1).

## Question

The hardened-harness re-run of `composition-deleted-while-xr-bound-hypothesis-1` produces 6 max-depth-aborted terminal states with 4 distinct `XWidget/default/example` hashes (`302b727`, `6b6a9d3`, `4390fb0`, `855c129`), none of which match the original baseline (`93d750b` / `9dc61c9`). The retraction in [draft 7222](../upstream-updates/7222-f3-composition-deletion.md) hinges on whether these new hashes are clean (XR converged, no permanent error) or carry the original F3 `ReconcileError` family — i.e. did the hash shift because the bug was fixed, or because cosmetic metadata changed while the same bug persists?

## Method

Dump file: `/tmp/crossplane-reeval-89acd8a/f3/hypothesis-1/crossplane_deletion_composition_deleted_while_xr_bound_hypothesi_0.jsonl` (~211 MB).

Despite the `.jsonl` extension, the file is a single pretty-printed JSON object. Top-level keys: `context`, `campaignMetrics`, `objects` (50 entries — a content-addressable table of every distinct object snapshot, keyed by full SHA-256 hash), `states` (6 entries — one per terminal state). Each state's `state.contents.objects[]` entry only carries `(key, hash)` pairs; the actual object content lives in the top-level `objects[]` table.

Procedure:

1. `jq` to extract `(state-id, XWidget-hash)` for each of the 6 terminal states. Confirmed the 4-distinct-hash inventory matches the runlog.
2. `jq` to look up the full object body in the top-level `objects[]` table for each unique hash prefix.
3. For each, extracted `status.conditions[]`, `spec.compositionRef`, `spec.compositionRevisionRef`, and `metadata.deletionTimestamp`.
4. Aggregated all `Synced=False` messages across every XWidget snapshot in the dump (not just terminals) to confirm the F3 message family.
5. Aggregated all `(type, status, reason)` tuples across every XWidget snapshot to confirm no other unanticipated condition shapes.

No scripts were committed; ad-hoc `jq` invocations from `/tmp`. The dump was not modified.

## Findings

### Per-hash condition table

| State ID(s) | XWidget hash (short) | `Synced` | `Ready` | `Responsive` | `compositionRef` | `compositionRevisionRef` | `deletionTimestamp` | `generation` / `resourceVersion` | Classification |
|---|---|---|---|---|---|---|---|---|---|
| `aborted-2azhpneh` | `302b727` | **False / ReconcileError** "cannot select Composition: no compatible Compositions found" | True / Available | True / WatchCircuitClosed | absent (cleared) | absent | none | 3 / 12 | **ReconcileError** |
| `aborted-2uhk5wqu`, `aborted-3jxjqcsk` | `6b6a9d3` | **False / ReconcileError** "cannot select Composition: no compatible Compositions found" | True / Available | True / WatchCircuitClosed | absent (cleared) | absent | none | 3 / 13 | **ReconcileError** |
| `aborted-2uxzakid` | `4390fb0` | **False / ReconcileError** "cannot select Composition: no compatible Compositions found" | True / Available | True / WatchCircuitClosed | absent (cleared) | absent | none | 4 / 19 | **ReconcileError** |
| `aborted-3ehm2of7`, `aborted-hz2c0tz0` | `855c129` | **False / ReconcileError** "cannot select Composition: no compatible Compositions found" | True / Available | True / WatchCircuitClosed | absent (cleared) | absent | none | 4 / 20 | **ReconcileError** |

All 4 unique hashes carry the **same** `Synced=False / ReconcileError` condition with the same message. The hash differences are entirely explained by `metadata.generation` (3 vs 4) and `metadata.resourceVersion` (12, 13, 19, 20) drift across paths.

`metadata.finalizers` is `["composite.apiextensions.crossplane.io"]` for all 4 — so the XR finalizer is intact.

### Cross-trace verification

Across **all** XWidget snapshots in the dump (not just terminals), the only Synced=False message is:

```
"cannot select Composition: no compatible Compositions found"
```

The original baseline had two error families:

- `errFetchComp`: "Composition not found" (CleanupReconciler-first ordering, 65 baseline paths).
- `errSelectComp`: "no compatible Compositions found" (CompositeReconciler-first ordering, 64 + 127 baseline paths).

The hardened re-run shows **only `errSelectComp`**. `errFetchComp` does not appear anywhere in this dump. This is consistent with the new `GarbageCollectorReconciler` reshaping the ordering space: only one of the two original error paths now reaches a max-depth terminal, but it does and it does not self-recover within the depth budget.

All unique condition tuples on XWidgets in the trace:

```
{ Ready,        True,  Available }
{ Synced,       True,  ReconcileSuccess }
{ Synced,       False, ReconcileError }
{ Responsive,   True,  WatchCircuitClosed }
```

No unanticipated condition types. The `Synced=False` does occur with `Ready=True` simultaneously on every terminal — i.e. **stale `Ready=True` while `Synced=False`** is present here too (consistent with R-3's finding that `Ready` is a system condition the controller skips when transitioning unknowns on the error path; the F6 stale-Ready pattern is independent of F3 and surfaces here as well).

### Other terminal-state objects

For the same 6 terminals:
- `Composition` is absent (the trigger event — Composition was deleted).
- `CompositionRevision` is **identical across all 6 terminals** (single hash `f353fca`).
- `ConfigMap/default/xr-config` is **identical across all 6 terminals** (single hash `709d71b`) — the original baseline showed this as differing/missing in some states.

So the `ConfigMap` collapse documented in the 7222 draft is real and still holds: in this re-run the dependent ConfigMap is present and identical across all six paths. The XR's `compositionRef` and `compositionRevisionRef` are both cleared on the way to the terminal. The XR retains a stale `resourceRefs[ConfigMap/xr-config]` pointing at the dependent.

## Classification

- **Clean:** 0 of 4 unique hashes.
- **ReconcileError (F3 `errSelectComp` family):** 4 of 4 unique hashes (covering all 6 terminals).
- **Other:** 0.

## Conclusion

**F3 shifts, does not retract.** The original "permanent ReconcileError loop after Composition delete with bound XR" pathology is reproduced in the hardened harness with the same `errSelectComp` message family. The hash drift between baseline and re-run is cosmetic (driven by `metadata.generation` / `resourceVersion` re-sequencing under the new GC reconciler), not a change in the underlying error condition.

The `errFetchComp` ("Composition not found") family does not appear in this dump. That is a real change — the new GC controller appears to suppress the ordering that previously reached the cleanup-first path within the depth budget. This should be characterized in the upstream comment as "narrowed to one of two original error families," not as evidence of retraction.

The `ConfigMap` collapse and the convergence of the `primary` variant remain genuine signals of fidelity-driven change. They are not, on their own, evidence that the XR-side bug is resolved.

## Implications for upstream-updates draft 7222

**Hold and rewrite.** The draft's current framing — "provisional retraction with a maintainer ask" — is now contradicted by trace-level evidence. Specifically:

- The draft says: "The XWidget hash drift is an open question. Without trace-level error-family verification, we can't distinguish 'new genuine bug' from 'GC-controller fidelity artifact.'" → **R-4 resolves this question: the XWidget hashes carry the original `errSelectComp` ReconcileError. The pathology persists.**
- The draft's Classification line ("shifts (with a strong retraction-candidate signal)") is closer to correct than the "Recommendation" body. The shift is the dominant story; the retraction-candidate signal was provisional and is now closed against retraction.
- The draft's "Suggested comment text" (paragraph beginning "Provisionally closing this as a retraction candidate") should be replaced with a "shifts" framing: the bug still reproduces with the same error family in 4/4 unique terminals, the original ConfigMap divergence has collapsed (the dependent is no longer GC'd in any path within the depth budget), and one of the two original error orderings is no longer surfaced — but the XR `Synced=False / ReconcileError` permanent loop is intact.

Recommended next actions (for tgoodwin's manual cascade — **not done by this audit**):

1. Edit `7222-f3-composition-deletion.md`:
   - Update "Classification" to **shifts (no retraction signal)**.
   - Replace the "Recommendation" and "Suggested comment text" sections with a "shifts" framing matching the F1/F5/F6 templates.
   - Move the `errFetchComp` absence into the evidence section as a narrowing observation, not a retraction signal.
2. Update [`AUDIT-threats-to-validity.md`](../upstream-updates/AUDIT-threats-to-validity.md) F3-A1/A2/A3 rows: A1 and A2 are now refuted by R-4 (the bug is real, not a clobbered-DELETE artifact and not a GC fidelity artifact). A3 stands as-is — the ConfigMap is no longer GC'd in this re-run, but that doesn't bear on the XR-side error loop.
3. R-14 (real-cluster F3 reproduction) becomes the highest-leverage remaining audit: a 5-minute observation of an XR after `kubectl delete composition` will give the strongest possible confirmation that this is a production-real loop and not a Kamera artifact.

## What's NOT addressed

- Whether real K8s production reaches the `errSelectComp` terminal as quickly as the harness, or has alternative recovery paths (e.g. CompositionRevisionRef pinning a still-cached revision, or human intervention re-creating the Composition). This is the R-14 question.
- Whether the `errFetchComp` family is unreachable in production with v2.2.0's GC behavior, or merely unreachable within the harness's depth budget under the new ordering. Either way it doesn't affect the retraction posture, since `errSelectComp` alone is a sufficient permanent-error pathology.
- The harness GC behavior on the ConfigMap (R-8 already documented that the harness GC is more eager than production K8s for multi-owner cases; here the ConfigMap has a single owner so the result is consistent with Background semantics, but the timing is synchronous-on-REMOVE rather than asynchronous).
