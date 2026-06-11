# C4 — Cross-XR ownership theft of shared composed resource (NEGATIVE RESULT — Kamera SSA fidelity gap)

**Issue:** [crossplane/crossplane#7224](https://github.com/crossplane/crossplane/issues/7224) (C4 sub-claim, closed by author 2026-04-09)
**Status:** 🟡 Closed by author. **Identified as a Kamera SSA-fidelity bug; harness fix not yet implemented; not pursued in SPRINT-0001 re-evaluation.**

## TL;DR

The original SPRINT-0001 baseline reported that when two XRs are configured to compose the same target resource, the harness produced a 55/45 split on which XR ends up "owning" the resource — an apparent ordering-sensitive ownership-theft race in Crossplane's composition logic. Maintainer review (jbw976) identified this as a Kamera SSA (server-side apply) fidelity bug: the harness's SSA implementation handles field-manager merge semantics incorrectly, while real Crossplane's `meta.AddControllerReference()` rejects cross-XR theft via the API server's actual conflict detection. The harness fix ("Fix 3 — SSA field-manager conflict detection") was scoped as planned work but is not yet implemented; C4 was therefore not re-evaluated under SPRINT-0001.

## What was originally claimed

Scenario: two XRs (`xr-foo`, `xr-bar`) are each configured by their respective Compositions to compose a `ConfigMap` with the same name in the same namespace. The Crossplane composite reconciler applies these via SSA with each XR's controller reference attached. Original baseline showed 55/45 across trials on which XR's controllerRef ended up on the shared ConfigMap — looking like a race where whichever XR reconciled second silently overwrote the first XR's ownership.

The framing in the original report was "Crossplane's composition logic fails to detect or reject cross-XR ownership conflicts."

## Why this was a Kamera bug, not a Crossplane bug

Real Kubernetes' SSA implementation uses **field manager** tracking. Each apply operation declares a field manager name; the API server records, per field, which manager last set it. When a different manager attempts to write a conflicting value, the API server returns a `Conflict` error unless the apply explicitly opts into `force=true`.

Crossplane's `meta.AddControllerReference()` (in xpkg utility code) sets the XR's UID as `controller=true` on the composed resource. If a second XR tries to add itself as a controller via the same code path, real K8s rejects the apply because there's already a `controller=true` ownerReference and the field manager conflict is detected.

The harness's SSA implementation at the time of the SPRINT-0001 baseline did not model field-manager-level conflict detection. It treated SSA as a structured patch with field-level merge but no field-ownership tracking. So when the second XR's reconcile applied its `controllerRef`, the harness silently overwrote the first XR's `controllerRef` instead of returning a Conflict.

Result: the harness saw the ownership-theft happen 100% of the time when ordered second-XR-after-first-XR; combined with the inverse ordering it produced the 55/45 split. Real Crossplane on a real cluster would have rejected the second apply with a Conflict and surfaced an error.

## Why C4 was not re-evaluated under SPRINT-0001

The harness fix for SSA field-manager conflict detection was named "Fix 3" and scoped in [`docs/plans/2026-03-25-patch-semantics-fidelity.md`](../plans/2026-03-25-patch-semantics-fidelity.md) (referenced from the C2 internal-validation doc). It was not implemented during SPRINT-0001 because:

- C4 was already closed by the issue author after the maintainer's fidelity diagnosis. There was no upstream-update pressure.
- Fix 3 is a non-trivial harness change (full field-manager bookkeeping with conflict semantics). It was prioritized below the F3/F5/F6 re-runs and the Tier 3 cluster pass.
- C4-class scenarios are not currently in the harness's active scenario set, so the unfixed SSA fidelity gap doesn't affect any other SPRINT-0001 finding.

C4 is therefore documented as a negative result with a known pending harness fix. Re-investigation of C4-class scenarios (cross-XR ownership theft, shared composed resources, multi-controller field conflicts) is gated on Fix 3 landing.

## Why this is in the talk

Same reasons as C2 — it's the second negative result in the SPRINT-0001 set, and being honest about it is part of the methodology:

1. **Different fidelity issue from C2.** C2 was a DELETE-semantics issue (finalizer clobbering); C4 is an SSA-semantics issue (field-manager conflict detection). Surfacing both shows the harness has had multiple distinct fidelity issues identified through maintainer review.
2. **Harness work is open.** Unlike C2 (fix landed as `e4daf33`), C4's fix is still planned. This is a fair "ongoing work" disclosure.
3. **C4-class scenarios are interesting if Fix 3 lands.** Cross-XR field conflicts on shared composed resources are exactly the kind of thing a model-checking-style harness should be good at finding. The current state is "we can't trust the result"; the future state, post-Fix-3, is "we can run these scenarios with confidence."

## Talking points for the audience

- Two negative results in a six-claim batch is honest. The methodology says "real controllers + real APIs"; when a fidelity gap is identified, the right move is to fix the harness, not defend the finding.
- The split between C2 and C4 is instructive: C2 was a one-line-fix-able simulation oversight (DELETE wasn't a patch); C4 is a deeper semantic gap (field-manager bookkeeping doesn't exist in the harness). The cost of fixing each maps to how foundational the fidelity issue is.
- C4 is currently shelved, not abandoned. If a maintainer or community member is interested in cross-XR ownership conflict scenarios specifically, the harness work needed (Fix 3) is well-scoped and would unlock that scenario family.
- This entry, paired with C2's, is the "we're not infallible" moment. Worth making early in the talk so the positive findings (F1, F3, F5, F6) land with appropriate credibility.
