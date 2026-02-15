# Backward-Trace Divergence Analysis Framework

**Date**: 2026-01-14
**Epic**: kamera-7tl
**Status**: Design complete, ready for implementation

## Problem Statement

When kamera exploration produces multiple converged states, we need to understand why they differ. Manual analysis of trial-1 revealed a complete causal chain from an initial "read your writes" race to final Endpoints state divergence, but the process was tedious and ad-hoc.

## Design Philosophy

**Backward-trace, not forward-trace.** Start from known divergence (final state differences) and trace backwards to root cause.

Why backward? Forward tracing from a divergence point produces noise from "diamond" patterns—paths that diverge then reconverge on objects that don't affect the final outcome. Backward tracing ensures we only investigate divergences that actually matter.

## Architecture

Three composable modules in `pkg/analysis/`:

```
┌─────────────────────────────────────────────────────────────┐
│                     dump.jsonl                              │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  Module 0: Converged State Diff (kamera-pwm)                │
│  Answers: "In what ways do the final states differ?"        │
│  Output: List of objects with different hashes across states│
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  Module 1: Last Write Analysis (kamera-eja)                 │
│  Answers: "What did the reconciler see when it wrote this?" │
│  Output: Last write step + reconciler's input state per path│
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  Module 2: Object Lifecycle Analysis (kamera-gwc)           │
│  Answers: "Does this value appear elsewhere in this path?"  │
│  Output: All steps where object had target hash             │
└─────────────────────────────────────────────────────────────┘
```

## Composition Flow

1. **Run Module 0** to identify which objects differ between converged states
2. **Run Module 1** on each differing object to find:
   - Which step produced the final value in each path
   - What the reconciler saw as input at that step
   - Diff of inputs across paths
3. **If inputs differ, run Module 2** to check "does the 'missing' state appear later in this path?"
4. **Interpret results:**
   - If missing state never appears → state dependency (that state wasn't reachable in this path)
   - If missing state appears later but reconciler didn't run again → watch/trigger configuration issue

## Module Specifications

### Module 0: Converged State Diff

```go
// Input: dump.jsonl
// Output: Objects that differ between converged states

type ConvergedStateDiff struct {
    NumStates        int
    DifferingObjects []ObjectDiff
    IdenticalObjects []ObjectKey
}

type ObjectDiff struct {
    Key     ObjectKey
    ByState map[string]VersionHash  // stateID → hash
}

func DiffConvergedStates(dump *DumpOutput) *ConvergedStateDiff
```

### Module 1: Last Write Analysis

```go
// Input: dump + differing object keys
// Output: Last write step + input state per path

type LastWriteAnalysis struct {
    Object  ObjectKey
    ByPath  []PathLastWrite
}

type PathLastWrite struct {
    PathIndex     int
    StateID       string
    FinalHash     VersionHash
    LastWriteStep LastWriteStep
}

type LastWriteStep struct {
    StepIndex    int
    ControllerId string
    StateBefore  []ObjectVersion
}

func AnalyzeLastWrite(dump *DumpOutput, key ObjectKey) *LastWriteAnalysis
```

### Module 2: Object Lifecycle Analysis

```go
// Input: dump + path + object + target hash
// Output: All appearances of that hash in the path

type ObjectLifecycleResult struct {
    Object      ObjectKey
    TargetHash  VersionHash
    PathIndex   int
    Appearances []StepInfo
}

type StepInfo struct {
    StepIndex    int
    ControllerId string
}

func AnalyzeObjectLifecycle(dump *DumpOutput, pathIndex int,
    key ObjectKey, targetHash VersionHash) *ObjectLifecycleResult
```

## Package Structure

```
pkg/analysis/
├── types.go           # Shared types (moved from pkg/interactive)
├── dump.go            # Dump loading utilities
├── diff.go            # Module 0: Converged State Diff
├── lastwrite.go       # Module 1: Last Write Analysis
├── lifecycle.go       # Module 2: Object Lifecycle Analysis
└── analysis_test.go   # Tests using trial-1 dump as fixture
```

## CLI Integration (kamera-slm)

```
kamera-analyze diff <dump.jsonl>              # Module 0
kamera-analyze lastwrite <dump.jsonl>         # Module 1
kamera-analyze lifecycle <dump.jsonl> [opts]  # Module 2
kamera-analyze report <dump.jsonl>            # Full backward-trace
```

## Validation

Use `analysis/nondeterminism-verification/trials-study1-both/trial-1/dump.jsonl` as test fixture.

Expected output from full analysis:
1. Module 0: "Endpoints differs between states pdu8xy8e and 15wjkorq"
2. Module 1: "Path 0 last write at step 25 (EC saw Pod.Ready=false), Path 1 at step 40 (EC saw Pod.Ready=true)"
3. Module 2: "Pod.Ready=true appears at step 38 in Path 0"
4. Conclusion: "EC didn't run after Pod.Ready=true appeared → watch/trigger issue"

## Related Tasks

- **kamera-189**: Move dump types from pkg/interactive to pkg/analysis (prerequisite)
- **kamera-pwm**: Implement Module 0
- **kamera-eja**: Implement Module 1
- **kamera-gwc**: Implement Module 2
- **kamera-slm**: Unified CLI
- **kamera-bl9**: Forward-trace divergence finder (separate tool, not part of backward-trace framework)

## Open Questions

1. Should Module 1 auto-diff the `stateBefore` across paths, or just report raw data?
2. For human-readable output, how much object detail to show? Full YAML diff? Summary only?
3. Should we support filtering by object kind (e.g., "only show Pod differences")?
