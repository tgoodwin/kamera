# Ablation Studies

This directory contains configuration files and analysis for ablation studies on the exploration optimizations.

## Study: Does Subtree Completion Subsume Completed-Path-Dedup?

**Hypothesis**: The subtree completion optimization should be a superset of the completed-path-dedup optimization. Both optimizations skip redundant exploration, but they use different mechanisms:

```
┌────────────────────────────────────────────────────────────────────────┐
│                    Completed-Path-Dedup                                 │
├────────────────────────────────────────────────────────────────────────┤
│ Key: (state hash) + (execution history signature)                       │
│ Skips: Same exact path reaching same exact state                        │
│ Tracks: "Completion" - only skips after path reaches a leaf             │
└────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────┐
│                    Subtree Completion                                   │
├────────────────────────────────────────────────────────────────────────┤
│ Key: (objects hash) + (pending set) - NO history!                       │
│ Skips: Any path reaching same logical state once subtree done           │
│ Tracks: "Completion" via stack markers                                  │
└────────────────────────────────────────────────────────────────────────┘
```

### Theoretical Analysis

Subtree completion should subsume path-dedup because:

1. **Coarser key**: Subtree completion ignores execution history, so it matches MORE states
2. **Same completion semantics**: Both wait for "completion" before skipping
3. **Inclusion**: Any (state, history) pair that path-dedup would skip is part of a logical state that subtree completion would skip

However, there are edge cases where path-dedup might fire and subtree completion doesn't:

- **Diamond convergence**: If two paths reach the same (state, history) but different logical states (different pending order), path-dedup might catch duplicates that subtree completion's diamond check misses
- **Timing**: Path-dedup marks completion at leaf nodes; subtree completion marks when marker pops - the timing differs

### Config Files

```
configs/
├── study-1-both.json          # Both optimizations enabled
├── study-2-subtree-only.json  # Subtree ON, path-dedup OFF
├── study-3-pathdedup-only.json # Subtree OFF, path-dedup ON
├── study-4-neither.json       # Both OFF (baseline)
```

### Running the Study

```bash
cd analysis
./study-subtree-subsumes-pathdedup.sh
```

### Expected Results

If subtree completion **fully subsumes** path-dedup:
- `study-1-both`: `completedPathDedup skips = 0` (or very low)
- `study-1-both` ≈ `study-2-subtree-only` in time and visits
- `study-2-subtree-only` should be faster than `study-3-pathdedup-only`

If subtree completion **does NOT fully subsume** path-dedup:
- `study-1-both`: `completedPathDedup skips > 0`
- `study-1-both` faster than `study-2-subtree-only`
- Both optimizations provide complementary benefit

### Interpreting the Results

| Result Pattern | Interpretation |
|----------------|----------------|
| study-1 ≈ study-2, pathDedup skips = 0 | Subtree completion fully subsumes path-dedup |
| study-1 < study-2, pathDedup skips > 0 | Complementary benefits, keep both |
| study-1 ≈ study-2, pathDedup skips > 0 | Path-dedup fires but adds no benefit (overhead only) |
