# User-Action Interleaving Perturbation Strategy Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a default closed-loop perturbation strategy for multi-step workflows that executes reruns for every valid interleaving depth of subsequent user actions, derived from the reference trace.

**Architecture:** Extend perturbation config with user-action depth targets, update the user-action scheduler to honor depth targets with early-convergence fallback, and replace the auto broad-rerun planner for multi-step scenarios with an interleaving phase generator that emits one phase per `(actionIndex, targetDepth)` choice. Keep single-step and custom planners unchanged.

**Tech Stack:** Go (`pkg/explore`, `pkg/tracecheck`), existing closed-loop runner, existing execution history metadata.

## Summary
Add a new default closed-loop perturbation strategy for scenarios with `len(UserInputs) >= 2` that generates one rerun phase per valid insertion depth for each subsequent action (one action perturbed at a time).
Depth windows are derived from the first converged reference path.
For two-step workflows, if action 0 is at depth `D0` and action 1 is at depth `D1`, generate reruns for depths `D0+1..D1-1`; if that window is empty, generate one rerun at `D1`.

## Public/API Changes
1. Extend `tracecheck` perturbation config to carry user-action depth targeting.
2. Keep existing scenario and input schemas unchanged.
3. Behavior change applies only to auto closed-loop planning when `Scenario.ClosedLoop == nil` and `len(UserInputs) >= 2`.
4. Custom scenario planners (`ClosedLoop.Plan`) remain authoritative and unchanged.
5. Single-step auto-planning keeps current broad rerun behavior.

## Exact Implementation Plan
1. Add a new scheduler knob in `pkg/tracecheck/explore.go`.
2. Concrete field: add `UserActionTargetDepth map[int]int` under `PerturbationConfig` in `pkg/tracecheck/explore.go`.
3. Update `ExploreConfig.Clone()` to deep-copy `UserActionTargetDepth`.
4. Update `disablePerturbations` in `pkg/explore/parallel_runner.go` to clear `UserActionTargetDepth`.
5. Update `shouldApplyNextUserAction` in `pkg/tracecheck/explore.go` with this policy:
6. If no remaining action: `false`.
7. If current action has a scheduled target depth `T`: return `true` when `state.depth >= T`.
8. If current action has scheduled `T` but branch converged early (`state.IsConverged()==true` and depth `< T`): return `true`.
9. Otherwise fallback to existing quiescence rule.
10. Add interleaving plan builder in `pkg/explore/parallel_runner.go` or a new helper file under `pkg/explore`:
11. Select reference source as first converged path: `reference.Result.ConvergedStates[0].Paths[0]`.
12. Parse user-action steps from `step.ControllerID == tracecheck.UserControllerID` and `step.StepMetadata["userAction.index"]`.
13. For each subsequent action index `i` from `1..len(UserInputs)-1`, compute `Dprev` (action `i-1`) and `Di` (action `i`).
14. Generate depths:
15. If `Di > Dprev+1`: emit `Dprev+1..Di-1` (strictly between).
16. If `Di <= Dprev+1`: emit one depth at `Di`.
17. For each generated depth `d`, create one phase with:
18. `cfg := disablePerturbations(baseCfg)`.
19. `cfg.Perturbations.UserActionTargetDepth = map[int]int{i: d}`.
20. Phase name format: `interleave_action_<i>_depth_<d>`.
21. Context attributes (in phase context copy): `perturbation.strategy=user_action_interleaving`, `perturbation.action_index`, `perturbation.target_depth`, `perturbation.reference_prev_depth`, `perturbation.reference_action_depth`.
22. Wire planner selection in `runScenario` auto-planner path in `pkg/explore/parallel_runner.go`:
23. If `len(scenario.UserInputs) >= 2`: use interleaving planner only (replace broad rerun).
24. If reference never reaches required subsequent actions (`Di` missing): emit zero rerun phases for that scenario (reference-only result), no fallback broad rerun.
25. If `len(UserInputs) < 2`: keep existing broad rerun (`buildDefaultScenarioRerunPlans`).

## Test Plan
1. Add/extend tests in `pkg/tracecheck/explore_user_action_scheduler_test.go`:
2. Scheduled depth applies on non-converged state at/after target.
3. Scheduled depth does not apply before target when non-converged.
4. Early-convergence fallback applies action before target when converged.
5. Add planner tests in `pkg/explore/parallel_runner_test.go` (or new focused planner test file):
6. Multi-step scenario emits reference + interleaving phases; no broad rerun phase.
7. Two-step emits strictly-between depths.
8. Empty window (`Di <= Dprev+1`) emits exactly one phase at `Di`.
9. Missing subsequent action in reference emits reference-only (zero reruns) for multi-step.
10. Single-step still emits current broad rerun behavior.
11. Add clone safety tests for new perturbation map in `pkg/tracecheck` config clone tests.

## Acceptance Criteria
1. Multi-step auto closed-loop produces a phase per derived interleaving depth (one action perturbed per phase).
2. Two-step behavior matches formula exactly: `D0+1..D1-1`, with `Di` fallback when window is empty.
3. Strategy uses first converged reference path.
4. No rerun phases are produced when required subsequent action depths are missing in reference.
5. Single-step and custom closed-loop planners remain behaviorally unchanged.

## Assumptions and Defaults Locked
1. Generalization mode: one subsequent action perturbed at a time.
2. Reference selection: first converged path.
3. Multi-step default: replace broad rerun planner.
4. Depth window: strictly between previous and current reference action depths.
5. Empty window policy: emit one phase at `Di`.
6. Missing action depth policy: skip interleaving reruns for that scenario.
7. Early-converged branch policy: apply action at convergence (best-effort lower-bound schedule).
8. Future strategy composition/cross-product is out of scope for this change and intentionally deferred.
