# User Controller Workflow Design

## Objective
Promote multi-step user workflows to a first-class concept in Kamera by modeling
the user as an in-engine actor, integrated directly into `pkg/tracecheck/explore.go`,
so user actions can interleave with controller reconciles during exploration.

## Context
Current workflow handling is scenario/phase oriented at runner level and does not
provide a direct mechanism for injecting user actions at arbitrary points during
an unfolding execution path.

This design narrows to one user actor per exploration instance and keeps existing
controller reconcile semantics intact.

## Core Design Decisions
1. Single user actor: each `Explorer` has exactly one `UserController`.
2. The user actor is conceptually "another controller", but executed via a
   dedicated user-action path in `tracecheck` (not as a normal pending
   reconciler entry).
3. User actions are ordered and stateful across a branch via
   `StateNode.nextUserActionIdx`.
4. Action scheduling is abstracted behind:
   `shouldApplyNextUserAction(state StateNode) bool`.
5. Initial scheduler policy is quiescence-only.
6. Assumption for v1: every user action mutates state.
7. User action writes must flow through the same replay/effect recording path
   used by controller-runtime reconcilers.

## Data Model
### UserAction
`UserAction` is data-only, intended to support future external workflow files:
- `id`
- `type`
- `payload`

No per-action function fields are stored on `UserAction`.

### UserController
One controller object per `Explorer` instance:
- Owns an internal ordered list of `UserAction`.
- Executes the next action for a branch based on `nextUserActionIdx`.
- Returns a normal step result (`Changes`, effects, errors) through the same
  effect recording mechanism used by other reconciler paths.

### Branch Progress Tracking
`StateNode` gets:
- `nextUserActionIdx int`

This is branch-local progress and advances only when a user action step is
successfully applied on that branch.

## Explore Loop Integration
Integrate in `pkg/tracecheck/explore.go` main step loop:

1. Pop state from stack/queue as today.
2. Before terminal convergence classification, evaluate:
   `shouldApplyNextUserAction(currentState)`.
3. If true:
   - Execute one user action step.
   - Apply resulting effects to produce a successor state.
   - Determine triggered reconciles.
   - Update pending reconciles.
   - Increment `nextUserActionIdx`.
   - Append synthetic history step with `ControllerID = "UserController"`.
   - Enqueue successor and continue.
4. If false, proceed with normal reconcile step selection/execution.
5. A state is terminal/converged only if:
   - there is no actionable pending reconcile work, and
   - there are no remaining user actions.

## Scheduling Abstraction
`shouldApplyNextUserAction(state)` is called each explore step.

Initial internal predicate:
- apply next user action when the branch is quiescent.

Future policies can plug in behind this method without changing outer loop
structure, enabling finer-grained interleavings like midpoint injections.

## Invariants and Guardrails
1. Mutating action invariant:
   - if a user action produces no effective write/effect, treat as invalid under
     v1 assumptions (fail branch or fail run, policy TBD).
2. History visibility:
   - user action steps must appear in `ExecutionHistory` and dumps for
     explainability.
3. Trigger semantics:
   - reconciler triggering after user actions uses existing trigger manager logic.

## Non-Goals (This Iteration)
- Multi-user concurrency models.
- Generic executor registries or per-action handler maps.
- External workflow file parser implementation.
- Custom midpoint predicate language.

## Follow-on Implementation Tasks
1. Add user workflow/controller types in `pkg/tracecheck`.
2. Add `nextUserActionIdx` to `StateNode` and clone/copy plumbing.
3. Add `shouldApplyNextUserAction(state)` and quiescence predicate in `Explorer`.
4. Add user action step execution path in `explore.go` using replay/effect path.
5. Update convergence gate to require no remaining user actions.
6. Emit user action history metadata in dumps/inspector context as needed.
7. Add tests for:
   - quiescence scheduling behavior,
   - user-action-triggered reconcile fanout,
   - branch-local index progression,
   - mutating-action invariant enforcement.

