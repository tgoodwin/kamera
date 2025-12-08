## Context
check out @examples/knative-serving/AGENTS.md for instructions on how to run this simulation.
currently, explore.go finds converged states, as well as all possible paths to that converged state. this means that if we arrive at an intermediate state we've seen before, we don't skip it, as there may be additional paths to that state that we want to uncover.
I want to re-evaluate this behavior, as it makes traversing the entire state space very inefficient. Instead, let's just focus on finding all potential converged states. this should allow us to skip more execution branches and traverse teh state space more efficiently.

In the future, I envision a mode where IF the explore routine finds multiple converged states, then it goes back and finds all possible paths (to aid debugging / root cause analysis when there is some order-sensitivity among reconciler logic). SO, don't remove the data structures that track execution paths. let's just modify the DFS to not continue exploring in pursuit of finding additional paths to a converged state it's seen before.

## Goal
Let's modify explore.go to only focus on finding converged states, not all possible paths to them. This should allow the DFS to do a lot less work, thereby enabling us to cover the state space more efficiently.

## Notes
- when running the simulation, use a depth of 100 for the knative example, as convergence usually happens around depth 80. you should also use a timeout; say 30s.
- "failed to sync informers" errors are transient; they appear when the timeout fires mid informer sync. do not consider them some serious issue.