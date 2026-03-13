package explore

import "flag"

var (
	interactiveFlag       = flag.Bool("interactive", true, "launch interactive trace inspector")
	dumpPathFlag          = flag.String("output", "", "optional path to write exploration dump to disk (states, plus stats when --emit-stats is enabled)")
	configPathFlag        = flag.String("explore-config", "", "optional JSON file to configure exploration")
	inputsPathFlag        = flag.String("inputs", "", `path to input JSON file`)
	closedLoopFlag        = flag.Bool("closed-loop", true, "enable closed-loop rerun pipeline: runs a reference phase then auto-generated perturbation phases derived from the reference trace")
	noPerturbationsFlag   = flag.Bool("no-perturbations", false, "force-disable all perturbations (ordering, staleness) for a clean reference run, regardless of what is configured in the inputs file")
	parallelProcessesFlag = flag.Bool("parallel-processes", false, "run batch mode using process-isolated child executions")
	// these ones are internal flags used by child processes in --parallel-processes mode,
	// not intended for manual setting
	parallelChildIndexFlag = flag.Int(
		"parallel-child-index",
		-1,
		"internal input index selector used by --parallel-processes supervisor",
	)
	parallelChildTrialIndexFlag = flag.Int(
		"parallel-child-trial-index",
		0,
		"internal trial index selector used by --parallel-processes supervisor",
	)
	parallelChildJobIndexFlag = flag.Int(
		"parallel-child-job-index",
		-1,
		"internal job index selector used by --parallel-processes supervisor",
	)
)

// InteractiveEnabled returns the parsed value for the interactive flag.
func InteractiveEnabled() bool {
	return *interactiveFlag
}

// DumpPath returns the parsed path for dumping inspector results.
func DumpPath() string {
	return *dumpPathFlag
}

// ConfigPath returns the parsed path to an external explore config file.
func ConfigPath() string {
	return *configPathFlag
}

// InputsPath returns the parsed path to an optional inputs file.
func InputsPath() string {
	return *inputsPathFlag
}

// ClosedLoopEnabled returns true when the closed-loop rerun pipeline is active.
// When true, runs a reference phase (perturbations stripped) followed by auto-generated
// perturbation phases derived from the reference trace.
// When false, runs a single phase with the config as specified in the inputs file.
func ClosedLoopEnabled() bool {
	return *closedLoopFlag
}

// NoPerturbationsEnabled returns true when --no-perturbations is set.
// When true, all perturbation config (ordering, staleness) is stripped before running,
// producing a clean reference run regardless of what is configured in the inputs file.
// Use this to get a baseline trace from a JSON that already has permuteControllers configured.
func NoPerturbationsEnabled() bool {
	return *noPerturbationsFlag
}

// ParallelProcessesEnabled reports whether process-isolated parallel mode is enabled.
func ParallelProcessesEnabled() bool {
	return *parallelProcessesFlag
}

// ParallelChildIndex returns the selected child index for process-isolated mode.
func ParallelChildIndex() int {
	return *parallelChildIndexFlag
}

// ParallelChildTrialIndex returns the selected trial index for process-isolated mode.
func ParallelChildTrialIndex() int {
	return *parallelChildTrialIndexFlag
}

// ParallelChildJobIndex returns the selected job index for process-isolated mode.
func ParallelChildJobIndex() int {
	return *parallelChildJobIndexFlag
}
