package explore

import "flag"

var (
	interactiveFlag       = flag.Bool("interactive", true, "launch interactive trace inspector")
	dumpPathFlag          = flag.String("output", "", "optional path to write exploration dump to disk (states, plus stats when --emit-stats is enabled)")
	configPathFlag        = flag.String("explore-config", "", "optional JSON file to configure exploration")
	inputsPathFlag        = flag.String("inputs", "", `path to input JSON file`)
	perturbFlag            = flag.Bool("perturb", true, "enable closed-loop rerun pipeline for batch inputs when supported by scenario generation")
	noPerturbationsFlag    = flag.Bool("no-perturbations", false, "force-disable all perturbations (ordering, staleness) for a clean reference run, regardless of what is configured in the inputs file")
	parallelProcessesFlag  = flag.Bool("parallel-processes", false, "run batch mode using process-isolated child executions")
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

// PerturbEnabled returns the parsed value for the perturb flag.
// When true, enables the closed-loop rerun pipeline (reference phase + auto-planned perturbation phases).
// When false, runs a single phase with the config as specified in the inputs file.
func PerturbEnabled() bool {
	return *perturbFlag
}

// NoPerturbationsEnabled returns true when --no-perturbations is set.
// When true, all perturbation config (ordering, staleness) is stripped before running,
// producing a clean reference run regardless of what is configured in the inputs file.
// This is mode 1: "run this scenario as a reference without changing the JSON."
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
