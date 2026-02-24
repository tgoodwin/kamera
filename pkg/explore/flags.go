package explore

import "flag"

var (
	interactiveFlag        = flag.Bool("interactive", true, "launch interactive trace inspector")
	dumpPathFlag           = flag.String("dump-output", "", "optional path to write exploration results (converged + aborted) to disk")
	configPathFlag         = flag.String("explore-config", "", "optional JSON file to configure exploration")
	dumpStatsPath          = flag.String("dump-stats", "", "optional path to write exploration stats (JSON)")
	inputsPathFlag         = flag.String("inputs", "", `path to input JSON file`)
	perturbFlag            = flag.Bool("perturb", true, "enable closed-loop rerun pipeline for batch inputs when supported by scenario generation")
	parallelProcessesFlag  = flag.Bool("parallel-processes", false, "run batch mode using process-isolated child executions")
	parallelChildIndexFlag = flag.Int(
		"parallel-child-index",
		-1,
		"internal input index selector used by --parallel-processes supervisor",
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

// DumpStatsPath returns the parsed path for dumping exploration stats.
func DumpStatsPath() string {
	return *dumpStatsPath
}

// InputsPath returns the parsed path to an optional inputs file.
func InputsPath() string {
	return *inputsPathFlag
}

// PerturbEnabled returns the parsed value for the perturb flag.
func PerturbEnabled() bool {
	return *perturbFlag
}

// ParallelProcessesEnabled reports whether process-isolated parallel mode is enabled.
func ParallelProcessesEnabled() bool {
	return *parallelProcessesFlag
}

// ParallelChildIndex returns the selected child index for process-isolated mode.
func ParallelChildIndex() int {
	return *parallelChildIndexFlag
}
