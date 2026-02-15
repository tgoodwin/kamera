package explore

import "flag"

var (
	interactiveFlag = flag.Bool("interactive", true, "launch interactive trace inspector")
	dumpPathFlag    = flag.String("dump-output", "", "optional path to write exploration results (converged + aborted) to disk")
	configPathFlag  = flag.String("explore-config", "", "optional JSON file to configure exploration")
	dumpStatsPath   = flag.String("dump-stats", "", "optional path to write exploration stats (JSON)")
	inputsPathFlag  = flag.String("inputs", "", `path to input JSON file`)
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
