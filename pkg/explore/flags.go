package explore

import "flag"

var (
	interactiveFlag = flag.Bool("interactive", true, "launch interactive trace inspector")
	dumpPathFlag    = flag.String("dump-output", "", "optional path to write exploration results (converged + aborted) to disk")
	depth           = flag.Int("max-explore-depth", 10, "maximum depth to explore in the state space")
)

// InteractiveEnabled returns the parsed value for the interactive flag.
func InteractiveEnabled() bool {
	return *interactiveFlag
}

// DumpPath returns the parsed path for dumping inspector results.
func DumpPath() string {
	return *dumpPathFlag
}
