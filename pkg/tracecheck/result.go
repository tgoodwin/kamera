package tracecheck

import "fmt"

type Result struct {
	ConvergedStates []ConvergedState
}

func (r *Result) Summarize() {

	for _, c := range r.ConvergedStates {
		hash := c.State.Hash()
		numPaths := len(c.Paths)
		fmt.Printf("Converged state hash: %s, Number of paths to state: %d\n", hash, numPaths)
	}
}
