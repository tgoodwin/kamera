package tracecheck

import "fmt"

type Result struct {
	ConvergedStates []ResultState
	AbortedStates   []ResultState
}

func (r *Result) Summarize() {

	for i, c := range r.ConvergedStates {
		hash := c.State.Hash()
		numPaths := len(c.Paths)
		uniquePaths := GetUniquePaths(c.Paths)
		fmt.Printf("Converged state %d - hash: %s, total paths: %d, unique paths: %d\n", i, hash, numPaths, len(uniquePaths))
	}
}
