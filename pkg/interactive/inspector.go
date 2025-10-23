package interactive

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	"github.com/tgoodwin/kamera/pkg/util"
)

func RunStateInspector(states []tracecheck.ConvergedState) {
	fmt.Println("\nInteractive state inspector ready.")
	fmt.Println("Commands: list, show <idx>, next, prev, paths [idx], path <pathIdx>|<stateIdx> <pathIdx>, help, quit")

	printStateList(states)

	scanner := bufio.NewScanner(os.Stdin)
	current := 0

	for {
		fmt.Print("explore> ")
		if !scanner.Scan() {
			fmt.Println("\ninput closed, exiting inspector.")
			return
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		args := strings.Fields(line)
		switch args[0] {
		case "help":
			fmt.Println("Commands: list, show <idx>, next, prev, paths [idx], quit")
		case "list":
			printStateList(states)
		case "show":
			if len(args) < 2 {
				fmt.Println("usage: show <idx>")
				continue
			}
			targetIdx, err := strconv.Atoi(args[1])
			if err != nil || targetIdx < 0 || targetIdx >= len(states) {
				fmt.Printf("invalid index %q\n", args[1])
				continue
			}
			current = targetIdx
			printStateDetail(states[current], current)
		case "next":
			if current+1 >= len(states) {
				fmt.Println("already at the last state")
				continue
			}
			current++
			printStateDetail(states[current], current)
		case "prev":
			if current == 0 {
				fmt.Println("already at the first state")
				continue
			}
			current--
			printStateDetail(states[current], current)
		case "paths":
			targetIdx := current
			if len(args) > 1 {
				idx, err := strconv.Atoi(args[1])
				if err != nil || idx < 0 || idx >= len(states) {
					fmt.Printf("invalid index %q\n", args[1])
					continue
				}
				targetIdx = idx
			}
			printStatePaths(states[targetIdx], targetIdx)
		case "path":
			var (
				stateIdx   = current
				pathIdxStr string
			)
			switch len(args) {
			case 2:
				pathIdxStr = args[1]
			case 3:
				idx, err := strconv.Atoi(args[1])
				if err != nil || idx < 0 || idx >= len(states) {
					fmt.Printf("invalid state index %q\n", args[1])
					continue
				}
				stateIdx = idx
				pathIdxStr = args[2]
			default:
				fmt.Println("usage: path <pathIdx> | path <stateIdx> <pathIdx>")
				continue
			}

			pathIdx, err := strconv.Atoi(pathIdxStr)
			if err != nil || pathIdx < 0 {
				fmt.Printf("invalid path index %q\n", pathIdxStr)
				continue
			}
			if stateIdx < 0 || stateIdx >= len(states) {
				fmt.Printf("invalid state index %d\n", stateIdx)
				continue
			}
			if pathIdx >= len(states[stateIdx].Paths) {
				fmt.Printf("state %d only has %d path(s)\n", stateIdx, len(states[stateIdx].Paths))
				continue
			}
			current = stateIdx
			if !inspectPath(scanner, states[stateIdx], pathIdx) {
				return
			}
		case "quit", "exit":
			fmt.Println("exiting inspector.")
			return
		default:
			fmt.Printf("unknown command %q, type 'help' for options\n", args[0])
		}
	}
}

func printStateList(states []tracecheck.ConvergedState) {
	fmt.Println("\nConverged states:")
	for idx, state := range states {
		summary := fmt.Sprintf("hash=%s", state.State.Hash())
		fmt.Printf("  [%d] %s objects=%d paths=%d\n", idx, summary, len(state.State.Objects()), len(state.Paths))
	}
}

func printStateDetail(state tracecheck.ConvergedState, idx int) {
	fmt.Printf("\nState [%d]\n", idx)
	if state.ID != "" {
		fmt.Println("ID:", state.ID)
	}
	fmt.Println("Hash:", state.State.Hash())
	fmt.Println("Objects:", len(state.State.Objects()))
	fmt.Println("Paths:", len(state.Paths))
	fmt.Println("Contents:")
	printObjectVersions(state.State.Objects())
}

func printStatePaths(state tracecheck.ConvergedState, idx int) {
	fmt.Printf("\nPaths for state [%d]\n", idx)
	if len(state.Paths) == 0 {
		fmt.Println("  (no paths captured)")
		return
	}
	for i, path := range state.Paths {
		fmt.Printf("  Path %d (%d steps): %s\n", i, len(path), summarizePath(path))
	}
	fmt.Println("  Use `path <pathIdx>` to inspect a path in detail.")
}

func summarizePath(path tracecheck.ExecutionHistory) string {
	if len(path) == 0 {
		return "(empty)"
	}
	parts := make([]string, len(path))
	for i, step := range path {
		if step == nil {
			parts[i] = "(nil)"
			continue
		}
		parts[i] = fmt.Sprintf("%s[%d]", step.ControllerID, len(step.Changes.ObjectVersions))
	}
	return strings.Join(parts, " -> ")
}

func printObjectVersions(objects tracecheck.ObjectVersions) {
	if len(objects) == 0 {
		fmt.Println("  (empty)")
		return
	}
	keys := make([]snapshot.CompositeKey, 0, len(objects))
	for key := range objects {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].String() < keys[j].String()
	})
	for _, key := range keys {
		fmt.Printf("  %s => %s\n", key.String(), objects[key].Value)
	}
}

func inspectPath(scanner *bufio.Scanner, state tracecheck.ConvergedState, pathIdx int) bool {
	path := state.Paths[pathIdx]
	fmt.Printf("\nInspecting path %d of state hash %s (steps=%d)\n", pathIdx, state.State.Hash(), len(path))
	if len(path) == 0 {
		fmt.Println("  (path is empty)")
		return true
	}

	printPathSteps(path)

	currentStep := 0
	for {
		fmt.Print("path> ")
		if !scanner.Scan() {
			fmt.Println("\ninput closed, exiting inspector.")
			return false
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		args := strings.Fields(line)
		nextIndex := func(defaultIdx int) (int, bool) {
			target := defaultIdx
			if len(args) > 1 {
				idx, err := strconv.Atoi(args[1])
				if err != nil || idx < 0 || idx >= len(path) {
					fmt.Printf("invalid step index %q\n", args[1])
					return 0, false
				}
				target = idx
			}
			return target, true
		}

		switch args[0] {
		case "help":
			fmt.Println("Path commands: steps, step <idx>, next, prev, effects [idx], objects [idx], deltas [idx], back, quit")
		case "steps":
			printPathSteps(path)
		case "step", "show":
			target, ok := nextIndex(currentStep)
			if !ok {
				continue
			}
			currentStep = target
			printPathStepDetail(path[currentStep], currentStep)
		case "next":
			if currentStep+1 >= len(path) {
				fmt.Println("already at the last step")
				continue
			}
			currentStep++
			printPathStepDetail(path[currentStep], currentStep)
		case "prev":
			if currentStep == 0 {
				fmt.Println("already at the first step")
				continue
			}
			currentStep--
			printPathStepDetail(path[currentStep], currentStep)
		case "effects":
			target, ok := nextIndex(currentStep)
			if !ok {
				continue
			}
			printStepEffects(path[target], target)
		case "objects":
			target, ok := nextIndex(currentStep)
			if !ok {
				continue
			}
			if path[target] == nil {
				fmt.Printf("step %d has no data\n", target)
				continue
			}
			fmt.Printf("\nObjects written at step %d:\n", target)
			printObjectVersions(path[target].Changes.ObjectVersions)
		case "deltas":
			target, ok := nextIndex(currentStep)
			if !ok {
				continue
			}
			printStepDeltas(path[target], target)
		case "back":
			return true
		case "quit", "exit":
			fmt.Println("exiting inspector.")
			return false
		default:
			fmt.Printf("unknown command %q, type 'help' for options\n", args[0])
		}
	}
}

func printPathSteps(path tracecheck.ExecutionHistory) {
	fmt.Println("\nPath steps:")
	for idx, step := range path {
		if step == nil {
			fmt.Printf("  [%d] (nil step)\n", idx)
			continue
		}
		frameID := util.Shorter(step.FrameID)
		fmt.Printf("  [%d] controller=%s frame=%s writes=%d\n", idx, step.ControllerID, frameID, len(step.Changes.Effects))
	}
	fmt.Println("  Use `step <idx>` to view details.")
}

func printPathStepDetail(step *tracecheck.ReconcileResult, idx int) {
	if step == nil {
		fmt.Printf("\nStep [%d] has no data\n", idx)
		return
	}

	fmt.Printf("\nStep [%d]\n", idx)
	fmt.Println("Controller:", step.ControllerID)
	fmt.Println("Frame:", util.Shorter(step.FrameID))
	fmt.Println("FrameType:", step.FrameType)
	fmt.Println("Writes:", len(step.Changes.Effects))
	fmt.Println("Contents:")
	printObjectVersions(step.Changes.ObjectVersions)
	printStepEffects(step, idx)
	printStepDeltas(step, idx)
}

func printStepEffects(step *tracecheck.ReconcileResult, idx int) {
	if step == nil {
		fmt.Printf("\nStep [%d] has no data\n", idx)
		return
	}
	if len(step.Changes.Effects) == 0 {
		fmt.Printf("\nEffects for step %d: (none)\n", idx)
		return
	}
	fmt.Printf("\nEffects for step %d:\n", idx)
	for i, eff := range step.Changes.Effects {
		precondition := ""
		if eff.Precondition != nil {
			precondition = " (precondition present)"
		}
		fmt.Printf("  [%d] %s %s => %s%s\n", i, string(eff.OpType), eff.Key.String(), eff.Version.Value, precondition)
	}
}

func printStepDeltas(step *tracecheck.ReconcileResult, idx int) {
	if step == nil {
		fmt.Printf("\nStep [%d] has no data\n", idx)
		return
	}
	if len(step.Deltas) == 0 {
		fmt.Printf("\nDeltas for step %d: (none)\n", idx)
		return
	}
	fmt.Printf("\nDeltas for step %d:\n", idx)
	keys := make([]snapshot.CompositeKey, 0, len(step.Deltas))
	for key := range step.Deltas {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].String() < keys[j].String()
	})
	for _, key := range keys {
		fmt.Printf("  %s => %s\n", key.String(), string(step.Deltas[key]))
	}
}
