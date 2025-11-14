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

func RunStateInspector(states []tracecheck.ResultState) {
	states = validateResultStates(states)
	states = tracecheck.TrimStatesForInspection(states)
	states = dedupeResultStates(states)

	fmt.Println("\nInteractive state inspector ready.")
	fmt.Println("Commands: list, show <idx>, next, prev, paths [idx], path <pathIdx>|<stateIdx> <pathIdx>, help, quit (or <esc> to exit)")

	printStateList(states)

	scanner := bufio.NewScanner(os.Stdin)
	current := -1

	for {
		if current == -1 {
			fmt.Print("explore> ")
		} else {
			fmt.Printf("state %d> ", current)
		}
		if !scanner.Scan() {
			fmt.Println("\ninput closed, exiting inspector.")
			return
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if isEscape(line) {
			if current != -1 {
				fmt.Printf("leaving state %d\n", current)
				current = -1
				continue
			}
			fmt.Println("exiting inspector.")
			return
		}

		args := strings.Fields(line)
		if len(args) == 0 {
			continue
		}
		switch args[0] {
		case "help":
			fmt.Println("Commands: list, show <idx>, next, prev, paths [idx], quit (or <esc> to exit)")
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
			if len(states) == 0 {
				fmt.Println("no states available")
				continue
			}
			if current == -1 {
				current = 0
				printStateDetail(states[current], current)
				continue
			}
			if current+1 >= len(states) {
				fmt.Println("already at the last state")
				continue
			}
			current++
			printStateDetail(states[current], current)
		case "prev":
			if len(states) == 0 {
				fmt.Println("no states available")
				continue
			}
			if current == -1 {
				current = 0
				printStateDetail(states[current], current)
				continue
			}
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
			} else if targetIdx == -1 {
				fmt.Println("select a state first")
				continue
			}
			printStatePaths(states[targetIdx], targetIdx)
		case "path":
			var (
				stateIdx   = current
				pathIdxStr string
			)
			switch len(args) {
			case 2:
				if stateIdx == -1 {
					fmt.Println("select a state first")
					continue
				}
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
			if !inspectPath(scanner, states[stateIdx], stateIdx, pathIdx) {
				return
			}
		case "quit", "exit":
			fmt.Println("exiting inspector.")
			return
		default:
			if idx, err := strconv.Atoi(args[0]); err == nil {
				if idx < 0 || idx >= len(states) {
					fmt.Printf("invalid index %d\n", idx)
					continue
				}
				current = idx
				printStateDetail(states[current], current)
				continue
			}
			fmt.Printf("unknown command %q, type 'help' for options\n", args[0])
		}
	}
}

func printStateList(states []tracecheck.ResultState) {
	fmt.Println("\nConverged states:")
	for idx, state := range states {
		summary := fmt.Sprintf("hash=%s", state.State.Hash())
		fmt.Printf("  [%d] %s objects=%d paths=%d\n", idx, summary, len(state.State.Objects()), len(state.Paths))
	}
}

func printStateDetail(state tracecheck.ResultState, idx int) {
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

func printStatePaths(state tracecheck.ResultState, idx int) {
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

func inspectPath(scanner *bufio.Scanner, state tracecheck.ResultState, stateIdx, pathIdx int) bool {
	path := state.Paths[pathIdx]
	fmt.Printf("\nInspecting state %d path %d (hash=%s, steps=%d)\n", stateIdx, pathIdx, state.State.Hash(), len(path))
	if len(path) == 0 {
		fmt.Println("  (path is empty)")
		return true
	}

	printPathSteps(path)

	currentStep := -1
	for {
		if currentStep == -1 {
			fmt.Printf("state %d:path %d> ", stateIdx, pathIdx)
		} else {
			fmt.Printf("state %d:path %d:step %d> ", stateIdx, pathIdx, currentStep)
		}
		if !scanner.Scan() {
			fmt.Println("\ninput closed, exiting inspector.")
			return false
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if isEscape(line) {
			return true
		}

		args := strings.Fields(line)
		if len(args) == 0 {
			continue
		}
		switch args[0] {
		case "help":
			fmt.Println("Path commands: steps, step <idx>, next, prev, effects [idx], objects [idx], deltas [idx], back, quit (or <esc> to go back)")
		case "steps":
			printPathSteps(path)
		case "step", "show":
			if len(path) == 0 {
				fmt.Println("path is empty")
				continue
			}
			target := currentStep
			if len(args) > 1 {
				idx, err := strconv.Atoi(args[1])
				if err != nil || idx < 0 || idx >= len(path) {
					fmt.Printf("invalid step index %q\n", args[1])
					continue
				}
				target = idx
			} else if target == -1 {
				target = 0
			}
			currentStep = target
			printPathStepDetail(path[currentStep], currentStep)
		case "next":
			if len(path) == 0 {
				fmt.Println("path is empty")
				continue
			}
			if currentStep == -1 {
				currentStep = 0
				printPathStepDetail(path[currentStep], currentStep)
				continue
			}
			if currentStep+1 >= len(path) {
				fmt.Println("already at the last step")
				continue
			}
			currentStep++
			printPathStepDetail(path[currentStep], currentStep)
		case "prev":
			if currentStep == -1 {
				fmt.Println("select a step first")
				continue
			}
			if currentStep == 0 {
				fmt.Println("already at the first step")
				continue
			}
			currentStep--
			printPathStepDetail(path[currentStep], currentStep)
		case "effects":
			target := currentStep
			if len(args) > 1 {
				idx, err := strconv.Atoi(args[1])
				if err != nil || idx < 0 || idx >= len(path) {
					fmt.Printf("invalid step index %q\n", args[1])
					continue
				}
				target = idx
			}
			if target == -1 {
				fmt.Println("select a step first")
				continue
			}
			printStepEffects(path[target], target)
		case "objects":
			target := currentStep
			if len(args) > 1 {
				idx, err := strconv.Atoi(args[1])
				if err != nil || idx < 0 || idx >= len(path) {
					fmt.Printf("invalid step index %q\n", args[1])
					continue
				}
				target = idx
			}
			if target == -1 {
				fmt.Println("select a step first")
				continue
			}
			if path[target] == nil {
				fmt.Printf("step %d has no data\n", target)
				continue
			}
			fmt.Printf("\nObjects written at step %d:\n", target)
			printObjectVersions(path[target].Changes.ObjectVersions)
		case "deltas":
			target := currentStep
			if len(args) > 1 {
				idx, err := strconv.Atoi(args[1])
				if err != nil || idx < 0 || idx >= len(path) {
					fmt.Printf("invalid step index %q\n", args[1])
					continue
				}
				target = idx
			}
			if target == -1 {
				fmt.Println("select a step first")
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
	for i, key := range keys {
		fmt.Printf("  %s\n", key.String())
		printIndentedBlock(string(step.Deltas[key]), "    ")
		if i < len(keys)-1 {
			fmt.Println()
		}
	}
}

func printIndentedBlock(text, indent string) {
	content := strings.TrimRight(text, "\n")
	if content == "" {
		fmt.Printf("%s(no diff)\n", indent)
		return
	}
	lines := strings.Split(content, "\n")
	for _, line := range lines {
		fmt.Printf("%s%s\n", indent, line)
	}
}

func isEscape(line string) bool {
	if len(line) == 0 {
		return false
	}
	if len(line) == 1 && line[0] == 0x1b {
		return true
	}
	return line == "^["
}
