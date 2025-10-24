package interactive

import (
	"fmt"
	"sort"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	"github.com/tgoodwin/kamera/pkg/util"
	"sigs.k8s.io/yaml"
)

type inspectorMode int

const (
	modeStates inspectorMode = iota
	modePaths
	modeSteps
)

type detailTableMode int

const (
	detailNone detailTableMode = iota
	detailStateObjects
	detailStepEffects
)

type stateObjectEntry struct {
	key  snapshot.CompositeKey
	hash snapshot.VersionHash
}

type effectEntry struct {
	effect tracecheck.Effect
	diff   string
}

// RunStateInspectorTUIView launches a tview-based inspector for converged states.
func RunStateInspectorTUIView(states []tracecheck.ConvergedState) error {
	if len(states) == 0 {
		return fmt.Errorf("no converged states supplied")
	}

	app := tview.NewApplication()

	mainTable := configureTable("States", true)
	detailTable := configureTable("Details", true)
	detailText := tview.NewTextView()
	detailText.SetDynamicColors(true)
	detailText.SetWrap(true)
	detailText.SetTitle("Details")
	detailText.SetBorder(true)
	detailContainer := tview.NewFlex()
	detailContainer.AddItem(detailTable, 0, 1, false)
	currentDetailPrim := tview.Primitive(detailTable)

	statusBar := tview.NewTextView().
		SetDynamicColors(true).
		SetText(`[yellow]<Tab>[/yellow] to move focus • [yellow]Enter[/yellow] to select • [yellow]q[/yellow] to quit`).
		SetTextAlign(tview.AlignCenter)

	root := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(mainTable, 0, 5, true).
		AddItem(detailContainer, 0, 3, false).
		AddItem(statusBar, 1, 0, false)

	var (
		selectedState     = 0
		selectedPath      = 0
		selectedStep      = 0
		mode              = modeStates
		currentDetailMode detailTableMode
		stateObjects      []stateObjectEntry
		stepEffects       []effectEntry
		returnFromText    func()
		stateDetailRow    = 1
		stepDetailRow     = 1
	)

	const (
		stateStatusMessage  = `[yellow]Enter/d[/yellow] describe object • [yellow]Tab[/yellow] swap focus • [yellow]q[/yellow] quit`
		stateDescribeStatus = `[yellow]Esc[/yellow] back • [yellow]q[/yellow] quit`
		pathStatusMessage   = `[yellow]Enter[/yellow] open steps • [yellow]Esc[/yellow] back • [yellow]Tab[/yellow] swap focus • [yellow]q[/yellow] quit`
		stepStatusMessage   = `[yellow]Enter/d[/yellow] view diff • [yellow]Esc[/yellow] back • [yellow]Tab[/yellow] swap focus • [yellow]q[/yellow] quit`
		stepDescribeStatus  = `[yellow]Esc[/yellow] back • [yellow]q[/yellow] quit`
	)

	var (
		stateSelectionChanged func(int, int)
		stateEnter            func(int, int)
		pathSelectionChanged  func(int, int)
		pathEnter             func(int, int)
		stepSelectionChanged  func(int, int)
		stepEnter             func(int, int)
		performDetailAction   func()
	)

	var (
		renderStateDetail func()
		renderPathDetail  func()
		renderStepDetail  func()
	)

	updateStatus := func(text string) {
		statusBar.SetText(text)
	}

	focusDetail := func() {
		app.SetFocus(currentDetailPrim)
	}

	showDetailTable := func() {
		detailContainer.Clear()
		detailContainer.AddItem(detailTable, 0, 1, false)
		currentDetailPrim = detailTable
		returnFromText = nil
	}

	showDetailText := func(title, body, status string) {
		detailText.SetTitle(title)
		detailText.SetText(body)
		detailContainer.Clear()
		detailContainer.AddItem(detailText, 0, 1, false)
		currentDetailPrim = detailText
		if status != "" {
			updateStatus(status)
		}
	}

	var applyMode func(inspectorMode)

	goBack := func() bool {
		if returnFromText != nil {
			fn := returnFromText
			returnFromText = nil
			fn()
			return true
		}
		switch mode {
		case modeSteps:
			applyMode(modePaths)
			return true
		case modePaths:
			applyMode(modeStates)
			return true
		default:
			return false
		}
	}

	mainTable.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyTab:
			focusDetail()
			return nil
		case tcell.KeyEscape:
			if goBack() {
				return nil
			}
		}
		switch event.Rune() {
		case 'q', 'Q':
			app.Stop()
			return nil
		}
		return event
	})

	detailTable.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyTab:
			app.SetFocus(mainTable)
			return nil
		case tcell.KeyEscape:
			if goBack() {
				return nil
			}
		case tcell.KeyEnter:
			if performDetailAction != nil {
				performDetailAction()
			}
			return nil
		}
		switch event.Rune() {
		case 'q', 'Q':
			app.Stop()
			return nil
		case 'd', 'D':
			if performDetailAction != nil {
				performDetailAction()
			}
			return nil
		}
		return event
	})

	detailText.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyTab:
			app.SetFocus(mainTable)
			return nil
		case tcell.KeyEscape:
			if goBack() {
				return nil
			}
		}
		switch event.Rune() {
		case 'q', 'Q':
			app.Stop()
			return nil
		}
		return event
	})

	detailTable.SetSelectionChangedFunc(func(row, _ int) {
		switch currentDetailMode {
		case detailStateObjects:
			stateDetailRow = row
		case detailStepEffects:
			stepDetailRow = row
		}
	})

	showObjectYAML := func(entry stateObjectEntry) {
		row := stateDetailRow
		title := fmt.Sprintf("Object %s", entry.key.String())
		body := jsonToYAMLString(entry.hash.Value)
		showDetailText(title, body, stateDescribeStatus)
		returnFromText = func() {
			stateDetailRow = row
			renderStateDetail()
			focusDetail()
		}
		focusDetail()
	}

	showEffectDiff := func(entry effectEntry) {
		row := stepDetailRow
		key := entry.effect.Key
		title := fmt.Sprintf("%s %s/%s/%s", string(entry.effect.OpType), key.ResourceKey.Kind, key.ResourceKey.Namespace, key.ResourceKey.Name)
		diff := entry.diff
		if strings.TrimSpace(diff) == "" {
			diff = jsonToYAMLString(entry.effect.Version.Value)
		}
		showDetailText(title, diff, stepDescribeStatus)
		returnFromText = func() {
			stepDetailRow = row
			renderStepDetail()
			focusDetail()
		}
		focusDetail()
	}

	renderStateDetail = func() {
		currentDetailMode = detailStateObjects
		if selectedState < 0 || selectedState >= len(states) {
			showDetailText("Details", "no state selected", stateStatusMessage)
			currentDetailMode = detailNone
			return
		}

		state := states[selectedState]
		objects := state.State.Objects()
		keys := make([]snapshot.CompositeKey, 0, len(objects))
		for key := range objects {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			return keys[i].String() < keys[j].String()
		})

		stateObjects = stateObjects[:0]
		for _, key := range keys {
			stateObjects = append(stateObjects, stateObjectEntry{
				key:  key,
				hash: objects[key],
			})
		}

		detailTable.Clear()
		headers := []string{"Idx", "Kind", "Namespace", "Name", "Hash"}
		for col, val := range headers {
			detailTable.SetCell(0, col, headerCell(val))
		}

		if len(stateObjects) == 0 {
			detailTable.SetCell(1, 0,
				tview.NewTableCell("(no objects)").
					SetSelectable(false).
					SetAlign(tview.AlignCenter))
			for col := 1; col < len(headers); col++ {
				detailTable.SetCell(1, col, tview.NewTableCell("").SetSelectable(false))
			}
			stateDetailRow = 0
		} else {
			if stateDetailRow <= 0 || stateDetailRow > len(stateObjects) {
				stateDetailRow = 1
			}
			for idx, entry := range stateObjects {
				key := entry.key
				detailTable.SetCell(idx+1, 0, valueCell(fmt.Sprintf("%d", idx)))
				detailTable.SetCell(idx+1, 1, valueCell(key.ResourceKey.Kind))
				detailTable.SetCell(idx+1, 2, valueCell(key.ResourceKey.Namespace))
				detailTable.SetCell(idx+1, 3, valueCell(key.ResourceKey.Name))
				detailTable.SetCell(idx+1, 4, valueCell(util.ShortenHash(entry.hash.Value)))
			}
		}

		detailTable.SetTitle(fmt.Sprintf("Objects • State %d", selectedState))
		showDetailTable()
		if stateDetailRow > 0 && len(stateObjects) > 0 {
			detailTable.Select(stateDetailRow, 0)
		} else {
			detailTable.Select(0, 0)
		}
		detailTable.SetSelectedFunc(func(row, _ int) {
			if row <= 0 || row-1 >= len(stateObjects) {
				return
			}
			stateDetailRow = row
			showObjectYAML(stateObjects[row-1])
		})
		updateStatus(stateStatusMessage)
	}

	renderPathDetail = func() {
		currentDetailMode = detailNone
		if selectedState < 0 || selectedState >= len(states) {
			showDetailText("Details", "no state selected", pathStatusMessage)
			return
		}
		state := states[selectedState]
		if selectedPath < 0 || selectedPath >= len(state.Paths) {
			showDetailText("Details", fmt.Sprintf("State %d has no path selected", selectedState), pathStatusMessage)
			return
		}
		summary := formatPathSummary(state, selectedPath)
		title := fmt.Sprintf("State %d • Path %d", selectedState, selectedPath)
		showDetailText(title, summary, pathStatusMessage)
	}

	renderStepDetail = func() {
		if selectedState < 0 || selectedState >= len(states) {
			currentDetailMode = detailNone
			showDetailText("Details", "no state selected", stepStatusMessage)
			return
		}
		state := states[selectedState]
		if selectedPath < 0 || selectedPath >= len(state.Paths) {
			currentDetailMode = detailNone
			showDetailText("Details", fmt.Sprintf("State %d has no path selected", selectedState), stepStatusMessage)
			return
		}
		path := state.Paths[selectedPath]
		if len(path) == 0 {
			currentDetailMode = detailNone
			showDetailText("Details", fmt.Sprintf("State %d path %d is empty", selectedState, selectedPath), stepStatusMessage)
			return
		}
		if selectedStep < 0 || selectedStep >= len(path) {
			selectedStep = len(path) - 1
		}
		step := path[selectedStep]

		stepEffects = stepEffects[:0]
		if step != nil {
			for _, eff := range step.Changes.Effects {
				diff := string(step.Deltas[eff.Key])
				if strings.TrimSpace(diff) == "" {
					diff = jsonToYAMLString(eff.Version.Value)
				}
				stepEffects = append(stepEffects, effectEntry{
					effect: eff,
					diff:   diff,
				})
			}
		}

		if len(stepEffects) == 0 {
			currentDetailMode = detailNone
			summary := formatStepSummary(step, selectedStep)
			showDetailText(fmt.Sprintf("State %d • Path %d • Step %d", selectedState, selectedPath, selectedStep), summary, stepStatusMessage)
			return
		}

		currentDetailMode = detailStepEffects
		detailTable.Clear()
		headers := []string{"Idx", "Verb", "Kind", "Namespace", "Name"}
		for col, val := range headers {
			detailTable.SetCell(0, col, headerCell(val))
		}
		if stepDetailRow <= 0 || stepDetailRow > len(stepEffects) {
			stepDetailRow = 1
		}
		for idx, entry := range stepEffects {
			key := entry.effect.Key
			detailTable.SetCell(idx+1, 0, valueCell(fmt.Sprintf("%d", idx)))
			detailTable.SetCell(idx+1, 1, valueCell(string(entry.effect.OpType)))
			detailTable.SetCell(idx+1, 2, valueCell(key.ResourceKey.Kind))
			detailTable.SetCell(idx+1, 3, valueCell(key.ResourceKey.Namespace))
			detailTable.SetCell(idx+1, 4, valueCell(key.ResourceKey.Name))
		}

		controller := "(nil)"
		frame := "-"
		if step != nil {
			controller = step.ControllerID
			frame = util.Shorter(step.FrameID)
		}
		detailTable.SetTitle(fmt.Sprintf("Effects • Step %d (%s @ %s)", selectedStep, controller, frame))
		detailTable.SetSelectedFunc(func(row, _ int) {
			if row <= 0 || row-1 >= len(stepEffects) {
				return
			}
			stepDetailRow = row
			showEffectDiff(stepEffects[row-1])
		})
		showDetailTable()
		detailTable.Select(stepDetailRow, 0)
		updateStatus(stepStatusMessage)
	}

	performDetailAction = func() {
		switch currentDetailMode {
		case detailStateObjects:
			row, _ := detailTable.GetSelection()
			if row <= 0 || row-1 >= len(stateObjects) {
				return
			}
			stateDetailRow = row
			showObjectYAML(stateObjects[row-1])
		case detailStepEffects:
			row, _ := detailTable.GetSelection()
			if row <= 0 || row-1 >= len(stepEffects) {
				return
			}
			stepDetailRow = row
			showEffectDiff(stepEffects[row-1])
		}
	}

	applyMode = func(newMode inspectorMode) {
		mode = newMode
		switch mode {
		case modeStates:
			mainTable.SetTitle("States")
			populateStates(mainTable, states)
			if selectedState >= len(states) {
				if len(states) == 0 {
					selectedState = 0
				} else {
					selectedState = len(states) - 1
				}
			}
			row := 0
			if len(states) > 0 {
				row = selectedState + 1
			}
			mainTable.Select(row, 0)
			if stateSelectionChanged != nil {
				mainTable.SetSelectionChangedFunc(stateSelectionChanged)
			}
			if stateEnter != nil {
				mainTable.SetSelectedFunc(stateEnter)
			}
			renderStateDetail()
		case modePaths:
			mainTable.SetTitle(fmt.Sprintf("Paths • State %d", selectedState))
			populatePaths(mainTable, states, selectedState)
			rowCount := len(states[selectedState].Paths)
			if rowCount == 0 {
				selectedPath = 0
			} else {
				if selectedPath >= rowCount {
					selectedPath = rowCount - 1
				}
			}
			row := 0
			if rowCount > 0 {
				row = selectedPath + 1
			}
			mainTable.Select(row, 0)
			if pathSelectionChanged != nil {
				mainTable.SetSelectionChangedFunc(pathSelectionChanged)
			}
			if pathEnter != nil {
				mainTable.SetSelectedFunc(pathEnter)
			}
			renderPathDetail()
		case modeSteps:
			mainTable.SetTitle(fmt.Sprintf("Steps • State %d • Path %d", selectedState, selectedPath))
			populateSteps(mainTable, states, selectedState, selectedPath)
			path := states[selectedState].Paths[selectedPath]
			if len(path) == 0 {
				selectedStep = 0
			} else if selectedStep >= len(path) {
				selectedStep = len(path) - 1
			}
			row := 0
			if len(path) > 0 {
				row = selectedStep + 1
			}
			mainTable.Select(row, 0)
			if stepSelectionChanged != nil {
				mainTable.SetSelectionChangedFunc(stepSelectionChanged)
			}
			if stepEnter != nil {
				mainTable.SetSelectedFunc(stepEnter)
			}
			renderStepDetail()
		}
	}

	stateSelectionChanged = func(row, _ int) {
		if row <= 0 || row-1 >= len(states) {
			return
		}
		selectedState = row - 1
		selectedPath = 0
		selectedStep = 0
		stateDetailRow = 1
		if mode == modeStates {
			renderStateDetail()
		}
	}

	stateEnter = func(row, _ int) {
		if row <= 0 || row-1 >= len(states) {
			return
		}
		selectedState = row - 1
		selectedPath = 0
		selectedStep = 0
		stateDetailRow = 1
		stepDetailRow = 1
		applyMode(modePaths)
	}

	pathSelectionChanged = func(row, _ int) {
		if row <= 0 {
			return
		}
		current := states[selectedState]
		if row-1 >= len(current.Paths) {
			return
		}
		selectedPath = row - 1
		selectedStep = 0
		stepDetailRow = 1
		if mode == modePaths {
			renderPathDetail()
		}
	}

	pathEnter = func(row, _ int) {
		if row <= 0 {
			return
		}
		current := states[selectedState]
		if row-1 >= len(current.Paths) {
			return
		}
		selectedPath = row - 1
		selectedStep = 0
		stepDetailRow = 1
		if len(current.Paths[selectedPath]) == 0 {
			return
		}
		applyMode(modeSteps)
	}

	stepSelectionChanged = func(row, _ int) {
		if row <= 0 {
			return
		}
		current := states[selectedState]
		if selectedPath >= len(current.Paths) {
			return
		}
		path := current.Paths[selectedPath]
		if row-1 >= len(path) {
			return
		}
		selectedStep = row - 1
		stepDetailRow = 1
		if mode == modeSteps {
			renderStepDetail()
		}
	}

	stepEnter = func(row, _ int) {
		if row <= 0 {
			return
		}
		current := states[selectedState]
		if selectedPath >= len(current.Paths) {
			return
		}
		path := current.Paths[selectedPath]
		if row-1 >= len(path) {
			return
		}
		selectedStep = row - 1
		stepDetailRow = 1
		if mode == modeSteps {
			renderStepDetail()
		}
	}

	applyMode(modeStates)

	return app.SetRoot(root, true).EnableMouse(true).Run()
}

func configureTable(title string, selectable bool) *tview.Table {
	table := tview.NewTable()
	table.SetSelectable(selectable, false)
	table.SetFixed(1, 1)
	table.SetBorders(false)
	table.SetTitle(title)
	table.SetBorder(true)
	return table
}

func populateStates(table *tview.Table, states []tracecheck.ConvergedState) {
	table.Clear()
	headers := []string{"Idx", "Hash", "Objects", "Paths"}
	for col, val := range headers {
		table.SetCell(0, col,
			tview.NewTableCell("[::b]"+val+"[::-]").
				SetSelectable(false))
	}
	for row, state := range states {
		hash := string(state.State.Hash())
		table.SetCell(row+1, 0, tview.NewTableCell(fmt.Sprintf("%d", row)))
		table.SetCell(row+1, 1, tview.NewTableCell(util.ShortenHash(hash)))
		table.SetCell(row+1, 2, tview.NewTableCell(fmt.Sprintf("%d", len(state.State.Objects()))))
		table.SetCell(row+1, 3, tview.NewTableCell(fmt.Sprintf("%d", len(state.Paths))))
	}
}

func populatePaths(table *tview.Table, states []tracecheck.ConvergedState, stateIdx int) {
	table.Clear()
	headers := []string{"Idx", "Steps", "Summary"}
	for col, val := range headers {
		table.SetCell(0, col,
			tview.NewTableCell("[::b]"+val+"[::-]").
				SetSelectable(false))
	}

	if stateIdx < 0 || stateIdx >= len(states) {
		return
	}

	state := states[stateIdx]
	for row, path := range state.Paths {
		table.SetCell(row+1, 0, tview.NewTableCell(fmt.Sprintf("%d", row)))
		table.SetCell(row+1, 1, tview.NewTableCell(fmt.Sprintf("%d", len(path))))
		table.SetCell(row+1, 2, tview.NewTableCell(summarizePath(path)))
	}
}

func populateSteps(table *tview.Table, states []tracecheck.ConvergedState, stateIdx, pathIdx int) {
	table.Clear()
	headers := []string{"Idx", "Controller", "Frame", "Writes"}
	for col, val := range headers {
		table.SetCell(0, col,
			tview.NewTableCell("[::b]"+val+"[::-]").
				SetSelectable(false))
	}

	if stateIdx < 0 || stateIdx >= len(states) {
		return
	}
	state := states[stateIdx]
	if pathIdx < 0 || pathIdx >= len(state.Paths) {
		return
	}

	path := state.Paths[pathIdx]
	for row, step := range path {
		controller := "(nil)"
		frame := "-"
		writes := "0"
		if step != nil {
			controller = step.ControllerID
			frame = util.Shorter(step.FrameID)
			writes = fmt.Sprintf("%d", len(step.Changes.Effects))
		}
		table.SetCell(row+1, 0, tview.NewTableCell(fmt.Sprintf("%d", row)))
		table.SetCell(row+1, 1, tview.NewTableCell(controller))
		table.SetCell(row+1, 2, tview.NewTableCell(frame))
		table.SetCell(row+1, 3, tview.NewTableCell(writes))
	}
}

func headerCell(text string) *tview.TableCell {
	return tview.NewTableCell("[::b]" + text + "[::-]").SetSelectable(false)
}

func valueCell(text string) *tview.TableCell {
	return tview.NewTableCell(text)
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

func formatPathSummary(state tracecheck.ConvergedState, pathIdx int) string {
	if pathIdx < 0 || pathIdx >= len(state.Paths) {
		return fmt.Sprintf("Path %d not found", pathIdx)
	}
	path := state.Paths[pathIdx]
	if len(path) == 0 {
		return "(path is empty)"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Steps: %d\nSummary: %s\n", len(path), summarizePath(path))
	b.WriteString("\nControllers:\n")
	for idx, step := range path {
		if step == nil {
			fmt.Fprintf(&b, "  [%d] (nil)\n", idx)
			continue
		}
		fmt.Fprintf(&b, "  [%d] %s\n", idx, step.ControllerID)
	}
	return b.String()
}

func formatStepSummary(step *tracecheck.ReconcileResult, stepIdx int) string {
	if step == nil {
		return fmt.Sprintf("Step %d has no data", stepIdx)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Controller: %s\nFrame: %s\nType: %s\n", step.ControllerID, util.Shorter(step.FrameID), step.FrameType)
	fmt.Fprintf(&b, "Writes: %d\n", len(step.Changes.Effects))

	if len(step.Changes.ObjectVersions) > 0 {
		b.WriteString("\nObjects:\n")
		b.WriteString(formatObjectVersions(step.Changes.ObjectVersions, "  "))
	}

	if len(step.Changes.Effects) > 0 {
		b.WriteString("\nEffects:\n")
		for idx, eff := range step.Changes.Effects {
			precondition := ""
			if eff.Precondition != nil {
				precondition = " (precondition)"
			}
			fmt.Fprintf(&b, "  [%d] %s %s => %s%s\n", idx, string(eff.OpType), eff.Key.String(), eff.Version.Value, precondition)
		}
	}

	if len(step.Deltas) > 0 {
		b.WriteString("\nDeltas:\n")
		keys := make([]snapshot.CompositeKey, 0, len(step.Deltas))
		for key := range step.Deltas {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			return keys[i].String() < keys[j].String()
		})
		for _, key := range keys {
			fmt.Fprintf(&b, "  %s\n", key.String())
			diffText := strings.TrimSpace(string(step.Deltas[key]))
			if diffText == "" {
				b.WriteString("    (no diff)\n")
				continue
			}
			for _, line := range strings.Split(diffText, "\n") {
				fmt.Fprintf(&b, "    %s\n", line)
			}
		}
	}
	return b.String()
}

func formatObjectVersions(objects tracecheck.ObjectVersions, indent string) string {
	if len(objects) == 0 {
		return indent + "(none)\n"
	}
	keys := make([]snapshot.CompositeKey, 0, len(objects))
	for key := range objects {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].String() < keys[j].String()
	})

	var b strings.Builder
	for _, key := range keys {
		fmt.Fprintf(&b, "%s%s => %s\n", indent, key.String(), objects[key].Value)
	}
	return b.String()
}

func jsonToYAMLString(jsonStr string) string {
	if strings.TrimSpace(jsonStr) == "" {
		return "(empty)"
	}
	out, err := yaml.JSONToYAML([]byte(jsonStr))
	if err != nil {
		return fmt.Sprintf("error converting to yaml: %v", err)
	}
	return string(out)
}
