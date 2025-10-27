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
)

type inspectorMode int

const (
	modeStates inspectorMode = iota
	modePaths
	modeSteps
	modeReconcile
)

type detailTableMode int

const (
	detailNone detailTableMode = iota
	detailStateObjects
	detailStepEffects
)

type stateObjectEntry struct {
	key   snapshot.CompositeKey
	hash  snapshot.VersionHash
	cache *objectCache
}

type effectEntry struct {
	effect tracecheck.Effect
	diff   string
	cache  *objectCache
}

// RunStateInspectorTUIView launches a tview-based inspector for converged/aborted states.
func RunStateInspectorTUIView(states []tracecheck.ResultState) error {
	states = validateResultStates(states)
	states = tracecheck.TrimStatesForInspection(states)
	states = dedupeResultStates(states)

	if len(states) == 0 {
		return fmt.Errorf("no converged states supplied")
	}

	resolverCaches := make(map[tracecheck.VersionManager]*objectCache)
	getCache := func(resolver tracecheck.VersionManager) *objectCache {
		if resolver == nil {
			return nil
		}
		if cache, ok := resolverCaches[resolver]; ok {
			return cache
		}
		cache := newObjectCache(resolver)
		resolverCaches[resolver] = cache
		return cache
	}

	app := tview.NewApplication()

	mainTable := configureTable("States", true)
	detailTable := configureTable("Details", true)
	effectsTable := configureTable("Effects", true)
	detailText := tview.NewTextView()
	detailText.SetDynamicColors(true)
	detailText.SetWrap(true)
	detailText.SetTitle("Details")
	detailText.SetBorder(true)
	detailContainer := tview.NewFlex()
	detailContainer.SetDirection(tview.FlexRow)

	contentFlex := tview.NewFlex()
	currentDetailPrim := tview.Primitive(detailTable)

	statusBar := tview.NewTextView().
		SetDynamicColors(true).
		SetText(`[yellow]<Tab>[-] to move focus • [yellow]Enter[-] to select • [yellow]q[-] to quit`).
		SetTextAlign(tview.AlignCenter)

	root := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(contentFlex, 0, 1, true).
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
		stateStatusMessage     = `[yellow]Enter/d[-] describe object • [yellow]Tab[-] swap focus • [yellow]q[-] quit`
		stateDescribeStatus    = `[yellow]Esc[-] back • [yellow]q[-] quit`
		pathStatusMessage      = `[yellow]Enter[-] open steps • [yellow]Esc[-] back • [yellow]Tab[-] swap focus • [yellow]q[-] quit`
		stepStatusMessage      = `[yellow]Enter/d[-] inspect reconcile • [yellow]Esc[-] back • [yellow]Tab[-] swap focus • [yellow]q[-] quit`
		stepDescribeStatus     = `[yellow]Esc[-] back • [yellow]q[-] quit`
		reconcileStatusMessage = `[yellow]Esc[-] back • [yellow]Tab[-] swap focus • [yellow]q[-] quit`
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
		renderStateDetail     func()
		renderPathDetail      func()
		renderStepDetail      func()
		renderReconcileDetail func()
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
		case modeReconcile:
			applyMode(modeSteps)
			return true
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
			if mode == modeSteps {
				currentDetailPrim = effectsTable
				currentDetailMode = detailStepEffects
				app.SetFocus(effectsTable)
			} else {
				app.SetFocus(mainTable)
			}
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

	effectsTable.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyTab:
			if mode == modeSteps {
				app.SetFocus(mainTable)
			} else if mode == modeReconcile {
				currentDetailPrim = detailText
				app.SetFocus(detailText)
			}
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
			if mode == modeReconcile {
				currentDetailPrim = effectsTable
				app.SetFocus(effectsTable)
			} else {
				app.SetFocus(mainTable)
			}
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
		}
	})

	buildEffectDetail := func(entry effectEntry) (string, string) {
		key := entry.effect.Key
		title := fmt.Sprintf("%s %s/%s/%s", string(entry.effect.OpType), key.ResourceKey.Kind, key.ResourceKey.Namespace, key.ResourceKey.Name)
		diff := entry.diff
		if strings.TrimSpace(diff) == "" {
			if entry.cache != nil {
				if yamlStr, err := entry.cache.YAML(entry.effect.Version); err == nil {
					diff = yamlStr
				} else {
					diff = formatResolveError(entry.effect.Version, err)
				}
			} else {
				diff = formatResolverUnavailable(entry.effect.Version)
			}
		}
		return title, diff
	}

	effectsTable.SetSelectionChangedFunc(func(row, _ int) {
		if row <= 0 {
			return
		}
		stepDetailRow = row
		if mode == modeReconcile && row-1 < len(stepEffects) {
			title, body := buildEffectDetail(stepEffects[row-1])
			detailText.SetTitle(title)
			detailText.SetText(body)
		}
	})

	showObjectYAML := func(entry stateObjectEntry) {
		row := stateDetailRow
		title := fmt.Sprintf("Object %s", formatResourceTitle(entry.key))
		var body string
		if entry.cache != nil {
			if yamlStr, err := entry.cache.YAML(entry.hash); err == nil {
				body = yamlStr
			} else {
				body = formatResolveError(entry.hash, err)
			}
		} else {
			body = formatResolverUnavailable(entry.hash)
		}
		showDetailText(title, body, stateDescribeStatus)
		returnFromText = func() {
			stateDetailRow = row
			if mode == modeSteps {
				renderStepDetail()
			} else {
				renderStateDetail()
			}
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
		cache := getCache(state.Resolver)
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
				key:   key,
				hash:  objects[key],
				cache: cache,
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

		title := fmt.Sprintf("Objects • State %d", selectedState)
		if state.Reason != "" {
			title = fmt.Sprintf("%s (%s)", title, state.Reason)
		}
		detailTable.SetTitle(title)
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
			detailContainer.Clear()
			detailContainer.AddItem(detailText, 0, 1, false)
			detailText.SetTitle("Details")
			detailText.SetText("no state selected")
			currentDetailPrim = detailText
			updateStatus(stepStatusMessage)
			return
		}
		state := states[selectedState]
		if selectedPath < 0 || selectedPath >= len(state.Paths) {
			currentDetailMode = detailNone
			detailContainer.Clear()
			detailContainer.AddItem(detailText, 0, 1, false)
			detailText.SetTitle("Details")
			detailText.SetText(fmt.Sprintf("State %d has no path selected", selectedState))
			currentDetailPrim = detailText
			updateStatus(stepStatusMessage)
			return
		}
		path := state.Paths[selectedPath]
		if len(path) == 0 {
			currentDetailMode = detailNone
			detailContainer.Clear()
			detailContainer.AddItem(detailText, 0, 1, false)
			detailText.SetTitle("Details")
			detailText.SetText(fmt.Sprintf("State %d path %d is empty", selectedState, selectedPath))
			currentDetailPrim = detailText
			updateStatus(stepStatusMessage)
			return
		}
		if selectedStep < 0 || selectedStep >= len(path) {
			selectedStep = len(path) - 1
		}
		step := path[selectedStep]

		detailContainer.Clear()
		detailContainer.SetDirection(tview.FlexRow)
		currentDetailPrim = detailTable
		currentDetailMode = detailStateObjects

		// Populate state objects using the stored post-step state snapshot
		stateObjects = stateObjects[:0]
		var stateMap tracecheck.ObjectVersions
		if step != nil && step.StateAfter != nil {
			stateMap = step.StateAfter
		} else {
			stateMap = state.State.Objects()
		}
		if len(stateMap) > 0 {
			keys := make([]snapshot.CompositeKey, 0, len(stateMap))
			for key := range stateMap {
				keys = append(keys, key)
			}
			sort.Slice(keys, func(i, j int) bool {
				return keys[i].String() < keys[j].String()
			})
			for _, key := range keys {
				stateObjects = append(stateObjects, stateObjectEntry{
					key:   key,
					hash:  stateMap[key],
					cache: getCache(state.Resolver),
				})
			}
		}

		detailTable.Clear()
		headers := []string{"Idx", "Kind", "Namespace", "Name", "Hash"}
		for col, val := range headers {
			detailTable.SetCell(0, col, headerCell(val))
		}
		if len(stateObjects) == 0 {
			detailTable.SetCell(1, 0,
				valueCell("(no objects)").
					SetSelectable(false).
					SetAlign(tview.AlignCenter))
			for col := 1; col < len(headers); col++ {
				detailTable.SetCell(1, col, valueCell("").SetSelectable(false))
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

		controller := "(nil)"
		frame := "-"
		if step != nil {
			controller = step.ControllerID
			frame = util.Shorter(step.FrameID)
		}
		detailTable.SetTitle(fmt.Sprintf("State • Step %d (%s @ %s)", selectedStep, controller, frame))
		detailTable.SetSelectedFunc(func(row, _ int) {
			if row <= 0 || row-1 >= len(stateObjects) {
				return
			}
			stateDetailRow = row
			showObjectYAML(stateObjects[row-1])
		})
		detailContainer.AddItem(detailTable, 0, 3, false)
		if stateDetailRow > 0 && len(stateObjects) > 0 {
			detailTable.Select(stateDetailRow, 0)
		} else {
			detailTable.Select(0, 0)
		}

		// Populate effects bottom panel
		stepEffects = stepEffects[:0]
		cache := getCache(state.Resolver)
		if step != nil {
			for _, eff := range step.Changes.Effects {
				diff := normalizeDeltaPresentation(string(step.Deltas[eff.Key]))
				if strings.TrimSpace(diff) == "" {
					if cache != nil {
						if yamlStr, err := cache.YAML(eff.Version); err == nil {
							diff = yamlStr
						} else {
							diff = formatResolveError(eff.Version, err)
						}
					} else {
						diff = formatResolverUnavailable(eff.Version)
					}
				}
				stepEffects = append(stepEffects, effectEntry{
					effect: eff,
					diff:   diff,
					cache:  cache,
				})
			}
		}

		effectsTable.Clear()
		if len(stepEffects) == 0 {
			effectsTable.SetTitle("Effects • (none)")
			effectsTable.SetCell(0, 0, valueCell("(no effects)").SetSelectable(false).SetAlign(tview.AlignCenter))
			effectsTable.SetSelectedFunc(nil)
			stepDetailRow = 0
		} else {
			headers := []string{"Idx", "Verb", "Kind", "Namespace", "Name"}
			for col, val := range headers {
				effectsTable.SetCell(0, col, headerCell(val))
			}
			if stepDetailRow <= 0 || stepDetailRow > len(stepEffects) {
				stepDetailRow = 1
			}
			for idx, entry := range stepEffects {
				key := entry.effect.Key
				effectsTable.SetCell(idx+1, 0, valueCell(fmt.Sprintf("%d", idx)))
				effectsTable.SetCell(idx+1, 1, valueCell(string(entry.effect.OpType)))
				effectsTable.SetCell(idx+1, 2, valueCell(key.ResourceKey.Kind))
				effectsTable.SetCell(idx+1, 3, valueCell(key.ResourceKey.Namespace))
				effectsTable.SetCell(idx+1, 4, valueCell(key.ResourceKey.Name))
			}
			effectsTable.Select(stepDetailRow, 0)
			effectsTable.SetTitle(fmt.Sprintf("Effects • Step %d (%s @ %s)", selectedStep, controller, frame))
			effectsTable.SetSelectedFunc(nil)
		}
		detailContainer.AddItem(effectsTable, 0, 2, false)
		currentDetailMode = detailStateObjects
		updateStatus(stepStatusMessage)
	}

	renderReconcileDetail = func() {
		if selectedState < 0 || selectedState >= len(states) {
			detailText.SetTitle("Effect Detail")
			detailText.SetText("no state selected")
			updateStatus(reconcileStatusMessage)
			return
		}

		state := states[selectedState]
		if selectedPath < 0 || selectedPath >= len(state.Paths) {
			detailText.SetTitle("Effect Detail")
			detailText.SetText("no path selected")
			updateStatus(reconcileStatusMessage)
			return
		}

		path := state.Paths[selectedPath]
		if len(path) == 0 {
			detailText.SetTitle("Effect Detail")
			detailText.SetText("path is empty")
			updateStatus(reconcileStatusMessage)
			return
		}

		if selectedStep < 0 || selectedStep >= len(path) {
			selectedStep = len(path) - 1
		}

		controller := "(nil)"
		frame := "-"
		step := path[selectedStep]
		if step != nil {
			controller = step.ControllerID
			frame = util.Shorter(step.FrameID)
		}

		effectsTable.Clear()
		if len(stepEffects) == 0 {
			effectsTable.SetTitle(fmt.Sprintf("Effects • Step %d (%s @ %s)", selectedStep, controller, frame))
			effectsTable.SetCell(0, 0, valueCell("(no effects)").SetSelectable(false).SetAlign(tview.AlignCenter))
			detailText.SetTitle("Effect Detail")
			detailText.SetText("(no effects to display)")
			updateStatus(reconcileStatusMessage)
			return
		}

		headers := []string{"Idx", "Verb", "Kind", "Namespace", "Name"}
		for col, val := range headers {
			effectsTable.SetCell(0, col, headerCell(val))
		}
		if stepDetailRow <= 0 || stepDetailRow > len(stepEffects) {
			stepDetailRow = 1
		}
		for idx, entry := range stepEffects {
			key := entry.effect.Key
			effectsTable.SetCell(idx+1, 0, valueCell(fmt.Sprintf("%d", idx)))
			effectsTable.SetCell(idx+1, 1, valueCell(string(entry.effect.OpType)))
			effectsTable.SetCell(idx+1, 2, valueCell(key.ResourceKey.Kind))
			effectsTable.SetCell(idx+1, 3, valueCell(key.ResourceKey.Namespace))
			effectsTable.SetCell(idx+1, 4, valueCell(key.ResourceKey.Name))
		}
		effectsTable.SetTitle(fmt.Sprintf("Effects • Step %d (%s @ %s)", selectedStep, controller, frame))
		effectsTable.SetSelectedFunc(func(row, _ int) {
			if row <= 0 || row-1 >= len(stepEffects) {
				return
			}
			stepDetailRow = row
			title, body := buildEffectDetail(stepEffects[row-1])
			detailText.SetTitle(title)
			detailText.SetText(body)
		})
		effectsTable.Select(stepDetailRow, 0)
		if stepDetailRow > 0 && stepDetailRow <= len(stepEffects) {
			title, body := buildEffectDetail(stepEffects[stepDetailRow-1])
			detailText.SetTitle(title)
			detailText.SetText(body)
		}
		currentDetailMode = detailStepEffects
		currentDetailPrim = effectsTable
		updateStatus(reconcileStatusMessage)
	}

	performDetailAction = func() {
		switch app.GetFocus() {
		case detailTable:
			row, _ := detailTable.GetSelection()
			if row <= 0 || row-1 >= len(stateObjects) {
				return
			}
			stateDetailRow = row
			showObjectYAML(stateObjects[row-1])
		case effectsTable:
			row, _ := effectsTable.GetSelection()
			if row <= 0 || row-1 >= len(stepEffects) {
				return
			}
			stepDetailRow = row
			if mode == modeReconcile {
				title, body := buildEffectDetail(stepEffects[row-1])
				detailText.SetTitle(title)
				detailText.SetText(body)
				currentDetailPrim = detailText
				updateStatus(reconcileStatusMessage)
			} else {
				applyMode(modeReconcile)
			}
		default:
			switch currentDetailMode {
			case detailStateObjects:
				row, _ := detailTable.GetSelection()
				if row <= 0 || row-1 >= len(stateObjects) {
					return
				}
				stateDetailRow = row
				showObjectYAML(stateObjects[row-1])
			case detailStepEffects:
				row, _ := effectsTable.GetSelection()
				if row <= 0 || row-1 >= len(stepEffects) {
					return
				}
				stepDetailRow = row
				if mode == modeReconcile {
					title, body := buildEffectDetail(stepEffects[row-1])
					detailText.SetTitle(title)
					detailText.SetText(body)
					currentDetailPrim = detailText
					updateStatus(reconcileStatusMessage)
				} else {
					applyMode(modeReconcile)
				}
			}
		}
	}

	applyMode = func(newMode inspectorMode) {
		mode = newMode
		returnFromText = nil
		switch mode {
		case modeStates:
			contentFlex.Clear()
			contentFlex.SetDirection(tview.FlexRow)
			contentFlex.AddItem(mainTable, 0, 5, true)
			detailContainer.Clear()
			detailContainer.SetDirection(tview.FlexRow)
			detailContainer.AddItem(detailTable, 0, 1, false)
			contentFlex.AddItem(detailContainer, 0, 3, false)
			currentDetailPrim = detailTable
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
			contentFlex.Clear()
			contentFlex.SetDirection(tview.FlexColumn)
			contentFlex.AddItem(mainTable, 0, 2, true)
			detailContainer.Clear()
			detailContainer.SetDirection(tview.FlexRow)
			contentFlex.AddItem(detailContainer, 0, 3, false)
			currentDetailPrim = detailTable
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
		case modeReconcile:
			contentFlex.Clear()
			contentFlex.SetDirection(tview.FlexColumn)
			contentFlex.AddItem(effectsTable, 0, 2, true)
			contentFlex.AddItem(detailText, 0, 3, false)
			currentDetailPrim = effectsTable
			renderReconcileDetail()
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
			if len(stepEffects) > 0 {
				applyMode(modeReconcile)
			}
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

func populateStates(table *tview.Table, states []tracecheck.ResultState) {
	table.Clear()
	headers := []string{"Idx", "Hash", "Objects", "Paths", "Reason"}
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
		table.SetCell(row+1, 4, tview.NewTableCell(state.Reason))
	}
}

func populatePaths(table *tview.Table, states []tracecheck.ResultState, stateIdx int) {
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

func populateSteps(table *tview.Table, states []tracecheck.ResultState, stateIdx, pathIdx int) {
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

func formatPathSummary(state tracecheck.ResultState, pathIdx int) string {
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
			diffText := strings.TrimSpace(normalizeDeltaPresentation(string(step.Deltas[key])))
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

func formatResolverUnavailable(hash snapshot.VersionHash) string {
	return fmt.Sprintf("object content unavailable; strategy=%s hash=%s (%s)", hash.Strategy, util.ShortenHash(hash.Value), hash.Value)
}

func formatResolveError(hash snapshot.VersionHash, err error) string {
	return fmt.Sprintf("error retrieving object (%s, %s): %v\nfull hash: %s", hash.Strategy, util.ShortenHash(hash.Value), err, hash.Value)
}

func formatResourceTitle(key snapshot.CompositeKey) string {
	namespace := key.ResourceKey.Namespace
	if namespace != "" {
		return fmt.Sprintf("%s %s/%s", key.ResourceKey.Kind, namespace, key.ResourceKey.Name)
	}
	return fmt.Sprintf("%s %s", key.ResourceKey.Kind, key.ResourceKey.Name)
}

// TODO : this is a bit of a hack to clean up the delta presentation
// produced by tracecheck. Ideally the diff generation would be improved
// upstream to avoid the need for this.
func normalizeDeltaPresentation(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}

	// unwrap surrounding parentheses that godebug/diff adds
	if strings.HasPrefix(trimmed, "(") && strings.HasSuffix(trimmed, ")") {
		trimmed = strings.TrimSpace(trimmed[1 : len(trimmed)-1])
	}

	lines := strings.Split(trimmed, "\n")
	filtered := lines[:0]
	for _, line := range lines {
		if strings.TrimSpace(line) == `"""` {
			continue
		}
		filtered = append(filtered, line)
	}

	minIndent := -1
	for _, line := range filtered {
		if strings.TrimSpace(line) == "" {
			continue
		}
		indent := leadingWhitespaceCount(line)
		if minIndent == -1 || indent < minIndent {
			minIndent = indent
		}
	}

	if minIndent > 0 {
		for i, line := range filtered {
			if strings.TrimSpace(line) == "" {
				filtered[i] = ""
				continue
			}
			filtered[i] = trimLeadingWhitespace(line, minIndent)
		}
	}

	return strings.TrimSpace(strings.Join(filtered, "\n"))
}

func leadingWhitespaceCount(s string) int {
	count := 0
	for _, r := range s {
		if r == ' ' || r == '\t' {
			count++
			continue
		}
		break
	}
	return count
}

func trimLeadingWhitespace(s string, count int) string {
	if count <= 0 {
		return s
	}
	consumed := 0
	for i, r := range s {
		if consumed >= count {
			return s[i:]
		}
		if r == ' ' || r == '\t' {
			consumed++
			continue
		}
		return s[i:]
	}
	return ""
}
