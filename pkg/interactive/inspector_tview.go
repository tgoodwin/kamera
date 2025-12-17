package interactive

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"maps"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/samber/lo"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	"github.com/tgoodwin/kamera/pkg/util"
	"golang.org/x/exp/slices"
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
	gvk   string
}

type effectEntry struct {
	effect   tracecheck.Effect
	diff     string
	cache    *objectCache
	delta    string
	cacheRef *stepCache
	cacheIdx int
	gvk      string
}

func (e *effectEntry) ensureDiff() string {
	if strings.TrimSpace(e.diff) != "" {
		return e.diff
	}

	if e.cacheRef != nil && e.cacheIdx >= 0 {
		if len(e.cacheRef.effectDiffsCached) > e.cacheIdx && e.cacheRef.effectDiffsCached[e.cacheIdx] {
			if len(e.cacheRef.effectDiffs) > e.cacheIdx {
				cached := e.cacheRef.effectDiffs[e.cacheIdx]
				if strings.TrimSpace(cached) != "" {
					e.diff = cached
					return e.diff
				}
			}
		}
	}

	diff := ""
	if strings.TrimSpace(e.delta) != "" {
		diff = normalizeDeltaPresentation(e.delta)
	}

	if strings.TrimSpace(diff) == "" {
		if e.cache != nil {
			if yamlStr, err := e.cache.YAML(e.effect.Version); err == nil {
				diff = yamlStr
			} else {
				diff = formatResolveError(e.effect.Version, err)
			}
		} else {
			diff = formatResolverUnavailable(e.effect.Version)
		}
	}

	e.diff = diff
	if e.cacheRef != nil && e.cacheIdx >= 0 {
		if len(e.cacheRef.effectDiffs) > e.cacheIdx {
			e.cacheRef.effectDiffs[e.cacheIdx] = diff
		}
		if len(e.cacheRef.effectDiffsCached) > e.cacheIdx {
			e.cacheRef.effectDiffsCached[e.cacheIdx] = true
		}
	}

	return e.diff
}

type stepCache struct {
	stateKeys         []snapshot.CompositeKey
	effectDiffs       []string
	effectDiffsCached []bool
}

// RunStateInspectorTUIView launches a tview-based inspector for converged/aborted states.
// When allowDump is false, the dump shortcut is disabled (used for trace-hydrated sessions).
// If the user requests a restart, the inspector exits and returns a RestartRequest to the caller.
func RunStateInspectorTUIView(states []tracecheck.ResultState, resolver tracecheck.VersionManager, allowDump bool, cfg tracecheck.ExploreConfig) (*tracecheck.RestartRequest, error) {
	states = validateResultStates(states)
	states = tracecheck.TrimStatesForInspection(states)
	states = dedupeResultStates(states)

	if len(states) == 0 {
		return nil, fmt.Errorf("no converged states supplied")
	}

	var resolverCache *objectCache
	if resolver != nil {
		resolverCache = newObjectCache(resolver)
	}
	getCache := func() *objectCache { return resolverCache }

	app := tview.NewApplication()
	pages := tview.NewPages()
	const comparePageName = "compare"

	mainTable := configureTable("States", true)
	detailTable := configureTable("Details", true)
	effectsTable := configureTable("Effects", true)
	pendingReconcilesTable := configureTable("Pending Reconciles", true)
	detailText := tview.NewTextView()
	detailText.SetDynamicColors(true)
	detailText.SetWrap(true)
	detailText.SetTitle("Details")
	detailText.SetBorder(true)
	detailContainer := tview.NewFlex()
	detailContainer.SetDirection(tview.FlexRow)

	contentFlex := tview.NewFlex()
	currentDetailPrim := tview.Primitive(detailTable)

	dumpHint := ""
	dumpShortcut := ""
	if allowDump {
		dumpHint = " • [yellow]s/Ctrl+S[-] dump"
		dumpShortcut = dumpHint
	}
	restartHint := " • [yellow]r[-] restart"

	statusBar := tview.NewTextView().
		SetDynamicColors(true).
		SetText(`[yellow]<Tab>[-] move • [yellow]Enter[-] select` + dumpHint + ` • [yellow]q[-] quit`).
		SetTextAlign(tview.AlignCenter)

	root := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(contentFlex, 0, 1, true).
		AddItem(statusBar, 1, 0, false)
	pages.AddPage("main", root, true, true)

	var (
		selectedState         = 0
		selectedPath          = 0
		selectedStep          = 0
		mode                  = modeStates
		currentDetailMode     detailTableMode
		stateObjects          []stateObjectEntry
		stepEffects           []effectEntry
		stepPendingReconciles []tracecheck.PendingReconcile
		returnFromText        func()
		stateDetailRow        = 1
		stepDetailRow         = 1
		pendingDetailRow      = 1
	)

	layoutMode := ""
	stateDetailDirty := true
	pathDetailDirty := true
	stepDetailDirty := true
	reconcileDirty := true
	currentConfig := cfg.Clone()
	lastStepState := -1
	lastStepPath := -1
	lastStepIdx := -1
	stepCaches := make(map[*tracecheck.ReconcileResult]*stepCache)
	var restartRequest *tracecheck.RestartRequest

	getStepCache := func(step *tracecheck.ReconcileResult) *stepCache {
		if step == nil {
			return nil
		}
		if cache, ok := stepCaches[step]; ok {
			if len(step.Changes.Effects) > 0 {
				if len(cache.effectDiffs) != len(step.Changes.Effects) {
					cache.effectDiffs = make([]string, len(step.Changes.Effects))
					cache.effectDiffsCached = make([]bool, len(step.Changes.Effects))
				}
			} else {
				cache.effectDiffs = nil
				cache.effectDiffsCached = nil
			}
			return cache
		}
		cache := &stepCache{}
		if len(step.Changes.Effects) > 0 {
			cache.effectDiffs = make([]string, len(step.Changes.Effects))
			cache.effectDiffsCached = make([]bool, len(step.Changes.Effects))
		}
		stepCaches[step] = cache
		return cache
	}

	baseQuit := " • [yellow]q[-] quit"
	stateStatusMessage := `[yellow]Enter/d[-] describe object • [yellow]c[-] compare across states • [yellow]Tab[-] swap focus` + dumpShortcut + baseQuit
	stateDescribeStatus := `[yellow]Esc[-] back` + dumpShortcut + baseQuit
	pathStatusMessage := `[yellow]Enter[-] open steps • [yellow]Esc[-] back • [yellow]Tab[-] swap focus` + dumpShortcut + baseQuit
	stepStatusMessage := `[yellow]Enter/d[-] inspect reconcile • [yellow]Esc[-] back • [yellow]Tab[-] swap focus` + restartHint + dumpShortcut + baseQuit
	reconcileStatusMessage := `[yellow]Esc[-] back • [yellow]Tab[-] swap focus` + restartHint + dumpShortcut + baseQuit

	var (
		stateSelectionChanged func(int, int)
		stateEnter            func(int, int)
		pathSelectionChanged  func(int, int)
		pathEnter             func(int, int)
		stepSelectionChanged  func(int, int)
		stepEnter             func(int, int)
		performDetailAction   func()
		setFocusForMode       func()
	)

	var (
		renderStateDetail     func()
		renderPathDetail      func()
		renderStepDetail      func()
		renderReconcileDetail func()
	)

	setFocusForMode = func() {}

	updateStatus := func(text string) {
		statusBar.SetText(text)
	}

	fallbackGVK := func(key snapshot.CompositeKey) string {
		kind := strings.TrimSpace(key.ResourceKey.Kind)
		if kind == "" {
			kind = strings.TrimSpace(key.IdentityKey.Kind)
		}
		if kind == "" {
			kind = "?"
		}
		return fmt.Sprintf("?/?/%s", kind)
	}

	resolveGVK := func(cache *objectCache, hash snapshot.VersionHash, key snapshot.CompositeKey) string {
		if cache != nil {
			if gvk, ok := cache.GVKString(hash); ok && strings.TrimSpace(gvk) != "" {
				return gvk
			}
		}
		return fallbackGVK(key)
	}
	if allowDump {
		promptDump := func() {
			var path string
			ok := app.Suspend(func() {
				reader := bufio.NewReader(os.Stdin)
				fmt.Print("\nDump file path (empty to cancel): ")
				input, _ := reader.ReadString('\n')
				path = strings.TrimSpace(input)
				fmt.Println()
				if path != "" {
					fmt.Printf("saving to %s\n", path)
				}
			})
			if !ok {
				updateStatus("[red]dump suspended: unable to pause application[-]")
				return
			}
			if path == "" {
				updateStatus(`[yellow]dump cancelled[-]`)
				return
			}
			if err := SaveInspectorDump(states, resolver, path); err != nil {
				updateStatus(fmt.Sprintf("[red]dump failed: %v[-]", err))
				return
			}
			updateStatus(fmt.Sprintf("[green]dumped to %s[-]", path))
		}

		app.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
			switch {
			case event.Key() == tcell.KeyCtrlS:
				promptDump()
				return nil
			case event.Rune() == 's' || event.Rune() == 'S':
				promptDump()
				return nil
			}
			return event
		})
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

	showConfirm := func(seed tracecheck.RestartSeed, prefix tracecheck.ExecutionHistory) {
		nextCfg := currentConfig.Clone()
		permuteSelections := maps.Clone(nextCfg.PermuteOrder)
		if permuteSelections == nil {
			permuteSelections = make(map[tracecheck.ReconcilerID]bool)
		}

		reconcilerIDs := lo.Keys(permuteSelections)
		if len(reconcilerIDs) == 0 {
			observed := make(map[tracecheck.ReconcilerID]struct{})
			for _, st := range states {
				for _, pr := range st.State.PendingReconciles {
					observed[pr.ReconcilerID] = struct{}{}
				}
				for _, path := range st.Paths {
					for _, step := range path {
						if step != nil {
							observed[step.ControllerID] = struct{}{}
						}
					}
				}
			}
			for id := range observed {
				if _, ok := permuteSelections[id]; !ok {
					permuteSelections[id] = false
				}
			}
			reconcilerIDs = lo.Keys(permuteSelections)
		}
		sort.Slice(reconcilerIDs, func(i, j int) bool { return string(reconcilerIDs[i]) < string(reconcilerIDs[j]) })

		form := tview.NewForm().
			AddInputField("Max Depth", fmt.Sprintf("%d", nextCfg.MaxDepth), 0, nil, nil).
			AddInputField("Timeout", nextCfg.Timeout.String(), 0, nil, nil)
		for _, id := range reconcilerIDs {
			id := id
			form.AddCheckbox(fmt.Sprintf("Permute order: %s", id), permuteSelections[id], func(checked bool) {
				permuteSelections[id] = checked
			})
		}
		form.AddButton("OK", func() {
			maxDepthStr := form.GetFormItemByLabel("Max Depth").(*tview.InputField).GetText()
			timeoutStr := form.GetFormItemByLabel("Timeout").(*tview.InputField).GetText()
			if strings.TrimSpace(maxDepthStr) != "" {
				if val, err := strconv.Atoi(strings.TrimSpace(maxDepthStr)); err == nil && val > 0 {
					nextCfg.MaxDepth = val
				} else {
					updateStatus("[red]invalid max depth[-]")
					return
				}
			}
			if strings.TrimSpace(timeoutStr) != "" {
				if d, err := time.ParseDuration(strings.TrimSpace(timeoutStr)); err == nil {
					nextCfg.Timeout = d
				} else {
					updateStatus("[red]invalid timeout[-]")
					return
				}
			}
			nextCfg.PermuteOrder = permuteSelections
			restartRequest = &tracecheck.RestartRequest{
				Seed:            seed,
				Config:          nextCfg,
				PreserveHistory: true,
				Prefix:          prefix,
			}
			app.Stop()
		})
		form.AddButton("Cancel", func() {
			pages.RemovePage("confirm")
			app.SetFocus(mainTable)
		})
		form.SetButtonsAlign(tview.AlignCenter)
		// Focus the first button (OK) so Enter immediately confirms.
		form.SetFocus(form.GetFormItemCount())
		form.SetBorder(true).SetTitle("Restart Config").SetTitleAlign(tview.AlignLeft)
		pages.AddAndSwitchToPage("confirm", form, true)
		app.SetFocus(form)
	}

	showCompare := func(entry stateObjectEntry) {
		type compareEntry struct {
			stateIdx int
			hash     snapshot.VersionHash
			cache    *objectCache
		}
		compareEntries := make([]compareEntry, 0)
		for idx, st := range states {
			if hv, ok := st.State.Objects()[entry.key]; ok {
				compareEntries = append(compareEntries, compareEntry{
					stateIdx: idx,
					hash:     hv,
					cache:    getCache(),
				})
			}
		}
		if len(compareEntries) == 0 {
			updateStatus("[yellow]object not found in other states[-]")
			return
		}

		table := configureTable("States with object", true)
		yamlView := tview.NewTextView()
		yamlView.SetDynamicColors(true)
		yamlView.SetWrap(true)
		yamlView.SetBorder(true)
		yamlView.SetTitle("Object YAML")

		renderSelection := func(row int) {
			if row < 1 || row-1 >= len(compareEntries) {
				yamlView.SetText("(no selection)")
				return
			}
			ce := compareEntries[row-1]
			yamlStr := "(unavailable)"
			if ce.cache != nil {
				if txt, err := ce.cache.YAML(ce.hash); err == nil {
					yamlStr = txt
				} else {
					yamlStr = formatResolveError(ce.hash, err)
				}
			} else {
				yamlStr = formatResolverUnavailable(ce.hash)
			}
			yamlView.SetTitle(fmt.Sprintf("State %d • Hash %s", ce.stateIdx, util.ShortenHash(ce.hash.Value)))
			yamlView.SetText(yamlStr)
		}

		table.SetCell(0, 0, headerCell("Idx"))
		table.SetCell(0, 1, headerCell("State"))
		table.SetCell(0, 2, headerCell("Hash"))
		for i, ce := range compareEntries {
			table.SetCell(i+1, 0, valueCell(fmt.Sprintf("%d", i)))
			table.SetCell(i+1, 1, valueCell(fmt.Sprintf("%d", ce.stateIdx)))
			table.SetCell(i+1, 2, valueCell(util.ShortenHash(ce.hash.Value)))
		}
		table.SetSelectedFunc(func(row, _ int) {
			renderSelection(row)
		})
		table.SetSelectionChangedFunc(func(row, _ int) {
			renderSelection(row)
		})
		if len(compareEntries) > 0 {
			table.Select(1, 0)
			renderSelection(1)
		}

		modal := tview.NewFlex().SetDirection(tview.FlexColumn).
			AddItem(table, 0, 1, true).
			AddItem(yamlView, 0, 2, false)
		modal.SetBorder(true).SetTitle("Object across states")

		overlay := tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(modal, 0, 1, true)

		pages.AddAndSwitchToPage(comparePageName, overlay, true)
		app.SetFocus(table)

		overlay.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
			switch event.Key() {
			case tcell.KeyEscape:
				pages.RemovePage(comparePageName)
				app.SetFocus(mainTable)
				return nil
			}
			switch event.Rune() {
			case 'q', 'Q':
				pages.RemovePage(comparePageName)
				app.SetFocus(mainTable)
				return nil
			}
			return event
		})
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

	restartFromStep := func() {
		if selectedState < 0 || selectedState >= len(states) {
			updateStatus("[yellow]select a state first[-]")
			return
		}
		if selectedPath < 0 || selectedPath >= len(states[selectedState].Paths) {
			updateStatus("[yellow]select a path first[-]")
			return
		}
		path := states[selectedState].Paths[selectedPath]
		if len(path) == 0 {
			updateStatus("[yellow]selected path is empty[-]")
			return
		}
		if selectedStep < 0 || selectedStep >= len(path) {
			selectedStep = len(path) - 1
		}
		step := path[selectedStep]
		if step == nil || step.StateAfter == nil {
			updateStatus("[red]selected step has no state snapshot[-]")
			return
		}
		if resolver == nil {
			updateStatus("[red]resolver unavailable for restart[-]")
			return
		}

		seed, err := tracecheck.BuildRestartSeedFromState(step.StateAfter, resolver, step.PendingReconciles)
		if err != nil {
			updateStatus(fmt.Sprintf("[red]restart seed failed: %v[-]", err))
			return
		}

		state := states[selectedState]
		prefix := slices.Clone(state.Paths[selectedPath][:selectedStep+1])
		seed.Depth = len(prefix)
		showConfirm(seed, prefix)
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
		case 'r', 'R':
			if mode == modeSteps || mode == modeReconcile {
				restartFromStep()
				return nil
			}
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
		case 'c', 'C':
			if currentDetailMode == detailStateObjects && mode == modeStates && stateDetailRow > 0 && stateDetailRow-1 < len(stateObjects) {
				showCompare(stateObjects[stateDetailRow-1])
				return nil
			}
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
		case 'c', 'C':
			if currentDetailMode == detailStateObjects && mode == modeStates && stateDetailRow > 0 && stateDetailRow-1 < len(stateObjects) {
				showCompare(stateObjects[stateDetailRow-1])
				return nil
			}
		}
		return event
	})

	detailTable.SetSelectionChangedFunc(func(row, _ int) {
		switch currentDetailMode {
		case detailStateObjects:
			stateDetailRow = row
		}
	})

	buildEffectDetail := func(entry *effectEntry) (string, string) {
		key := entry.effect.Key
		title := fmt.Sprintf("%s %s %s/%s", string(entry.effect.OpType), entry.gvk, key.ResourceKey.Namespace, key.ResourceKey.Name)
		diff := entry.ensureDiff()
		return title, diff
	}

	effectsTable.SetSelectionChangedFunc(func(row, _ int) {
		if row <= 0 {
			return
		}
		stepDetailRow = row
		if mode == modeReconcile && row-1 < len(stepEffects) {
			entry := &stepEffects[row-1]
			title, body := buildEffectDetail(entry)
			detailText.SetTitle(title)
			detailText.SetText(body)
		}
	})

	showObjectYAML := func(entry stateObjectEntry) {
		row := stateDetailRow
		title := fmt.Sprintf("Object %s", formatResourceTitle(entry.key, entry.gvk))
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
				stepDetailDirty = true
				renderStepDetail()
			} else {
				stateDetailDirty = true
				renderStateDetail()
			}
			focusDetail()
		}
		focusDetail()
	}

	renderStateDetail = func() {
		if !stateDetailDirty {
			return
		}
		stateDetailDirty = false
		currentDetailMode = detailStateObjects
		if selectedState < 0 || selectedState >= len(states) {
			showDetailText("Details", "no state selected", stateStatusMessage)
			currentDetailMode = detailNone
			return
		}

		state := states[selectedState]
		objects := state.State.Objects()
		cache := getCache()
		keys := make([]snapshot.CompositeKey, 0, len(objects))
		for key := range objects {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			return keys[i].String() < keys[j].String()
		})

		stateObjects = stateObjects[:0]
		for _, key := range keys {
			hash := objects[key]
			stateObjects = append(stateObjects, stateObjectEntry{
				key:   key,
				hash:  hash,
				cache: cache,
				gvk:   resolveGVK(cache, hash, key),
			})
		}

		detailTable.Clear()
		headers := []string{"Idx", "GVK", "Namespace", "Name", "Hash"}
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
				detailTable.SetCell(idx+1, 1, valueCell(entry.gvk))
				detailTable.SetCell(idx+1, 2, valueCell(key.ResourceKey.Namespace))
				detailTable.SetCell(idx+1, 3, valueCell(key.ResourceKey.Name))
				detailTable.SetCell(idx+1, 4, valueCell(util.ShortenHash(entry.hash.Value)))
			}
		}

		title := fmt.Sprintf("Objects • State %d", selectedState)
		if state.Error != nil {
			title = fmt.Sprintf("%s – %s", title, truncateString(state.Error.Error(), 64))
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
		if !pathDetailDirty {
			return
		}
		pathDetailDirty = false
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

	renderPendingReconciles := func(step *tracecheck.ReconcileResult, emptyTitle string) {
		stepPendingReconciles = stepPendingReconciles[:0]
		if step != nil {
			stepPendingReconciles = step.PendingReconciles
		}

		pendingReconcilesTable.Clear()
		if len(stepPendingReconciles) == 0 {
			pendingReconcilesTable.SetTitle(emptyTitle)
			pendingReconcilesTable.SetCell(0, 0, valueCell("(no pending reconciles)").SetSelectable(false).SetAlign(tview.AlignCenter))
			pendingReconcilesTable.SetSelectedFunc(nil)
			pendingDetailRow = 0
		} else {
			headers := []string{"Idx", "Reconciler", "Namespace", "Name", "Source"}
			for col, val := range headers {
				pendingReconcilesTable.SetCell(0, col, headerCell(val))
			}
			if pendingDetailRow <= 0 || pendingDetailRow > len(stepPendingReconciles) {
				pendingDetailRow = 1
			}
			for idx, pr := range stepPendingReconciles {
				pendingReconcilesTable.SetCell(idx+1, 0, valueCell(fmt.Sprintf("%d", idx)))
				pendingReconcilesTable.SetCell(idx+1, 1, valueCell(string(pr.ReconcilerID)))
				pendingReconcilesTable.SetCell(idx+1, 2, valueCell(pr.Request.Namespace))
				pendingReconcilesTable.SetCell(idx+1, 3, valueCell(pr.Request.Name))
				pendingReconcilesTable.SetCell(idx+1, 4, valueCell(string(pr.Source)))
			}
			pendingReconcilesTable.Select(pendingDetailRow, 0)
			pendingReconcilesTable.SetTitle(fmt.Sprintf("Resulting Pending Reconciles • Step %d (%d)", selectedStep, len(stepPendingReconciles)))
			pendingReconcilesTable.SetSelectedFunc(nil)
		}
		detailContainer.AddItem(pendingReconcilesTable, 0, 2, false)
	}

	renderStepDetail = func() {
		if !stepDetailDirty && lastStepState == selectedState && lastStepPath == selectedPath && lastStepIdx == selectedStep {
			updateStatus(stepStatusMessage)
			return
		}
		stepDetailDirty = false
		lastStepState = selectedState
		lastStepPath = selectedPath
		lastStepIdx = selectedStep
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
		var stateKeys []snapshot.CompositeKey
		stepCache := getStepCache(step)
		if step != nil && step.StateAfter != nil {
			stateMap = step.StateAfter
			if stepCache != nil && len(stepCache.stateKeys) > 0 {
				stateKeys = stepCache.stateKeys
			} else if len(stateMap) > 0 {
				keys := make([]snapshot.CompositeKey, 0, len(stateMap))
				for key := range stateMap {
					keys = append(keys, key)
				}
				sort.Slice(keys, func(i, j int) bool {
					return keys[i].String() < keys[j].String()
				})
				if stepCache != nil {
					stepCache.stateKeys = keys
				}
				stateKeys = keys
			}
		} else {
			stateMap = state.State.Objects()
			if len(stateMap) > 0 {
				stateKeys = make([]snapshot.CompositeKey, 0, len(stateMap))
				for key := range stateMap {
					stateKeys = append(stateKeys, key)
				}
				sort.Slice(stateKeys, func(i, j int) bool {
					return stateKeys[i].String() < stateKeys[j].String()
				})
			}
		}
		if len(stateKeys) > 0 {
			resolverCache := getCache()
			for _, key := range stateKeys {
				hash := stateMap[key]
				stateObjects = append(stateObjects, stateObjectEntry{
					key:   key,
					hash:  hash,
					cache: resolverCache,
					gvk:   resolveGVK(resolverCache, hash, key),
				})
			}
		}

		detailTable.Clear()
		headers := []string{"Idx", "GVK", "Namespace", "Name", "Hash"}
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
				detailTable.SetCell(idx+1, 1, valueCell(entry.gvk))
				detailTable.SetCell(idx+1, 2, valueCell(key.ResourceKey.Namespace))
				detailTable.SetCell(idx+1, 3, valueCell(key.ResourceKey.Name))
				detailTable.SetCell(idx+1, 4, valueCell(util.ShortenHash(entry.hash.Value)))
			}
		}

		controller := "(nil)"
		frame := "-"
		if step != nil {
			controller = string(step.ControllerID)
			frame = util.Shorter(step.FrameID)
		}
		detailTable.SetTitle(fmt.Sprintf("Resulting State • Step %d (%s @ %s)", selectedStep, controller, frame))
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
		resolverCache := getCache()
		if step != nil {
			for idx, eff := range step.Changes.Effects {
				gvk := resolveGVK(resolverCache, eff.Version, eff.Key)
				entry := effectEntry{
					effect:   eff,
					cache:    resolverCache,
					cacheRef: stepCache,
					cacheIdx: idx,
					gvk:      gvk,
				}
				if stepCache != nil && idx < len(stepCache.effectDiffs) && stepCache.effectDiffsCached[idx] {
					entry.diff = stepCache.effectDiffs[idx]
				}
				if val, ok := step.Deltas[eff.Key]; ok {
					entry.delta = string(val)
				}
				stepEffects = append(stepEffects, entry)
			}
		}

		effectsTable.Clear()
		if len(stepEffects) == 0 {
			effectsTable.SetTitle("Effects • (none)")
			effectsTable.SetCell(0, 0, valueCell("(no effects)").SetSelectable(false).SetAlign(tview.AlignCenter))
			effectsTable.SetSelectedFunc(nil)
			stepDetailRow = 0
		} else {
			headers := []string{"Idx", "Verb", "GVK", "Namespace", "Name"}
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
				effectsTable.SetCell(idx+1, 2, valueCell(entry.gvk))
				effectsTable.SetCell(idx+1, 3, valueCell(key.ResourceKey.Namespace))
				effectsTable.SetCell(idx+1, 4, valueCell(key.ResourceKey.Name))
			}
			effectsTable.Select(stepDetailRow, 0)
			effectsTable.SetTitle(fmt.Sprintf("Effects • Step %d (%s @ %s)", selectedStep, controller, frame))
			effectsTable.SetSelectedFunc(nil)
		}
		detailContainer.AddItem(effectsTable, 0, 2, false)

		// Populate pending reconciles panel
		renderPendingReconciles(step, "Pending Reconciles • (none)")
		currentDetailMode = detailStateObjects
		reconcileDirty = true
		updateStatus(stepStatusMessage)
	}

	renderReconcileDetail = func() {
		if !reconcileDirty {
			return
		}
		reconcileDirty = false
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
			controller = string(step.ControllerID)
			frame = util.Shorter(step.FrameID)
		}

		// Populate effects
		stepEffects = stepEffects[:0]
		resolverCache := getCache()
		if step != nil {
			for idx, eff := range step.Changes.Effects {
				gvk := resolveGVK(resolverCache, eff.Version, eff.Key)
				entry := effectEntry{
					effect:   eff,
					cache:    resolverCache,
					cacheRef: nil, // stepCache not available in this scope
					cacheIdx: idx,
					gvk:      gvk,
				}
				if val, ok := step.Deltas[eff.Key]; ok {
					entry.delta = string(val)
				}
				stepEffects = append(stepEffects, entry)
			}
		}

		effectsTable.Clear()
		if len(stepEffects) == 0 {
			effectsTable.SetTitle(fmt.Sprintf("Effects • Step %d (%s @ %s)", selectedStep, controller, frame))
			effectsTable.SetCell(0, 0, valueCell("(no effects)").SetSelectable(false).SetAlign(tview.AlignCenter))
			detailText.SetTitle("Effect Detail")
			detailText.SetText("(no effects to display)")
		} else {
			headers := []string{"Idx", "Verb", "GVK", "Namespace", "Name"}
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
				effectsTable.SetCell(idx+1, 2, valueCell(entry.gvk))
				effectsTable.SetCell(idx+1, 3, valueCell(key.ResourceKey.Namespace))
				effectsTable.SetCell(idx+1, 4, valueCell(key.ResourceKey.Name))
			}
			effectsTable.SetTitle(fmt.Sprintf("Effects • Step %d (%s @ %s)", selectedStep, controller, frame))
			effectsTable.SetSelectedFunc(func(row, _ int) {
				if row <= 0 || row-1 >= len(stepEffects) {
					return
				}
				stepDetailRow = row
				entry := &stepEffects[row-1]
				title, body := buildEffectDetail(entry)
				detailText.SetTitle(title)
				detailText.SetText(body)
			})
			effectsTable.Select(stepDetailRow, 0)
			if stepDetailRow > 0 && stepDetailRow <= len(stepEffects) {
				entry := &stepEffects[stepDetailRow-1]
				title, body := buildEffectDetail(entry)
				detailText.SetTitle(title)
				detailText.SetText(body)
			}
		}
		detailContainer.AddItem(effectsTable, 0, 2, false)

		// Populate pending reconciles
		renderPendingReconciles(step, fmt.Sprintf("Pending Reconciles • Step %d (none)", selectedStep))

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
				entry := &stepEffects[row-1]
				title, body := buildEffectDetail(entry)
				detailText.SetTitle(title)
				detailText.SetText(body)
				currentDetailPrim = detailText
				app.SetFocus(detailText)
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
					entry := &stepEffects[row-1]
					title, body := buildEffectDetail(entry)
					detailText.SetTitle(title)
					detailText.SetText(body)
					currentDetailPrim = detailText
					app.SetFocus(detailText)
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
		case modeStates, modePaths:
			if layoutMode != "vertical" {
				contentFlex.Clear()
				contentFlex.SetDirection(tview.FlexRow)
				detailContainer.Clear()
				detailContainer.SetDirection(tview.FlexRow)
				contentFlex.AddItem(mainTable, 0, 5, true)
				contentFlex.AddItem(detailContainer, 0, 3, false)
				layoutMode = "vertical"
			}
			currentDetailPrim = detailTable
		case modeSteps:
			if layoutMode != "steps" {
				contentFlex.Clear()
				contentFlex.SetDirection(tview.FlexColumn)
				detailContainer.Clear()
				detailContainer.SetDirection(tview.FlexRow)
				contentFlex.AddItem(mainTable, 0, 2, true)
				contentFlex.AddItem(detailContainer, 0, 3, false)
				layoutMode = "steps"
			}
			currentDetailPrim = detailTable
		case modeReconcile:
			if layoutMode != "reconcile" {
				contentFlex.Clear()
				contentFlex.SetDirection(tview.FlexColumn)
				detailContainer.Clear()
				contentFlex.AddItem(effectsTable, 0, 2, true)
				contentFlex.AddItem(detailText, 0, 3, false)
				layoutMode = "reconcile"
			}
			currentDetailPrim = effectsTable
		}

		switch mode {
		case modeStates:
			stateDetailDirty = true
		case modePaths:
			pathDetailDirty = true
		case modeSteps:
			stepDetailDirty = true
			reconcileDirty = true
		case modeReconcile:
			reconcileDirty = true
		}

		switch mode {
		case modeStates:
			mainTable.SetTitle("States")
			mainTable.SetSelectionChangedFunc(nil)
			mainTable.SetSelectedFunc(nil)
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
			if stateSelectionChanged != nil {
				mainTable.SetSelectionChangedFunc(stateSelectionChanged)
			}
			if stateEnter != nil {
				mainTable.SetSelectedFunc(stateEnter)
			}
			mainTable.Select(row, 0)
			renderStateDetail()
		case modePaths:
			mainTable.SetTitle(fmt.Sprintf("Paths • State %d", selectedState))
			mainTable.SetSelectionChangedFunc(nil)
			mainTable.SetSelectedFunc(nil)
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
			if pathSelectionChanged != nil {
				mainTable.SetSelectionChangedFunc(pathSelectionChanged)
			}
			if pathEnter != nil {
				mainTable.SetSelectedFunc(pathEnter)
			}
			mainTable.Select(row, 0)
			renderPathDetail()
		case modeSteps:
			mainTable.SetTitle(fmt.Sprintf("Steps • State %d • Path %d", selectedState, selectedPath))
			mainTable.SetSelectionChangedFunc(nil)
			mainTable.SetSelectedFunc(nil)
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
			if stepSelectionChanged != nil {
				mainTable.SetSelectionChangedFunc(stepSelectionChanged)
			}
			if stepEnter != nil {
				mainTable.SetSelectedFunc(stepEnter)
			}
			mainTable.Select(row, 0)
			renderStepDetail()
		case modeReconcile:
			renderReconcileDetail()
		}

		setFocusForMode()
	}

	setFocusForMode = func() {
		switch mode {
		case modeReconcile:
			app.SetFocus(currentDetailPrim)
		default:
			app.SetFocus(mainTable)
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
		stateDetailDirty = true
		pathDetailDirty = true
		stepDetailDirty = true
		reconcileDirty = true
		lastStepState, lastStepPath, lastStepIdx = -1, -1, -1
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
		stateDetailDirty = true
		pathDetailDirty = true
		stepDetailDirty = true
		reconcileDirty = true
		lastStepState, lastStepPath, lastStepIdx = -1, -1, -1
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
		pathDetailDirty = true
		stepDetailDirty = true
		reconcileDirty = true
		lastStepState, lastStepPath, lastStepIdx = -1, -1, -1
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
		pathDetailDirty = true
		stepDetailDirty = true
		reconcileDirty = true
		lastStepState, lastStepPath, lastStepIdx = -1, -1, -1
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
		stepDetailDirty = true
		reconcileDirty = true
		lastStepState, lastStepPath, lastStepIdx = -1, -1, -1
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
		stepDetailDirty = true
		reconcileDirty = true
		lastStepState, lastStepPath, lastStepIdx = -1, -1, -1
		if mode == modeSteps {
			renderStepDetail()
			if len(stepEffects) > 0 {
				applyMode(modeReconcile)
			}
		}
	}

	applyMode(modeStates)

	err := app.SetRoot(pages, true).EnableMouse(true).Run()
	return restartRequest, err
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

func pathHistoryHash(path tracecheck.ExecutionHistory) string {
	return util.ShortenHash(path.UniqueKey())
}

func countDistinctPathHashes(paths []tracecheck.ExecutionHistory) int {
	if len(paths) == 0 {
		return 0
	}
	uniq := lo.UniqBy(paths, func(path tracecheck.ExecutionHistory) string {
		return path.UniqueKey()
	})
	return len(uniq)
}

func populateStates(table *tview.Table, states []tracecheck.ResultState) {
	table.Clear()
	headers := []string{"Idx", "Hash", "Objects", "Paths", "Hashes", "Pending", "Status"}
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
		table.SetCell(row+1, 4, tview.NewTableCell(fmt.Sprintf("%d", countDistinctPathHashes(state.Paths))))
		table.SetCell(row+1, 5, tview.NewTableCell(fmt.Sprintf("%d", len(state.State.PendingReconciles))))
		status := "converged"
		if state.Error != nil {
			status = truncateString(state.Error.Error(), 48)
		}
		table.SetCell(row+1, 6, tview.NewTableCell(status))
	}
}

func populatePaths(table *tview.Table, states []tracecheck.ResultState, stateIdx int) {
	table.Clear()
	headers := []string{"Idx", "Steps", "Hash", "Summary"}
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
		table.SetCell(row+1, 2, tview.NewTableCell(pathHistoryHash(path)))
		table.SetCell(row+1, 3, tview.NewTableCell(summarizePath(path)))
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
			controller = string(step.ControllerID)
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
		suffix := ""
		if step.Error != "" {
			suffix = "!"
		}
		parts[i] = fmt.Sprintf("%s[%d]%s", step.ControllerID, len(step.Changes.ObjectVersions), suffix)
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

	if len(state.State.PendingReconciles) > 0 {
		b.WriteString("\nPending Reconciles:\n")
		for idx, pr := range state.State.PendingReconciles {
			req := pr.Request.NamespacedName
			fmt.Fprintf(&b, "  [%d] %s %s/%s\n", idx, pr.ReconcilerID, req.Namespace, req.Name)
		}
	}

	b.WriteString("\nOutcome:\n")
	if len(state.State.PendingReconciles) == 0 && state.Error == nil {
		b.WriteString("  Converged\n")
	} else {
		b.WriteString("  Aborted\n")
		if state.Error != nil {
			fmt.Fprintf(&b, "  Error: %s\n", state.Error.Error())
		}
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
	if step.Error != "" {
		fmt.Fprintf(&b, "Error: %s\n", step.Error)
	}

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

func truncateString(s string, max int) string {
	if max <= 0 {
		return ""
	}
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	if max <= 3 {
		return string(runes[:max])
	}
	return string(runes[:max-3]) + "..."
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

func formatResourceTitle(key snapshot.CompositeKey, gvk string) string {
	namespace := key.ResourceKey.Namespace
	if namespace != "" {
		return fmt.Sprintf("%s %s/%s", gvk, namespace, key.ResourceKey.Name)
	}
	return fmt.Sprintf("%s %s", gvk, key.ResourceKey.Name)
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
