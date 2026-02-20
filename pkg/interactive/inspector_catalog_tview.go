package interactive

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
)

// RunDumpCatalogTUIView displays dump summaries and returns the selected dump path.
// If the user exits without a selection, an empty path is returned.
func RunDumpCatalogTUIView(entries []DumpCatalogEntry) (string, error) {
	if len(entries) == 0 {
		return "", fmt.Errorf("no dump entries available")
	}

	app := tview.NewApplication()
	table := configureTable("Exploration Dumps", true)
	table.SetSelectable(true, false)

	detail := tview.NewTextView().
		SetDynamicColors(true).
		SetWrap(true)
	detail.SetBorder(true)
	detail.SetTitle("Dump Details")

	statusBar := tview.NewTextView().
		SetDynamicColors(true).
		SetText(`[yellow]Up/Down[-] select • [yellow]Enter[-] open dump • [yellow]q/Esc[-] close`).
		SetTextAlign(tview.AlignCenter)

	main := tview.NewFlex().
		AddItem(table, 0, 2, true).
		AddItem(detail, 0, 3, false)

	root := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(main, 0, 1, true).
		AddItem(statusBar, 1, 0, false)

	headers := []string{"#", "Scenario", "Run", "States", "Steps", "Workflow", "InputObjs", "File"}
	renderCatalogTable(table, headers, entries)

	selectedPath := ""
	table.SetSelectionChangedFunc(func(row, _ int) {
		if row <= 0 || row > len(entries) {
			return
		}
		renderCatalogDetail(detail, entries[row-1])
	})
	table.SetSelectedFunc(func(row, _ int) {
		if row <= 0 || row > len(entries) {
			return
		}
		selectedPath = entries[row-1].Path
		app.Stop()
	})

	root.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyEsc:
			app.Stop()
			return nil
		}
		switch event.Rune() {
		case 'q', 'Q':
			app.Stop()
			return nil
		}
		return event
	})

	table.Select(1, 0)
	renderCatalogDetail(detail, entries[0])

	if err := app.SetRoot(root, true).SetFocus(table).Run(); err != nil {
		return "", err
	}
	return selectedPath, nil
}

// RunDirectoryInspectorTUIView launches a single TUI app that hosts both catalog and inspector pages.
// Selecting a dump switches pages in-process (no terminal flicker).
func RunDirectoryInspectorTUIView(entries []DumpCatalogEntry) error {
	if len(entries) == 0 {
		return fmt.Errorf("no dump entries available")
	}

	app := tview.NewApplication()
	pages := tview.NewPages()
	const (
		catalogPageName   = "catalog"
		inspectorPageName = "inspector"
	)

	table := configureTable("Exploration Dumps", true)
	table.SetSelectable(true, false)

	detail := tview.NewTextView().
		SetDynamicColors(true).
		SetWrap(true)
	detail.SetBorder(true)
	detail.SetTitle("Dump Details")

	statusBar := tview.NewTextView().
		SetDynamicColors(true).
		SetText(`[yellow]Up/Down[-] select • [yellow]Enter[-] open dump • [yellow]q/Esc[-] close`).
		SetTextAlign(tview.AlignCenter)

	main := tview.NewFlex().
		AddItem(table, 0, 2, true).
		AddItem(detail, 0, 3, false)

	catalogRoot := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(main, 0, 1, true).
		AddItem(statusBar, 1, 0, false)

	headers := []string{"#", "Scenario", "Run", "States", "Steps", "Workflow", "InputObjs", "File"}
	renderCatalogTable(table, headers, entries)

	showCatalog := func() {
		statusBar.SetText(`[yellow]Up/Down[-] select • [yellow]Enter[-] open dump • [yellow]q/Esc[-] close`)
		pages.SwitchToPage(catalogPageName)
		app.SetFocus(table)
	}

	openInspector := func(entry DumpCatalogEntry) {
		states, resolver, err := LoadInspectorDump(entry.Path)
		if err != nil {
			statusBar.SetText(fmt.Sprintf("[red]load dump failed: %v[-]", err))
			return
		}

		inspectorRoot, _, err := buildStateInspectorRoot(
			app,
			states,
			resolver,
			false,
			tracecheck.ExploreConfig{},
			true,
			showCatalog,
		)
		if err != nil {
			statusBar.SetText(fmt.Sprintf("[red]inspector failed: %v[-]", err))
			return
		}
		pages.RemovePage(inspectorPageName)
		pages.AddPage(inspectorPageName, inspectorRoot, true, true)
		pages.SwitchToPage(inspectorPageName)
	}

	table.SetSelectionChangedFunc(func(row, _ int) {
		if row <= 0 || row > len(entries) {
			return
		}
		renderCatalogDetail(detail, entries[row-1])
	})
	table.SetSelectedFunc(func(row, _ int) {
		if row <= 0 || row > len(entries) {
			return
		}
		openInspector(entries[row-1])
	})

	catalogRoot.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyEsc:
			app.Stop()
			return nil
		}
		switch event.Rune() {
		case 'q', 'Q':
			app.Stop()
			return nil
		}
		return event
	})

	table.Select(1, 0)
	renderCatalogDetail(detail, entries[0])
	pages.AddPage(catalogPageName, catalogRoot, true, true)

	return app.SetRoot(pages, true).EnableMouse(true).SetFocus(table).Run()
}

func renderCatalogTable(table *tview.Table, headers []string, entries []DumpCatalogEntry) {
	for col, header := range headers {
		table.SetCell(0, col, tview.NewTableCell(header).
			SetTextColor(tcell.ColorYellow).
			SetSelectable(false).
			SetExpansion(1))
	}

	for idx, entry := range entries {
		run := "-"
		if entry.RunIndex >= 0 {
			run = strconv.Itoa(entry.RunIndex)
		}
		states := fmt.Sprintf("%d(%d/%d)", entry.States, entry.ConvergedStates, entry.AbortedStates)
		workflow := entry.InitialController
		if entry.ScenarioWorkflow != "" {
			workflow = entry.ScenarioWorkflow
		}
		if workflow == "" {
			workflow = "-"
		}
		values := []string{
			strconv.Itoa(idx + 1),
			entry.ScenarioLabel,
			run,
			states,
			strconv.Itoa(entry.Steps),
			workflow,
			strconv.Itoa(entry.InitialObjects),
			entry.File,
		}
		for col, value := range values {
			table.SetCell(idx+1, col, tview.NewTableCell(value).SetExpansion(1))
		}
	}
}

func renderCatalogDetail(detail *tview.TextView, entry DumpCatalogEntry) {
	run := "-"
	if entry.RunIndex >= 0 {
		run = strconv.Itoa(entry.RunIndex)
	}
	controllers := "-"
	if len(entry.Controllers) > 0 {
		controllers = strings.Join(entry.Controllers, ", ")
	}
	workflow := entry.InitialController
	if entry.ScenarioWorkflow != "" {
		workflow = entry.ScenarioWorkflow
	}
	if workflow == "" {
		workflow = "-"
	}
	firstObservedController := entry.InitialController
	if firstObservedController == "" {
		firstObservedController = "-"
	}
	inputRef := entry.ScenarioInputRef
	if inputRef == "" {
		inputRef = "-"
	}
	attributes := "-"
	if len(entry.ScenarioAttributes) > 0 {
		parts := make([]string, 0, len(entry.ScenarioAttributes))
		for key, value := range entry.ScenarioAttributes {
			parts = append(parts, fmt.Sprintf("%s=%s", key, value))
		}
		sort.Strings(parts)
		attributes = strings.Join(parts, ", ")
	}
	detail.SetText(fmt.Sprintf(
		`[yellow]Scenario:[-] %s
[yellow]Run:[-] %s
[yellow]File:[-] %s
[yellow]Path:[-] %s
[yellow]Modified:[-] %s
[yellow]Size:[-] %d bytes

[yellow]Input summary:[-]
  Input reference: %s
  Objects at first step: %d

[yellow]Scenario context:[-]
  Workflow: %s
  Attributes: %s

[yellow]Observed workflow:[-]
  First controller seen: %s
  Controllers observed: %s

[yellow]Exploration summary:[-]
  States: %d (converged=%d, aborted=%d)
  Paths: %d
  Steps: %d`,
		entry.ScenarioLabel,
		run,
		entry.File,
		entry.Path,
		entry.ModifiedAt.Format(time.RFC3339),
		entry.SizeBytes,
		inputRef,
		entry.InitialObjects,
		workflow,
		attributes,
		firstObservedController,
		controllers,
		entry.States,
		entry.ConvergedStates,
		entry.AbortedStates,
		entry.Paths,
		entry.Steps,
	))
}
