package interactive

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/tgoodwin/kamera/pkg/analysis"
)

var errNotInspectorDump = errors.New("not an inspector dump")

// DumpCatalogEntry captures summary metadata for one exploration dump file.
type DumpCatalogEntry struct {
	Path               string
	File               string
	Scenario           string
	ScenarioLabel      string
	RunIndex           int
	ScenarioWorkflow   string
	ScenarioInputRef   string
	ScenarioAttributes map[string]string
	ModifiedAt         time.Time
	SizeBytes          int64
	States             int
	ConvergedStates    int
	AbortedStates      int
	Paths              int
	Steps              int
	Controllers        []string
	InitialController  string
	InitialObjects     int
}

// LoadDumpCatalogEntries discovers inspector dump files in dir and returns sorted summaries.
func LoadDumpCatalogEntries(dir string) ([]DumpCatalogEntry, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read dump directory: %w", err)
	}

	out := make([]DumpCatalogEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if ext := strings.ToLower(filepath.Ext(name)); ext != ".jsonl" && ext != ".json" {
			continue
		}

		path := filepath.Join(dir, name)
		dump, err := loadInspectorDumpForCatalog(path)
		if err != nil {
			if errors.Is(err, errNotInspectorDump) {
				continue
			}
			continue
		}

		info, err := entry.Info()
		if err != nil {
			return nil, fmt.Errorf("stat dump file %s: %w", path, err)
		}

		out = append(out, summarizeDumpCatalogEntry(path, name, info, dump))
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("no inspector dumps found in %s", dir)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Scenario != out[j].Scenario {
			return out[i].Scenario < out[j].Scenario
		}
		if out[i].RunIndex != out[j].RunIndex {
			return out[i].RunIndex < out[j].RunIndex
		}
		return out[i].File < out[j].File
	})
	return out, nil
}

// RenderDumpCatalogTable renders catalog summaries for headless CLI output.
func RenderDumpCatalogTable(entries []DumpCatalogEntry) string {
	var b strings.Builder
	tw := tabwriter.NewWriter(&b, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "file\tscenario\trun\tstates(c/a)\tpaths\tsteps\tworkflow\tinput-objects")
	for _, entry := range entries {
		run := "-"
		if entry.RunIndex >= 0 {
			run = strconv.Itoa(entry.RunIndex)
		}
		workflow := entry.InitialController
		if entry.ScenarioWorkflow != "" {
			workflow = entry.ScenarioWorkflow
		}
		if workflow == "" {
			workflow = "-"
		}
		fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%d(%d/%d)\t%d\t%d\t%s\t%d\n",
			entry.File,
			entry.ScenarioLabel,
			run,
			entry.States,
			entry.ConvergedStates,
			entry.AbortedStates,
			entry.Paths,
			entry.Steps,
			workflow,
			entry.InitialObjects,
		)
	}
	_ = tw.Flush()
	return b.String()
}

func loadInspectorDumpForCatalog(path string) (*analysis.Dump, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read dump file: %w", err)
	}

	var probe struct {
		States json.RawMessage `json:"states"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return nil, errNotInspectorDump
	}
	if probe.States == nil {
		return nil, errNotInspectorDump
	}

	var dump analysis.Dump
	if err := json.Unmarshal(data, &dump); err != nil {
		return nil, fmt.Errorf("unmarshal dump: %w", err)
	}
	return &dump, nil
}

func summarizeDumpCatalogEntry(path, file string, info os.FileInfo, dump *analysis.Dump) DumpCatalogEntry {
	scenario, runIdx := inferScenarioFromFileName(file)
	contextWorkflow := ""
	contextInputRef := ""
	contextAttributes := map[string]string(nil)
	if dump.Context != nil && dump.Context.Scenario != nil {
		scenarioCtx := dump.Context.Scenario
		if strings.TrimSpace(scenarioCtx.Name) != "" {
			scenario = strings.TrimSpace(scenarioCtx.Name)
		}
		if scenarioCtx.RunIndex != nil {
			runIdx = *scenarioCtx.RunIndex
		}
		contextWorkflow = strings.TrimSpace(scenarioCtx.Workflow)
		contextInputRef = strings.TrimSpace(scenarioCtx.InputRef)
		if len(scenarioCtx.Attributes) > 0 {
			contextAttributes = make(map[string]string, len(scenarioCtx.Attributes))
			for key, value := range scenarioCtx.Attributes {
				contextAttributes[key] = value
			}
		}
	}

	stateCount := len(dump.States)
	convergedCount := 0
	abortedCount := 0
	pathCount := 0
	stepCount := 0
	initialController := ""
	initialObjects := 0
	controllerSet := make(map[string]struct{})

	for _, state := range dump.States {
		if state.Error == "" {
			convergedCount++
		} else {
			abortedCount++
		}
		pathCount += len(state.Paths)
		for _, path := range state.Paths {
			stepCount += len(path)
			for idx, step := range path {
				if strings.TrimSpace(step.ControllerID) != "" {
					controllerSet[step.ControllerID] = struct{}{}
				}
				if initialController == "" && idx == 0 && strings.TrimSpace(step.ControllerID) != "" {
					initialController = step.ControllerID
				}
				if initialObjects == 0 && idx == 0 && len(step.StateBefore) > 0 {
					initialObjects = len(step.StateBefore)
				}
			}
		}
	}

	if initialObjects == 0 && len(dump.States) > 0 {
		initialObjects = len(dump.States[0].State.Contents.Objects)
	}

	controllers := make([]string, 0, len(controllerSet))
	for controller := range controllerSet {
		controllers = append(controllers, controller)
	}
	sort.Strings(controllers)

	label := strings.ReplaceAll(scenario, "_", " ")
	if label == "" {
		label = file
	}

	return DumpCatalogEntry{
		Path:               path,
		File:               file,
		Scenario:           scenario,
		ScenarioLabel:      label,
		RunIndex:           runIdx,
		ScenarioWorkflow:   contextWorkflow,
		ScenarioInputRef:   contextInputRef,
		ScenarioAttributes: contextAttributes,
		ModifiedAt:         info.ModTime(),
		SizeBytes:          info.Size(),
		States:             stateCount,
		ConvergedStates:    convergedCount,
		AbortedStates:      abortedCount,
		Paths:              pathCount,
		Steps:              stepCount,
		Controllers:        controllers,
		InitialController:  initialController,
		InitialObjects:     initialObjects,
	}
}

func inferScenarioFromFileName(fileName string) (string, int) {
	base := strings.TrimSuffix(fileName, filepath.Ext(fileName))
	if base == "" {
		return "", -1
	}

	runIdx := -1
	scenario := base
	if split := strings.LastIndex(base, "_"); split > 0 {
		suffix := base[split+1:]
		if parsed, err := strconv.Atoi(suffix); err == nil {
			runIdx = parsed
			scenario = strings.TrimSpace(base[:split])
		}
	}
	return scenario, runIdx
}
