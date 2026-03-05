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
	"github.com/tgoodwin/kamera/pkg/tracecheck"
)

var errNotInspectorDump = errors.New("not an inspector dump")

// DumpCatalogEntry captures summary metadata for one exploration dump file.
type DumpCatalogEntry struct {
	Path                 string
	AggregateMemberPaths []string
	File                 string
	Scenario             string
	ScenarioLabel        string
	RunIndex             int
	ScenarioWorkflow     string
	ScenarioInputRef     string
	ScenarioAttributes   map[string]string
	ModifiedAt           time.Time
	SizeBytes            int64
	States               int
	ConvergedStates      int
	AbortedStates        int
	Paths                int
	Steps                int
	Controllers          []string
	InitialController    string
	InitialObjects       int
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
	out, err = collapseMonteCarloEntries(out)
	if err != nil {
		return nil, err
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

func collapseMonteCarloEntries(entries []DumpCatalogEntry) ([]DumpCatalogEntry, error) {
	if len(entries) == 0 {
		return entries, nil
	}

	type groupInfo struct {
		trials     []int
		aggregates []int
	}
	groups := make(map[string]*groupInfo)
	for idx, entry := range entries {
		group, role, ok := monteCarloCatalogMetadata(entry)
		if !ok {
			continue
		}
		if _, exists := groups[group]; !exists {
			groups[group] = &groupInfo{}
		}
		if role == "aggregate" {
			groups[group].aggregates = append(groups[group].aggregates, idx)
			continue
		}
		groups[group].trials = append(groups[group].trials, idx)
	}

	keep := make([]bool, len(entries))
	for i := range keep {
		keep[i] = true
	}
	virtualAggs := make([]DumpCatalogEntry, 0)
	for groupID, info := range groups {
		if len(info.aggregates) > 0 {
			bestAggIdx := info.aggregates[0]
			for _, aggIdx := range info.aggregates[1:] {
				if entries[aggIdx].ModifiedAt.After(entries[bestAggIdx].ModifiedAt) {
					bestAggIdx = aggIdx
				}
			}
			for _, trialIdx := range info.trials {
				keep[trialIdx] = false
			}
			for _, aggIdx := range info.aggregates {
				if aggIdx != bestAggIdx {
					keep[aggIdx] = false
				}
			}
			continue
		}
		if len(info.trials) <= 1 {
			continue
		}

		memberPaths := make([]string, 0, len(info.trials))
		newest := time.Time{}
		totalSize := int64(0)
		for _, trialIdx := range info.trials {
			entry := entries[trialIdx]
			memberPaths = append(memberPaths, entry.Path)
			if entry.ModifiedAt.After(newest) {
				newest = entry.ModifiedAt
			}
			totalSize += entry.SizeBytes
			keep[trialIdx] = false
		}
		aggDump, err := aggregateMonteCarloDumpFiles(memberPaths)
		if err != nil {
			return nil, fmt.Errorf("aggregate monte-carlo group %q: %w", groupID, err)
		}

		base := entries[info.trials[0]]
		base.Path = memberPaths[0]
		base.AggregateMemberPaths = memberPaths
		base.File = fmt.Sprintf("mc-aggregate:%s", groupID)
		base.RunIndex = -1
		base.ModifiedAt = newest
		base.SizeBytes = totalSize
		if base.ScenarioAttributes == nil {
			base.ScenarioAttributes = map[string]string{}
		}
		base.ScenarioAttributes = cloneStringMap(base.ScenarioAttributes)
		base.ScenarioAttributes["search_mode"] = "monte_carlo"
		base.ScenarioAttributes["mc_group_id"] = groupID
		base.ScenarioAttributes["mc_role"] = "aggregate"
		base.ScenarioAttributes["mc_trials_aggregated"] = strconv.Itoa(len(memberPaths))

		applyDumpMetricsToCatalogEntry(&base, aggDump)
		virtualAggs = append(virtualAggs, base)
	}

	filtered := make([]DumpCatalogEntry, 0, len(entries))
	for i, entry := range entries {
		if keep[i] {
			filtered = append(filtered, entry)
		}
	}
	filtered = append(filtered, virtualAggs...)
	return filtered, nil
}

func monteCarloCatalogMetadata(entry DumpCatalogEntry) (group string, role string, ok bool) {
	if len(entry.ScenarioAttributes) == 0 {
		return "", "", false
	}
	if strings.TrimSpace(entry.ScenarioAttributes["search_mode"]) != "monte_carlo" {
		return "", "", false
	}
	group = strings.TrimSpace(entry.ScenarioAttributes["mc_group_id"])
	if group == "" {
		return "", "", false
	}
	role = strings.TrimSpace(entry.ScenarioAttributes["mc_role"])
	if role == "" {
		role = "trial"
	}
	return group, role, true
}

func LoadInspectorDumpForCatalogEntry(entry DumpCatalogEntry) ([]tracecheck.ResultState, tracecheck.VersionManager, error) {
	if len(entry.AggregateMemberPaths) == 0 {
		return LoadInspectorDump(entry.Path)
	}
	dump, err := aggregateMonteCarloDumpFiles(entry.AggregateMemberPaths)
	if err != nil {
		return nil, nil, fmt.Errorf("aggregate monte-carlo entry %q: %w", entry.ScenarioLabel, err)
	}
	return dumpToResultStates(dump)
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

	label := strings.ReplaceAll(scenario, "_", " ")
	if label == "" {
		label = file
	}

	entry := DumpCatalogEntry{
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
	}
	applyDumpMetricsToCatalogEntry(&entry, dump)
	return entry
}

func applyDumpMetricsToCatalogEntry(entry *DumpCatalogEntry, dump *analysis.Dump) {
	if entry == nil || dump == nil {
		return
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

	entry.States = stateCount
	entry.ConvergedStates = convergedCount
	entry.AbortedStates = abortedCount
	entry.Paths = pathCount
	entry.Steps = stepCount
	entry.Controllers = controllers
	entry.InitialController = initialController
	entry.InitialObjects = initialObjects
}

func cloneStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
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
