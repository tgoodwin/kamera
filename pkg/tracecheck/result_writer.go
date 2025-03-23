package tracecheck

import (
	"fmt"
	"log"
	"os"
	"path"
	"sort"

	"github.com/samber/lo"
	"github.com/tgoodwin/sleeve/pkg/util"
)

type ResultWriter struct {
	emitter testEmitter
}

func NewResultWriter(emitter testEmitter) *ResultWriter {
	return &ResultWriter{
		emitter: emitter,
	}
}

func sanitizePath(outDir string) string {
	if path.IsAbs(outDir) {
		return outDir
	}
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("failed to get current working directory: %v", err)
	}
	return path.Join(cwd, outDir)
}

func (rw *ResultWriter) MaterializeClassified(results []ClassifiedState, outDir string) {
	sanitizedOutDir := sanitizePath(outDir)
	if err := os.MkdirAll(sanitizedOutDir, os.ModePerm); err != nil {
		log.Fatalf("failed to create output directory: %v", err)
	}

	happyDir := path.Join(sanitizedOutDir, "happy")
	badDir := path.Join(sanitizedOutDir, "bad")

	if err := os.MkdirAll(happyDir, os.ModePerm); err != nil {
		log.Fatalf("failed to create happy directory: %v", err)
	}
	if err := os.MkdirAll(badDir, os.ModePerm); err != nil {
		log.Fatalf("failed to create bad directory: %v", err)
	}

	happyTracesDir := path.Join(happyDir, "traces")
	badTracesDir := path.Join(badDir, "traces")

	if err := os.MkdirAll(happyTracesDir, os.ModePerm); err != nil {
		log.Fatalf("failed to create happy traces directory: %v", err)
	}
	if err := os.MkdirAll(badTracesDir, os.ModePerm); err != nil {
		log.Fatalf("failed to create bad traces directory: %v", err)
	}

	for _, result := range results {
		var outFile, tracePrefix string
		if result.Classification == "happy" {
			outFile = fmt.Sprintf("%s/%s-%s-summary.md", happyDir, result.ID, result.Signature)
			tracePrefix = fmt.Sprintf("%s/%s", happyTracesDir, result.ID)
		} else if result.Classification == "bad" {
			outFile = fmt.Sprintf("%s/%s-%s-summary.md", badDir, result.ID, result.Signature)
			tracePrefix = fmt.Sprintf("%s/%s", badTracesDir, result.ID)
		} else {
			log.Fatalf("unknown classification: %s", result.Classification)
		}
		rw.writeStateSummary(result.State, outFile)
		rw.materializeTraces(result.State, tracePrefix)
	}
}

func (rw *ResultWriter) MaterializeResults(result *Result, outDir string) {
	sanitizedOutDir := sanitizePath(outDir)
	if err := os.MkdirAll(sanitizedOutDir, os.ModePerm); err != nil {
		log.Fatalf("failed to create output directory: %v", err)
	}

	tracesDir := path.Join(sanitizedOutDir, "traces")
	if err := os.MkdirAll(tracesDir, os.ModePerm); err != nil {
		log.Fatalf("failed to create traces directory: %v", err)
	}

	for i, convergedState := range result.ConvergedStates {
		outFile := fmt.Sprintf("%s/state-%d-summary.md", sanitizedOutDir, i)
		tracePrefix := fmt.Sprintf("%s/state-%d", tracesDir, i)
		rw.writeStateSummary(convergedState, outFile)
		rw.materializeTraces(convergedState, tracePrefix)
	}
}

func (rw *ResultWriter) writeStateSummary(state ConvergedState, outPath string) {
	file, err := os.Create(outPath)
	if err != nil {
		log.Fatalf("failed to create state summary file: %v", err)
	}
	defer file.Close()

	// Write state summary to the file
	file.WriteString("# Converged State Summary:\n")
	file.WriteString(fmt.Sprintf("Divergence point: %s\n", state.State.DivergencePoint))
	file.WriteString("state keys:\n")
	keys := lo.Keys(state.State.Objects())
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].ObjectID < keys[j].ObjectID
	})
	for _, k := range keys {
		file.WriteString(fmt.Sprintf("\t%s\n", k))
	}
	file.WriteString("\n")
	file.WriteString("## Converged Objects:\n")
	objectKeys := lo.Keys(state.State.Objects())
	sort.Slice(objectKeys, func(i, j int) bool {
		// first sort by kind, then by objectID
		if objectKeys[i].IdentityKey.Kind != objectKeys[j].IdentityKey.Kind {
			return objectKeys[i].IdentityKey.Kind < objectKeys[j].IdentityKey.Kind
		}
		return objectKeys[i].ObjectID < objectKeys[j].ObjectID
	})

	for _, key := range objectKeys {
		version := state.State.Objects()[key]
		prettyVersion, err := util.PrettyPrintJSON(string(version.Value))
		if err != nil {
			log.Fatalf("failed to pretty print version: %v", err)
		}
		file.WriteString(fmt.Sprintf("Key: %s\n", key))
		if _, err := file.WriteString(fmt.Sprintf("```\n%s\n```\n", prettyVersion)); err != nil {
			log.Fatalf("failed to write state summary: %v", err)
		}
	}
	uniquePaths := GetUniquePaths(state.Paths)
	file.WriteString(fmt.Sprintf("\n## Unique Paths: %d\n", len(uniquePaths)))
	for i, path := range uniquePaths {
		if _, err := file.WriteString(fmt.Sprintf("\nPath %d:\n", i+1)); err != nil {
			log.Fatalf("failed to write state summary: %v", err)
		}
		file.WriteString("```\n")
		if err := path.SummarizeToFile(file); err != nil {
			log.Fatalf("failed to write state summary: %v", err)
		}
		file.WriteString("```\n")
	}
}

func (rw *ResultWriter) materializeTraces(state ConvergedState, outPrefix string) {
	// filter out no-ops
	uniquePaths := GetUniquePaths(state.Paths)
	for i, path := range uniquePaths {
		paddedIdx := fmt.Sprintf("%02d", i+1)
		outPath := fmt.Sprintf("%s-path-%s.trace", outPrefix, paddedIdx)
		file, err := os.Create(outPath)
		if err != nil {
			log.Fatalf("failed to create trace file: %v", err)
		}
		defer file.Close()

		for _, reconcileResult := range path {
			logs := rw.emitter.Dump(reconcileResult.FrameID)
			for _, line := range logs {
				if _, err := file.WriteString(line + "\n"); err != nil {
					log.Fatalf("failed to write trace: %v", err)
				}
			}
		}
	}
}
