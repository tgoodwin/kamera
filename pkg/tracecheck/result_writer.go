package tracecheck

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"

	"github.com/tgoodwin/sleeve/pkg/event"
)

func prettyPrintJSON(jsonStr string) (string, error) {
	var prettyJSON bytes.Buffer
	err := json.Indent(&prettyJSON, []byte(jsonStr), "", "  ")
	if err != nil {
		return "", err
	}
	return prettyJSON.String(), nil
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

func (tc *TraceChecker) MaterializeResults(result *Result, outDir string) {
	sanitizedOutDir := sanitizePath(outDir)
	if err := os.MkdirAll(outDir, os.ModePerm); err != nil {
		log.Fatalf("failed to create output directory: %v", err)
	}
	for i, convergedState := range result.ConvergedStates {
		outFile := fmt.Sprintf("%s/state-%d-summary.md", sanitizedOutDir, i)
		tracePrefix := fmt.Sprintf("%s/state-%d", sanitizedOutDir, i)
		tc.writeStateSummary(convergedState, outFile)
		tc.materializeTraces(convergedState, tracePrefix)
	}
}

func (tc *TraceChecker) writeStateSummary(state ConvergedState, outPath string) {
	file, err := os.Create(outPath)
	if err != nil {
		log.Fatalf("failed to create state summary file: %v", err)
	}
	defer file.Close()

	// Write state summary to the file
	file.WriteString("# State Summary:\n")
	for key, version := range state.State.ObjectVersions {
		prettyVersion, err := prettyPrintJSON(string(version))
		if err != nil {
			log.Fatalf("failed to pretty print version: %v", err)
		}
		file.WriteString(fmt.Sprintf("Key: %s\n", key))
		if _, err := file.WriteString(fmt.Sprintf("```\n%s\n```\n", prettyVersion)); err != nil {
			log.Fatalf("failed to write state summary: %v", err)
		}
	}
	// if _, err := file.WriteString(fmt.Sprintf("%#v\n\n", state.State.ObjectVersions)); err != nil {
	// 	log.Fatalf("failed to write state summary: %v", err)
	// }
	file.WriteString("\n## Paths:\n")
	for i, path := range state.Paths {
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

func (tc *TraceChecker) materializeTraces(state ConvergedState, outPrefix string) {
	if emitter, ok := tc.emitter.(*event.InMemoryEmitter); ok {
		for i, path := range state.Paths {
			paddedIdx := fmt.Sprintf("%02d", i+1)
			outPath := fmt.Sprintf("%s-path-%s.trace", outPrefix, paddedIdx)
			file, err := os.Create(outPath)
			if err != nil {
				log.Fatalf("failed to create trace file: %v", err)
			}
			defer file.Close()

			for _, reconcileResult := range path {
				logs := emitter.Dump(reconcileResult.FrameID)
				for _, line := range logs {
					if _, err := file.WriteString(line + "\n"); err != nil {
						log.Fatalf("failed to write trace: %v", err)
					}
				}
			}
		}
	} else {
		log.Fatalf("emitter is not an InMemoryEmitter")
	}
}
