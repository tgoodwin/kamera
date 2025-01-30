package main

import (
	"flag"
	"os"
	"regexp"
	"strings"

	"github.com/samber/lo"
	"github.com/tgoodwin/sleeve/pkg/replay"
	"github.com/tgoodwin/sleeve/pkg/tag"
)

var inFile = flag.String("logfile", "default.log", "path to the log file")
var objectID = flag.String("objectID", "", "object ID to analyze")
var reconcileID = flag.String("reconcileID", "", "object ID to analyze")

var logTypes = []string{tag.ControllerOperationKey, tag.ObjectVersionKey}
var pattern = regexp.MustCompile(`{"LogType": "(?:` + strings.Join(logTypes, "|") + `)"}`)

func stripLogtype(line string) string {
	return pattern.ReplaceAllString(line, "")
}

func stripLogtypeFromLines(lines []string) []string {
	return lo.Map(lines, func(line string, _ int) string {
		return stripLogtype(line)
	})
}

func main() {
	flag.Parse()
	f, err := os.Open(*inFile)
	if err != nil {
		panic(err.Error())
	}
	defer f.Close()
	data, err := os.ReadFile(*inFile)
	if err != nil {
		panic(err.Error())
	}
	builder, err := replay.ParseTrace(data)
	if err != nil {
		panic(err.Error())
	}
	builder.Debug()
	if *reconcileID != "" {
		builder.AnalyzeReconcile(*reconcileID)
	}
	if *objectID != "" {
		builder.AnalyzeObject(*objectID)
	}
}
