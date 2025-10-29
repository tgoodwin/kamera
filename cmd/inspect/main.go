package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/tgoodwin/kamera/pkg/interactive"
)

func main() {
	var dumpPath string
	flag.StringVar(&dumpPath, "dump", "", "Path to an inspector dump file to load")
	flag.Parse()

	if dumpPath == "" {
		fmt.Fprintln(os.Stderr, "usage: inspect --dump <path>")
		os.Exit(1)
	}

	states, err := interactive.LoadInspectorDump(dumpPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load dump: %v\n", err)
		os.Exit(1)
	}

	if err := interactive.RunStateInspectorTUIView(states, false); err != nil {
		fmt.Fprintf(os.Stderr, "run inspector: %v\n", err)
		os.Exit(1)
	}
}
