package main

import (
	"fmt"
	"os"

	"github.com/tgoodwin/kamera/internal/kamera"
)

func main() {
	code, err := runInspect(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	os.Exit(code)
}

func runInspect(args []string) (int, error) {
	return kamera.RunInspect(args)
}

func dependencyGraphDOT(path string) (string, error) {
	return kamera.DependencyGraphDOT(path)
}

func openArgs(path string) []string {
	return kamera.OpenArgs(path)
}
