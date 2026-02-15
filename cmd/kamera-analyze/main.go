package main

import (
	"os"

	"github.com/tgoodwin/kamera/internal/kamera"
)

func main() {
	os.Exit(kamera.RunAnalyze(os.Args[1:], os.Stdout, os.Stderr))
}
