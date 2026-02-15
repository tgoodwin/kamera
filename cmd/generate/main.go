package main

import (
	"fmt"
	"os"

	"github.com/tgoodwin/kamera/internal/kamera"
)

func main() {
	code, err := runGenerate(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	os.Exit(code)
}

func runGenerate(args []string) (int, error) {
	return kamera.RunGenerate(args)
}
