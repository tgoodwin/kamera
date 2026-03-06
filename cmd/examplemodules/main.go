package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/tgoodwin/kamera/internal/ci/examplemodules"
)

func main() {
	mode := flag.String("mode", "portable", "output mode: portable or skipped")
	flag.Parse()

	root, err := os.Getwd()
	if err != nil {
		log.Fatalf("resolve working directory: %v", err)
	}

	discovery, err := examplemodules.Discover(root)
	if err != nil {
		log.Fatalf("list example modules: %v", err)
	}

	var dirs []string
	switch *mode {
	case "portable":
		dirs = discovery.Portable
	case "skipped":
		dirs = discovery.Skipped
	default:
		log.Fatalf("unsupported mode %q", *mode)
	}

	fmt.Print(strings.Join(dirs, "\n"))
}
