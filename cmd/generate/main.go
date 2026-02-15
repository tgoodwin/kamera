package main

import (
	"fmt"
	"os"
)

func main() {
	code, err := runGenerate(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	os.Exit(code)
}
