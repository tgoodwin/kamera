package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "sleeve is legacy and unsupported; use `kamera`")
	os.Exit(1)
}
