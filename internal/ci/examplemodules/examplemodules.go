package examplemodules

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"golang.org/x/mod/modfile"
)

type Discovery struct {
	Portable []string
	Skipped  []string
}

// PortableDirs returns example module directories that can be tested in the
// current environment. Modules with missing absolute replace targets are
// skipped because they depend on sibling checkouts that are not present.
func PortableDirs(root string) ([]string, error) {
	discovery, err := Discover(root)
	if err != nil {
		return nil, err
	}
	return discovery.Portable, nil
}

// SkippedDirs returns example module directories that cannot be tested in the
// current environment because one or more absolute replace targets are missing.
func SkippedDirs(root string) ([]string, error) {
	discovery, err := Discover(root)
	if err != nil {
		return nil, err
	}
	return discovery.Skipped, nil
}

// Discover classifies example modules into portable and skipped buckets.
func Discover(root string) (Discovery, error) {
	goMods, err := filepath.Glob(filepath.Join(root, "examples", "*", "go.mod"))
	if err != nil {
		return Discovery{}, fmt.Errorf("glob example modules: %w", err)
	}
	sort.Strings(goMods)

	discovery := Discovery{
		Portable: make([]string, 0, len(goMods)),
		Skipped:  make([]string, 0, len(goMods)),
	}
	for _, goModPath := range goMods {
		ok, err := IsPortable(goModPath)
		if err != nil {
			return Discovery{}, err
		}
		if ok {
			discovery.Portable = append(discovery.Portable, filepath.Dir(goModPath))
			continue
		}
		discovery.Skipped = append(discovery.Skipped, filepath.Dir(goModPath))
	}

	return discovery, nil
}

// IsPortable reports whether a module's go.mod can resolve all absolute
// replace targets on the current machine.
func IsPortable(goModPath string) (bool, error) {
	data, err := os.ReadFile(goModPath)
	if err != nil {
		return false, fmt.Errorf("read %s: %w", goModPath, err)
	}

	parsed, err := modfile.Parse(goModPath, data, nil)
	if err != nil {
		return false, fmt.Errorf("parse %s: %w", goModPath, err)
	}

	for _, replace := range parsed.Replace {
		target := replace.New.Path
		if !filepath.IsAbs(target) {
			continue
		}
		if _, err := os.Stat(target); err != nil {
			if os.IsNotExist(err) {
				return false, nil
			}
			return false, fmt.Errorf("stat replace target %s for %s: %w", target, goModPath, err)
		}
	}

	return true, nil
}
