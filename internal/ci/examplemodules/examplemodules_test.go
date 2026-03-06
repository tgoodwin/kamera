package examplemodules

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPortableDirsSkipsModulesWithMissingAbsoluteReplaceTargets(t *testing.T) {
	root := t.TempDir()
	mustWriteGoMod(t, filepath.Join(root, "examples", "portable", "go.mod"), `module example.com/portable

go 1.24.0

replace github.com/tgoodwin/kamera => ../..
`)
	mustWriteGoMod(t, filepath.Join(root, "examples", "localonly", "go.mod"), `module example.com/localonly

go 1.24.0

replace example.com/upstream => /definitely/missing/module
`)

	got, err := PortableDirs(root)
	if err != nil {
		t.Fatalf("PortableDirs returned error: %v", err)
	}

	want := []string{
		filepath.Join(root, "examples", "portable"),
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d portable dir, got %d: %v", len(want), len(got), got)
	}
	if got[0] != want[0] {
		t.Fatalf("expected portable dir %q, got %q", want[0], got[0])
	}
}

func TestSkippedDirsReportsModulesWithMissingAbsoluteReplaceTargets(t *testing.T) {
	root := t.TempDir()
	mustWriteGoMod(t, filepath.Join(root, "examples", "portable", "go.mod"), `module example.com/portable

go 1.24.0

replace github.com/tgoodwin/kamera => ../..
`)
	mustWriteGoMod(t, filepath.Join(root, "examples", "localonly", "go.mod"), `module example.com/localonly

go 1.24.0

replace example.com/upstream => /definitely/missing/module
`)

	got, err := SkippedDirs(root)
	if err != nil {
		t.Fatalf("SkippedDirs returned error: %v", err)
	}

	want := []string{
		filepath.Join(root, "examples", "localonly"),
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d skipped dir, got %d: %v", len(want), len(got), got)
	}
	if got[0] != want[0] {
		t.Fatalf("expected skipped dir %q, got %q", want[0], got[0])
	}
}

func TestPortableDirsKeepsModulesWithExistingAbsoluteReplaceTargets(t *testing.T) {
	root := t.TempDir()
	upstream := filepath.Join(root, "deps", "upstream")
	if err := os.MkdirAll(upstream, 0o755); err != nil {
		t.Fatalf("mkdir upstream: %v", err)
	}
	mustWriteGoMod(t, filepath.Join(root, "examples", "local", "go.mod"), `module example.com/local

go 1.24.0

replace example.com/upstream => `+filepath.ToSlash(upstream)+`
`)

	got, err := PortableDirs(root)
	if err != nil {
		t.Fatalf("PortableDirs returned error: %v", err)
	}
	if len(got) != 1 || got[0] != filepath.Join(root, "examples", "local") {
		t.Fatalf("expected existing absolute replacement to stay eligible, got %v", got)
	}
}

func mustWriteGoMod(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
