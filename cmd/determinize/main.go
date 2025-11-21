package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/tools/go/ast/astutil"
	"golang.org/x/tools/imports"
)

var skipModulePrefixes = []string{
	"golang.org/toolchain@",
	"k8s.io/utils@",
}

func skipModule(path string) bool {
	normalized := filepath.ToSlash(path)
	for _, prefix := range skipModulePrefixes {
		if strings.Contains(normalized, prefix) {
			return true
		}
	}
	return false
}

const simclockImportPath = "github.com/tgoodwin/kamera/pkg/simclock"

func main() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: determinize [paths...]\n")
	}
	flag.Parse()
	targets := flag.Args()
	if len(targets) == 0 {
		targets = []string{"."}
	}

	files, err := collectGoFiles(targets)
	if err != nil {
		fmt.Fprintf(os.Stderr, "collect files: %v\n", err)
		os.Exit(1)
	}

	var failed bool
	for _, file := range files {
		if err := rewriteFile(file); err != nil {
			fmt.Fprintf(os.Stderr, "rewrite %s: %v\n", file, err)
			failed = true
		}
	}
	if failed {
		os.Exit(1)
	}
}

func collectGoFiles(paths []string) ([]string, error) {
	seen := make(map[string]struct{})
	var files []string
	for _, path := range paths {
		if strings.HasSuffix(path, "...") {
			path = strings.TrimSuffix(path, "...")
			if path == "" {
				path = "."
			}
		}
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		if skipModule(path) {
			continue
		}
		if info.IsDir() {
			err = filepath.WalkDir(path, func(p string, d os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if skipModule(p) {
					if d.IsDir() {
						return filepath.SkipDir
					}
					return nil
				}
				if d.IsDir() {
					name := d.Name()
					if name == "vendor" || name == ".git" || strings.HasPrefix(name, ".") {
						return filepath.SkipDir
					}
					// skipping testdata directories cause they contain purposefully invalid Go code
					// TODO: handle testdata directories better, e.g. by rewriting only specific files
					if name == "testdata" {
						return filepath.SkipDir
					}
					return nil
				}
				if strings.Contains(p, string(filepath.Separator)+"testdata"+string(filepath.Separator)) {
					return nil
				}
				if strings.HasSuffix(p, ".go") && !strings.HasSuffix(p, "_test.go") {
					if _, ok := seen[p]; !ok {
						seen[p] = struct{}{}
						files = append(files, p)
					}
				}
				return nil
			})
			if err != nil {
				return nil, err
			}
		} else if strings.Contains(path, string(filepath.Separator)+"testdata"+string(filepath.Separator)) {
			continue
		} else if strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") {
			if _, ok := seen[path]; !ok {
				seen[path] = struct{}{}
				files = append(files, path)
			}
		}
	}
	return files, nil
}

func rewriteFile(filename string) error {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filename, nil, parser.ParseComments)
	if err != nil {
		return err
	}

	importMap := buildImportMap(file)
	changed := replaceSelectors(fset, file, importMap)
	if !changed {
		return nil
	}

	trimUnusedImports(fset, file, importMap)

	var buf bytes.Buffer
	if err := format.Node(&buf, fset, file); err != nil {
		return err
	}
	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		return err
	}
	processed, err := imports.Process(filename, formatted, nil)
	if err != nil {
		return err
	}
	return os.WriteFile(filename, processed, 0644)
}

func buildImportMap(file *ast.File) map[string]string {
	aliasToPath := make(map[string]string)
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			continue
		}
		alias := ""
		if spec.Name != nil {
			alias = spec.Name.Name
		} else {
			alias = filepath.Base(path)
		}
		if alias == "." || alias == "_" {
			continue
		}
		aliasToPath[alias] = path
	}
	return aliasToPath
}

func replaceSelectors(fset *token.FileSet, file *ast.File, importMap map[string]string) bool {
	var changed bool
	simclockAlias := ""

	astutil.Apply(file, func(c *astutil.Cursor) bool {
		sel, ok := c.Node().(*ast.SelectorExpr)
		if !ok {
			return true
		}
		ident, ok := sel.X.(*ast.Ident)
		if !ok {
			return true
		}

		path := importMap[ident.Name]
		switch path {
		case "time":
			if sel.Sel.Name == "Now" {
				alias := ensureSimclockAlias(fset, file, importMap, &simclockAlias)
				if alias == "" {
					return true
				}
				sel.X = ast.NewIdent(alias)
				sel.Sel = ast.NewIdent("Now")
				changed = true
				importMap[alias] = simclockImportPath
			}
		case "k8s.io/apimachinery/pkg/apis/meta/v1":
			if sel.Sel.Name == "Now" {
				alias := ensureSimclockAlias(fset, file, importMap, &simclockAlias)
				if alias == "" {
					return true
				}
				metaAlias := importAlias(importMap, "k8s.io/apimachinery/pkg/apis/meta/v1")
				if metaAlias == "" {
					metaAlias = ident.Name
				}
				call := &ast.CallExpr{
					Fun: &ast.SelectorExpr{X: ast.NewIdent(metaAlias), Sel: ast.NewIdent("NewTime")},
					Args: []ast.Expr{
						&ast.CallExpr{Fun: &ast.SelectorExpr{X: ast.NewIdent(alias), Sel: ast.NewIdent("Now")}},
					},
				}
				c.Replace(call)
				changed = true
				importMap[alias] = simclockImportPath
			}
		case "k8s.io/utils/clock":
			if sel.Sel.Name == "RealClock" {
				alias := ensureSimclockAlias(fset, file, importMap, &simclockAlias)
				if alias == "" {
					return true
				}
				sel.X = ast.NewIdent(alias)
				sel.Sel = ast.NewIdent("DeterministicClock")
				changed = true
				importMap[alias] = simclockImportPath
			}
		}
		return true
	}, nil)

	return changed
}

func ensureSimclockAlias(fset *token.FileSet, file *ast.File, importMap map[string]string, cached *string) string {
	if *cached != "" {
		return *cached
	}
	for alias, path := range importMap {
		if path == simclockImportPath {
			*cached = alias
			return alias
		}
	}
	alias := "simclock"
	if _, exists := importMap[alias]; exists {
		for i := 2; ; i++ {
			candidate := fmt.Sprintf("%s%d", alias, i)
			if _, ok := importMap[candidate]; !ok {
				alias = candidate
				break
			}
		}
	}
	if alias == "simclock" {
		astutil.AddImport(fset, file, simclockImportPath)
	} else {
		astutil.AddNamedImport(fset, file, alias, simclockImportPath)
	}
	importMap[alias] = simclockImportPath
	*cached = alias
	return alias
}

func trimUnusedImports(fset *token.FileSet, file *ast.File, importMap map[string]string) {
	// Remove time/meta/clock if unused.
	candidates := []string{"time", "k8s.io/apimachinery/pkg/apis/meta/v1", "k8s.io/utils/clock"}
	for _, pkg := range candidates {
		alias := importAlias(importMap, pkg)
		if alias == "" {
			continue
		}
		if !usesIdent(file, alias) {
			astutil.DeleteImport(fset, file, pkg)
			delete(importMap, alias)
		}
	}
}

func importAlias(importMap map[string]string, path string) string {
	for alias, candidate := range importMap {
		if candidate == path {
			return alias
		}
	}
	return ""
}

func usesIdent(file *ast.File, name string) bool {
	found := false
	ast.Inspect(file, func(n ast.Node) bool {
		if ident, ok := n.(*ast.Ident); ok && ident.Name == name {
			found = true
			return false
		}
		return true
	})
	return found
}
