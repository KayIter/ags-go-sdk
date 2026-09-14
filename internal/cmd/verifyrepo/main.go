// Command verifyrepo checks repository-local documentation and sensitive-path invariants.
package main

import (
	"bufio"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var markdownLink = regexp.MustCompile(`\[[^]]*\]\(([^)]+)\)`)

func main() {
	var failures []string
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if path == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(data)
		for _, forbidden := range []string{"/data/" + "workspace/", "/data/" + "home/"} {
			if strings.Contains(text, forbidden) {
				failures = append(failures, fmt.Sprintf("%s contains local path %q", path, forbidden))
			}
		}
		if strings.HasSuffix(path, ".md") {
			failures = append(failures, brokenLinks(path, text)...)
		}
		return nil
	})
	if err != nil {
		failures = append(failures, err.Error())
	}
	failures = append(failures, exportedDocFailures(".")...)
	if len(failures) > 0 {
		for _, failure := range failures {
			fmt.Fprintln(os.Stderr, failure)
		}
		os.Exit(1)
	}
	fmt.Println("repository documentation and path checks passed")
}

func exportedDocFailures(root string) []string {
	var failures []string
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if rel == "." || rel == "sandbox" {
				return nil
			}
			return filepath.SkipDir
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			return err
		}
		for _, decl := range file.Decls {
			switch typed := decl.(type) {
			case *ast.FuncDecl:
				if typed.Name.IsExported() && exportedReceiver(typed.Recv) && typed.Doc == nil {
					failures = append(failures, fmt.Sprintf("%s:%d exported %s has no GoDoc", path, fset.Position(typed.Pos()).Line, typed.Name.Name))
				}
			case *ast.GenDecl:
				for _, spec := range typed.Specs {
					switch item := spec.(type) {
					case *ast.TypeSpec:
						if !item.Name.IsExported() {
							continue
						}
						if item.Doc == nil && typed.Doc == nil {
							failures = append(failures, fmt.Sprintf("%s:%d exported %s has no GoDoc", path, fset.Position(item.Pos()).Line, item.Name.Name))
						}
						failures = append(failures, exportedFieldDocFailures(path, item.Name.Name, item.Type, fset)...)
					case *ast.ValueSpec:
						for _, name := range item.Names {
							if name.IsExported() && item.Doc == nil && typed.Doc == nil {
								failures = append(failures, fmt.Sprintf("%s:%d exported %s has no GoDoc", path, fset.Position(name.Pos()).Line, name.Name))
							}
						}
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		failures = append(failures, err.Error())
	}
	return failures
}

func exportedReceiver(receiver *ast.FieldList) bool {
	if receiver == nil {
		return true
	}
	if len(receiver.List) != 1 {
		return false
	}
	typeExpr := receiver.List[0].Type
	if pointer, ok := typeExpr.(*ast.StarExpr); ok {
		typeExpr = pointer.X
	}
	name, ok := typeExpr.(*ast.Ident)
	return ok && name.IsExported()
}

func exportedFieldDocFailures(path, typeName string, expr ast.Expr, fset *token.FileSet) []string {
	var fields *ast.FieldList
	switch typed := expr.(type) {
	case *ast.StructType:
		fields = typed.Fields
	case *ast.InterfaceType:
		fields = typed.Methods
	default:
		return nil
	}
	var failures []string
	for _, field := range fields.List {
		for _, name := range field.Names {
			if name.IsExported() && field.Doc == nil && field.Comment == nil {
				failures = append(failures, fmt.Sprintf("%s:%d exported %s.%s has no GoDoc", path, fset.Position(name.Pos()).Line, typeName, name.Name))
			}
		}
	}
	return failures
}

func brokenLinks(markdownPath, text string) []string {
	var failures []string
	scanner := bufio.NewScanner(strings.NewReader(text))
	line := 0
	for scanner.Scan() {
		line++
		for _, match := range markdownLink.FindAllStringSubmatch(scanner.Text(), -1) {
			target := strings.Trim(strings.Fields(match[1])[0], "<>")
			if target == "" || strings.HasPrefix(target, "#") {
				continue
			}
			parsed, err := url.Parse(target)
			if err != nil || parsed.Scheme != "" || parsed.Host != "" {
				continue
			}
			path, err := url.PathUnescape(parsed.Path)
			if err != nil || path == "" {
				continue
			}
			resolved := filepath.Clean(filepath.Join(filepath.Dir(markdownPath), filepath.FromSlash(path)))
			if _, err = os.Stat(resolved); err != nil {
				failures = append(failures, fmt.Sprintf("%s:%d broken link %s", markdownPath, line, target))
			}
		}
	}
	return failures
}
