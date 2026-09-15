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
	failures = append(failures, privateWireBoundaryFailures(".")...)
	if len(failures) > 0 {
		for _, failure := range failures {
			fmt.Fprintln(os.Stderr, failure)
		}
		os.Exit(1)
	}
	fmt.Println("repository documentation and path checks passed")
}

func privateWireBoundaryFailures(root string) []string {
	var failures []string
	if _, err := os.Stat(filepath.Join(root, "pb")); err == nil {
		failures = append(failures, "generated bindings must live under internal/gen, not public pb")
	} else if !os.IsNotExist(err) {
		failures = append(failures, err.Error())
	}
	header := "X-Access-" + "Token"
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if entry.IsDir() {
			if rel == ".git" || rel == "internal/gen" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(data), header) && !strings.HasPrefix(rel, "internal/dataplane/") {
			failures = append(failures, fmt.Sprintf("%s contains runtime authentication outside internal/dataplane", path))
		}
		file, parseErr := parser.ParseFile(fset, path, data, 0)
		if parseErr != nil {
			return parseErr
		}
		for _, imported := range file.Imports {
			value := strings.Trim(imported.Path.Value, `"`)
			if !strings.Contains(rel, "/") && (value == "connectrpc.com/connect" || strings.Contains(value, "/internal/gen/")) {
				failures = append(failures, fmt.Sprintf("%s imports private wire package %s from the public root package", path, value))
			}
			if strings.Contains(value, "/internal/gen/") && !strings.HasPrefix(rel, "internal/dataplane/") {
				failures = append(failures, fmt.Sprintf("%s imports generated bindings outside internal/dataplane", path))
			}
		}
		if strings.HasPrefix(rel, "internal/dataplane/") {
			failures = append(failures, dataPlaneEscapeHatchFailures(path, file, fset)...)
		}
		return nil
	})
	if err != nil {
		failures = append(failures, err.Error())
	}
	return failures
}

func dataPlaneEscapeHatchFailures(path string, file *ast.File, fset *token.FileSet) []string {
	forbiddenMethods := map[string]bool{
		"BaseURL": true, "HTTPClient": true, "Filesystem": true, "Process": true,
	}
	var failures []string
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if function.Recv == nil && function.Name.Name == "Request" {
			failures = append(failures, fmt.Sprintf("%s:%d exposes forbidden data-plane escape hatch Request", path, fset.Position(function.Pos()).Line))
			continue
		}
		if forbiddenMethods[function.Name.Name] && receiverName(function.Recv) == "Client" {
			failures = append(failures, fmt.Sprintf("%s:%d exposes forbidden data-plane escape hatch Client.%s", path, fset.Position(function.Pos()).Line, function.Name.Name))
		}
	}
	return failures
}

func receiverName(receiver *ast.FieldList) string {
	if receiver == nil || len(receiver.List) != 1 {
		return ""
	}
	expr := receiver.List[0].Type
	if pointer, ok := expr.(*ast.StarExpr); ok {
		expr = pointer.X
	}
	name, _ := expr.(*ast.Ident)
	if name == nil {
		return ""
	}
	return name.Name
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
