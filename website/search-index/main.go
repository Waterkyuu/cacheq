// Command search-index generates documentation and public Go API search records after a site build.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/doc"
	"go/format"
	"go/parser"
	"go/token"
	"html"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// entry describes a searchable page section or exported Go declaration.
type entry struct {
	// Title labels the search result and is given extra weight by the browser.
	Title string `json:"title"`
	// URL opens a published chapter anchor or the declaration's source line.
	URL string `json:"url"`
	// Language restricts documentation matches to the reader's current locale.
	Language string `json:"language"`
	// Content includes searchable prose, code, and declaration comments.
	Content string `json:"content"`
}

// articlePattern isolates documentation, excluding navigation and footers.
var articlePattern = regexp.MustCompile(`(?s)<article\b[^>]*>(.*?)</article>`)

// titlePattern extracts the built page's human-readable browser title.
var titlePattern = regexp.MustCompile(`(?s)<title\b[^>]*>(.*?)</title>`)

// languagePattern reads the locale declared by the rendered HTML document.
var languagePattern = regexp.MustCompile(`<html\b[^>]*\blang="([^"]+)"`)

// headingPattern identifies real section anchors emitted by the documentation renderer.
var headingPattern = regexp.MustCompile(`(?s)<h[23]\b[^>]*\bid="([^"]+)"[^>]*>(.*?)</h[23]>`)

// tagPattern removes generated presentation markup from searchable text.
var tagPattern = regexp.MustCompile(`<[^>]*>`)

// hiddenPattern excludes controls and invisible navigation labels from search snippets.
var hiddenPattern = regexp.MustCompile(
	`(?s)<(?:script|style|nav|button|footer)\b[^>]*>.*?</(?:script|style|nav|button|footer)>` +
		`|<span\b[^>]*\bclass="sr-only"[^>]*>.*?</span>`,
)

// main fails the build when the website or source cannot produce a complete index.
func main() {
	build := flag.String("build", "build", "Static website output directory")
	base := flag.String("base-path", "/cacheq", "Published repository URL prefix")
	source := flag.String("source", "..", "Go library source directory")
	sourceRef := os.Getenv("DOCS_REF")
	if sourceRef == "" {
		sourceRef = "waterkyuu/feat/docs-site"
	}
	ref := flag.String("source-ref", sourceRef, "Git reference used for source links")
	flag.Parse()
	records, err := buildIndex(*build, *base)
	if err != nil {
		log.Fatal(err)
	}
	symbols, err := apiEntries(*source, *ref)
	if err != nil {
		log.Fatal(err)
	}
	records = append(records, symbols...)
	content, err := json.Marshal(records)
	if err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(*build, "search-index.json"), content, 0o600); err != nil {
		log.Fatal(err)
	}
	log.Printf("Indexed %d documentation sections and %d API records", len(records)-len(symbols), len(symbols))
}

// buildIndex uses published HTML paths and anchors so results cannot drift from site routing.
func buildIndex(directory, base string) ([]entry, error) {
	if strings.ContainsAny(base, "?#\\") || strings.Contains(base, "..") {
		return nil, fmt.Errorf("invalid base path %q", base)
	}
	base = "/" + strings.Trim(base, "/")
	if base == "/" {
		base = ""
	}
	records := make([]entry, 0)
	root := os.DirFS(directory)
	err := fs.WalkDir(root, ".", func(filename string, item fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if item.IsDir() || item.Name() != "index.html" {
			return nil
		}
		content, err := fs.ReadFile(root, filename)
		if err != nil {
			return fmt.Errorf("read page: %w", err)
		}
		source := string(content)
		article := articlePattern.FindStringSubmatch(source)
		if len(article) == 0 {
			return nil
		}
		title := titlePattern.FindStringSubmatch(source)
		language := languagePattern.FindStringSubmatch(source)
		if len(title) == 0 || len(language) == 0 {
			return fmt.Errorf("missing page metadata in %s", filename)
		}
		relative, err := filepath.Rel(".", filepath.Dir(filename))
		if err != nil {
			return err
		}
		url := base + "/"
		if relative != "." {
			url += filepath.ToSlash(relative) + "/"
		}
		pageTitle := strings.TrimSuffix(plainText(title[1]), " | cacheq")
		records = append(
			records,
			entry{Title: pageTitle, URL: url, Language: language[1], Content: plainText(article[1])},
		)
		headings := headingPattern.FindAllStringSubmatchIndex(article[1], -1)
		for i, heading := range headings {
			end := len(article[1])
			if i+1 < len(headings) {
				end = headings[i+1][0]
			}
			anchor := html.UnescapeString(article[1][heading[2]:heading[3]])
			sectionTitle := plainText(article[1][heading[4]:heading[5]])
			sectionTitle = strings.TrimRight(sectionTitle, "\u200b#")
			records = append(records, entry{Title: sectionTitle + " · " + pageTitle, URL: url + "#" + anchor,
				Language: language[1], Content: plainText(article[1][heading[1]:end])})
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("index documentation: %w", err)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("no rendered documentation in %s", directory)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].URL < records[j].URL })
	return records, nil
}

// plainText retains Unicode and Go code while decoding escaped HTML entities.
func plainText(content string) string {
	// Starlight adds screen-reader anchor labels after headings; they must not replace useful snippets.
	content = hiddenPattern.ReplaceAllString(content, " ")
	// Inline tags must not split identifiers highlighted into separate spans.
	content = regexp.MustCompile(`</?(?:span|code|a|strong|em)\b[^>]*>`).ReplaceAllString(content, "")
	content = tagPattern.ReplaceAllString(content, " ")
	return strings.Join(strings.Fields(html.UnescapeString(content)), " ")
}

// apiEntries indexes only the library's exported API without importing or executing application code.
func apiEntries(directory, ref string) ([]entry, error) {
	names, err := filepath.Glob(filepath.Join(directory, "*.go"))
	if err != nil {
		return nil, fmt.Errorf("list source: %w", err)
	}
	files := make([]*ast.File, 0)
	fileSet := token.NewFileSet()
	for _, name := range names {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fileSet, name, nil, parser.ParseComments)
		if err != nil {
			return nil, fmt.Errorf("parse source: %w", err)
		}
		files = append(files, file)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no library source in %s", directory)
	}
	pkg, err := doc.NewFromFiles(fileSet, files, "github.com/Waterkyuu/cacheq", doc.PreserveAST)
	if err != nil {
		return nil, fmt.Errorf("read API: %w", err)
	}
	records := make([]entry, 0)
	appendDeclaration := func(name, comment string, declaration ast.Node) error {
		var signature bytes.Buffer
		if err := format.Node(&signature, fileSet, declaration); err != nil {
			return fmt.Errorf("format %s: %w", name, err)
		}
		position := fileSet.Position(declaration.Pos())
		url := fmt.Sprintf(
			"https://github.com/Waterkyuu/cacheq/blob/%s/%s#L%d",
			ref,
			filepath.Base(position.Filename),
			position.Line,
		)
		for _, language := range []string{"zh-CN", "en"} {
			records = append(
				records,
				entry{Title: name, URL: url, Language: language, Content: comment + " " + signature.String()},
			)
		}
		return nil
	}
	functions := append([]*doc.Func{}, pkg.Funcs...)
	for _, typ := range pkg.Types {
		if err := appendDeclaration(typ.Name, typ.Doc, typ.Decl); err != nil {
			return nil, err
		}
		functions = append(functions, typ.Funcs...)
		for _, method := range typ.Methods {
			if err := appendDeclaration(typ.Name+"."+method.Name, method.Doc, method.Decl.Type); err != nil {
				return nil, err
			}
		}
	}
	for _, function := range functions {
		if err := appendDeclaration(function.Name, function.Doc, function.Decl.Type); err != nil {
			return nil, err
		}
	}
	values := append([]*doc.Value{}, pkg.Consts...)
	values = append(values, pkg.Vars...)
	for _, value := range values {
		for _, name := range value.Names {
			if !token.IsExported(name) {
				continue
			}
			if err := appendDeclaration(name, value.Doc, value.Decl); err != nil {
				return nil, err
			}
		}
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].Title == records[j].Title {
			return records[i].Language < records[j].Language
		}
		return records[i].Title < records[j].Title
	})
	return records, nil
}
