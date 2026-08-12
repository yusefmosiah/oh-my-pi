package main

import (
	"strings"
	"testing"
)

func TestRewriteQualifiedIdentifiersRespectsLexicalScopes(t *testing.T) {
	const alias = "__omp_fmt"
	tests := []struct {
		name               string
		source             string
		want               string
		persistentShadowed bool
	}{
		{
			name:   "unresolved package selector",
			source: `fmt.Println("outer")`,
			want:   `__omp_fmt.Println("outer")`,
		},
		{
			name:   "statement local shadows package",
			source: `fmt := 1; fmt.Println(fmt)`,
			want:   `fmt := 1; fmt.Println(fmt)`,
		},
		{
			name:   "nested local shadows only inner selector",
			source: `if true { fmt := 1; fmt.Println(fmt) }; fmt.Println(2)`,
			want:   `if true { fmt := 1; fmt.Println(fmt) }; __omp_fmt.Println(2)`,
		},
		{
			name:   "parameter shadows package",
			source: `func f(fmt int) { fmt.Println(fmt) }`,
			want:   `func f(fmt int) { fmt.Println(fmt) }`,
		},
		{
			name:               "persistent package declaration shadows facade",
			source:             `fmt.Println("later")`,
			want:               `fmt.Println("later")`,
			persistentShadowed: true,
		},
		{
			name:   "import declaration is removed but use is rewritten",
			source: `import "fmt"; fmt.Println("value")`,
			want:   ` __omp_fmt.Println("value")`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var got string
			var err error
			if test.persistentShadowed {
				got, err = rewriteCellImports(test.source, alias, "__omp", map[string]bool{"fmt": true})
			} else {
				got, err = rewriteCellImports(test.source, alias, "__omp")
			}
			if err != nil {
				t.Fatalf("rewriteCellImports: %v", err)
			}
			if got != test.want {
				t.Fatalf("got %q, want %q", got, test.want)
			}
		})
	}

	declared := topLevelDeclarations(`var fmt = 1; type Widget struct{}; func retained(){}`)
	for _, name := range []string{"fmt", "Widget", "retained"} {
		if !declared[name] {
			t.Fatalf("topLevelDeclarations omitted %q: %#v", name, declared)
		}
	}

	// An identifier in a comment/string must never be rewritten, even when a
	// nearby selector is package-qualified.
	got, err := rewriteCellImports(`// fmt.Println("comment")
fmt.Println("value")
_ = "fmt.Println(\"string\")"`, alias, "__omp")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, `// fmt.Println("comment")`) || !strings.Contains(got, `__omp_fmt.Println("value")`) || !strings.Contains(got, `"fmt.Println(\"string\")"`) {
		t.Fatalf("scope-safe rewrite touched comment/string: %q", got)
	}
}

func TestSplitCellSourceMixedDeclarationsAndStatements(t *testing.T) {
	source := "import \"fmt\"\nvar count = 41\nfmt.Println(\"set\", count)\ncount++\nfmt.Println(\"next\", count)"
	parts, err := splitCellSource(source)
	if err != nil {
		t.Fatalf("splitCellSource: %v", err)
	}
	if len(parts) != 5 {
		t.Fatalf("got %d parts, want 5: %#v", len(parts), parts)
	}
	wantDeclarations := []bool{true, true, false, false, false}
	for i, want := range wantDeclarations {
		if parts[i].declaration != want {
			t.Fatalf("part %d declaration=%v, want %v", i, parts[i].declaration, want)
		}
		if parts[i].start < 0 || parts[i].end > len(source) || parts[i].start >= parts[i].end {
			t.Fatalf("part %d has invalid span %#v", i, parts[i])
		}
	}
	if got := source[parts[0].start:parts[0].end]; got != `import "fmt"` {
		t.Fatalf("first part %q", got)
	}
	if got := source[parts[1].start:parts[1].end]; got != "var count = 41" {
		t.Fatalf("second part %q", got)
	}
	if got := source[parts[2].start:parts[2].end]; got != `fmt.Println("set", count)` {
		t.Fatalf("third part %q", got)
	}
}

func TestSplitCellSourcePreservesMultilineDeclarations(t *testing.T) {
	source := "func retained() int {\n\treturn 7\n}\nfmt.Println(retained())"
	parts, err := splitCellSource(source)
	if err != nil {
		t.Fatalf("splitCellSource: %v", err)
	}
	if len(parts) != 2 || !parts[0].declaration || parts[1].declaration {
		t.Fatalf("unexpected parts: %#v", parts)
	}
	if got := source[parts[0].start:parts[0].end]; got != "func retained() int {\n\treturn 7\n}" {
		t.Fatalf("function part %q", got)
	}
}
