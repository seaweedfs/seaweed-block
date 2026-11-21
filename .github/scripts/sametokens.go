// Command sametokens reports whether two versions of a Go source file have the
// same tokens in the same order, ignoring comments that the compiler ignores.
//
// Usage: go run sametokens.go OLD NEW
//
// A missing file counts as empty. Directive comments are compared as tokens:
// //go:..., // +build, //line, /*line, //export; in a file that uses cgo
// (any "C" string literal, as in import "C"), every comment counts, since the
// preamble is code. Exit 0: same tokens; 1: different
// (the first difference is printed); 2: usage error or a file that does not scan.
package main

import (
	"fmt"
	"go/scanner"
	"go/token"
	"os"
	"strings"
)

type tok struct {
	t   token.Token
	lit string
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: sametokens OLD NEW")
		os.Exit(2)
	}
	a, err := tokens(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	b, err := tokens(os.Args[2])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	for i := 0; i < len(a) || i < len(b); i++ {
		if i >= len(a) || i >= len(b) || a[i] != b[i] {
			fmt.Printf("token %d differs: %v vs %v\n", i, at(a, i), at(b, i))
			os.Exit(1)
		}
	}
}

func at(ts []tok, i int) string {
	if i >= len(ts) {
		return "end of file"
	}
	return fmt.Sprintf("%s %q", ts[i].t, ts[i].lit)
}

// tokens scans a file and keeps every token except comments the compiler
// ignores. An automatic semicolon is compared by kind only.
func tokens(path string) ([]tok, error) {
	src, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	fset := token.NewFileSet()
	file := fset.AddFile(path, -1, len(src))
	var s scanner.Scanner
	var scanErr error
	s.Init(file, src, func(pos token.Position, msg string) {
		if scanErr == nil {
			scanErr = fmt.Errorf("%s: %s", pos, msg)
		}
	}, scanner.ScanComments)
	var all []tok
	for {
		_, t, lit := s.Scan()
		if t == token.EOF {
			break
		}
		if t == token.SEMICOLON {
			lit = ""
		}
		all = append(all, tok{t, lit})
	}
	if scanErr != nil {
		return nil, scanErr
	}
	cgo := false
	for _, x := range all {
		if x.t == token.STRING && x.lit == "\"C\"" {
			cgo = true
		}
	}
	var out []tok
	for _, x := range all {
		if x.t == token.COMMENT && !cgo && !directive(x.lit) {
			continue
		}
		out = append(out, x)
	}
	return out, nil
}

func directive(c string) bool {
	for _, p := range []string{"//go:", "// +build", "//+build", "//line ", "/*line ", "//export "} {
		if strings.HasPrefix(c, p) {
			return true
		}
	}
	return false
}
