// Package invcheck checks docs/design/invariants.md against the code: the
// row count it states is the count its tables hold, every Go test a row
// names exists, an ACTIVE row names at least one test, and a statement is
// short (one property per row).
package invcheck

import (
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"
)

type invRow struct {
	id        string
	statement string
	tests     []string
	status    string
	line      int
}

// maxStatement is the longest Statement cell, in characters, a row may have.
const maxStatement = 300

var (
	rowID     = regexp.MustCompile("^\\| `(INV-[A-Z0-9-]+)` \\|")
	testName  = regexp.MustCompile("`(Test[A-Za-z0-9_]+)`")
	testFunc  = regexp.MustCompile(`(?m)^func (Test[A-Za-z0-9_]+)\(`)
	countLine = regexp.MustCompile("(\\d+) (?:`INV-` )?rows: (\\d+) ACTIVE, (\\d+) STUB, (\\d+) ARCHIVED")
)

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

// partB returns the Part B rows of the invariant document and its text.
func partB(t *testing.T, root string) ([]invRow, string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, "docs", "design", "invariants.md"))
	if err != nil {
		t.Fatal(err)
	}
	doc := strings.ReplaceAll(string(raw), "\r\n", "\n")
	start := strings.Index(doc, "\n## Part B")
	end := strings.Index(doc, "\n## Part C")
	if start < 0 || end < start {
		t.Fatal("invariants.md: Part B or Part C heading not found")
	}
	firstLine := strings.Count(doc[:start], "\n") + 1
	var rows []invRow
	for i, line := range strings.Split(doc[start:end], "\n") {
		m := rowID.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		cells := splitRow(line)
		if len(cells) != 7 {
			t.Errorf("invariants.md:%d: row %s has %d cells, want 7", firstLine+i, m[1], len(cells))
			continue
		}
		r := invRow{id: m[1], statement: strings.TrimSpace(cells[1]), status: strings.TrimSpace(cells[6]), line: firstLine + i}
		for _, tm := range testName.FindAllStringSubmatch(cells[4], -1) {
			r.tests = append(r.tests, tm[1])
		}
		rows = append(rows, r)
	}
	return rows, doc
}

// splitRow splits a table row on unescaped pipes outside code spans.
func splitRow(line string) []string {
	line = strings.TrimSpace(line)
	line = strings.TrimPrefix(line, "|")
	line = strings.TrimSuffix(line, "|")
	var cells []string
	var cur strings.Builder
	inCode := false
	for i := 0; i < len(line); i++ {
		c := line[i]
		switch {
		case c == '\\' && i+1 < len(line) && line[i+1] == '|':
			cur.WriteString(`\|`)
			i++
		case c == '`':
			inCode = !inCode
			cur.WriteByte(c)
		case c == '|' && !inCode:
			cells = append(cells, cur.String())
			cur.Reset()
		default:
			cur.WriteByte(c)
		}
	}
	return append(cells, cur.String())
}

// goTests returns every top-level Test function name in the repository.
func goTests(t *testing.T, root string) map[string]bool {
	t.Helper()
	found := map[string]bool{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if name := d.Name(); name == ".git" || name == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, m := range testFunc.FindAllSubmatch(src, -1) {
			found[string(m[1])] = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return found
}

func TestInvariantRowsNameExistingTests(t *testing.T) {
	root := repoRoot(t)
	rows, _ := partB(t, root)
	if len(rows) == 0 {
		t.Fatal("no INV rows found in Part B")
	}
	tests := goTests(t, root)
	seen := map[string]int{}
	for _, r := range rows {
		if prev, dup := seen[r.id]; dup {
			t.Errorf("invariants.md:%d: %s is also at line %d", r.line, r.id, prev)
		}
		seen[r.id] = r.line
		switch r.status {
		case "ACTIVE":
			if len(r.tests) == 0 {
				t.Errorf("invariants.md:%d: %s is ACTIVE and names no test", r.line, r.id)
			}
		case "STUB", "ARCHIVED":
		default:
			t.Errorf("invariants.md:%d: %s has status %q", r.line, r.id, r.status)
		}
		for _, name := range r.tests {
			if !tests[name] {
				t.Errorf("invariants.md:%d: %s names %s, which is not a test in this repository", r.line, r.id, name)
			}
		}
	}
}

func TestInvariantRowCountMatchesTables(t *testing.T) {
	rows, doc := partB(t, repoRoot(t))
	by := map[string]int{}
	for _, r := range rows {
		by[r.status]++
	}
	want := fmt.Sprintf("%d rows: %d ACTIVE, %d STUB, %d ARCHIVED", len(rows), by["ACTIVE"], by["STUB"], by["ARCHIVED"])
	m := strings.Replace(countLine.FindString(doc), "`INV-` ", "", 1)
	if m == "" {
		t.Fatalf("invariants.md states no row count; the tables hold: %s", want)
	}
	if m != want {
		t.Fatalf("invariants.md says %q; its tables hold %q", m, want)
	}
}

// TestInvariantStatementsAreShort checks that the Statement of every row that
// is not ARCHIVED is at most maxStatement characters, except the rows named in
// testdata/long-statements.txt. That list only shrinks: it must be sorted,
// name each row once, and name only rows that exist and are still too long.
func TestInvariantStatementsAreShort(t *testing.T) {
	root := repoRoot(t)
	rows, _ := partB(t, root)
	const list = "internal/invcheck/testdata/long-statements.txt"
	raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(list)))
	if err != nil {
		t.Fatal(err)
	}
	listed := map[string]int{}
	prev := ""
	for i, line := range strings.Split(strings.ReplaceAll(string(raw), "\r\n", "\n"), "\n") {
		id := strings.TrimSpace(line)
		if id == "" || strings.HasPrefix(id, "#") {
			continue
		}
		if at, dup := listed[id]; dup {
			t.Errorf("%s:%d: %s is also at line %d", list, i+1, id, at)
			continue
		}
		if id < prev {
			t.Errorf("%s:%d: %s is not in sorted order (after %s)", list, i+1, id, prev)
		}
		listed[id], prev = i+1, id
	}
	exists := map[string]bool{}
	for _, r := range rows {
		exists[r.id] = true
		n := utf8.RuneCountInString(r.statement)
		long := r.status != "ARCHIVED" && n > maxStatement
		at, ok := listed[r.id]
		switch {
		case long && !ok:
			t.Errorf("invariants.md:%d: %s has a %d-character statement, over %d: split it into rows of one property each", r.line, r.id, n, maxStatement)
		case !long && ok:
			t.Errorf("%s:%d: %s is no longer over %d characters (%d, %s): delete its line", list, at, r.id, maxStatement, n, r.status)
		}
	}
	for id, at := range listed {
		if !exists[id] {
			t.Errorf("%s:%d: %s is not a row of invariants.md: delete its line", list, at, id)
		}
	}
	// The list only shrinks. Its size is pinned here, so a new exception needs
	// an edit of this constant, and where the base commit is at hand every
	// listed row must already have been listed there.
	if len(listed) != longStatementsLeft {
		t.Errorf("%s names %d rows, longStatementsLeft is %d: lower the constant when a line is deleted; it is never raised", list, len(listed), longStatementsLeft)
	}
	base, err := exec.Command("git", "-C", root, "show", "HEAD^1:"+list).Output()
	if err != nil {
		t.Logf("no base version of %s (%v): the only-shrink comparison is skipped", list, err)
		return
	}
	was := map[string]bool{}
	for _, line := range strings.Split(strings.ReplaceAll(string(base), "\r\n", "\n"), "\n") {
		if id := strings.TrimSpace(line); id != "" && !strings.HasPrefix(id, "#") {
			was[id] = true
		}
	}
	for id, at := range listed {
		if !was[id] {
			t.Errorf("%s:%d: %s was not listed in the base commit: the list only shrinks", list, at, id)
		}
	}
}

// longStatementsLeft is the number of rows testdata/long-statements.txt may
// name. It only goes down.
const longStatementsLeft = 0

// conflictMarkers returns the 1-based lines of text that are a merge
// conflict marker: a line starting with "<<<<<<< " or ">>>>>>> ", or a line
// that is exactly "=======".
func conflictMarkers(text string) []int {
	var at []int
	for i, line := range strings.Split(strings.ReplaceAll(text, "\r\n", "\n"), "\n") {
		if strings.HasPrefix(line, "<<<<<<< ") || strings.HasPrefix(line, ">>>>>>> ") || line == "=======" {
			at = append(at, i+1)
		}
	}
	return at
}

// The marker check finds every marker of a conflicted region and nothing
// that only looks like one (a longer rule, a marker without its space).
func TestConflictMarkersAreFound(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "conflict-markers.fixture"))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := fmt.Sprint(conflictMarkers(string(raw))), "[2 4 6]"; got != want {
		t.Fatalf("conflict markers at lines %s; want %s", got, want)
	}
}

// The ledger and the lists invcheck reads carry no merge conflict marker:
// a merge resolved by hand must not leave one behind.
func TestLedgerFilesHaveNoConflictMarkers(t *testing.T) {
	root := repoRoot(t)
	for _, rel := range []string{"docs/design/invariants.md", "internal/invcheck/testdata/long-statements.txt", "internal/invcheck/testdata/specs-without-bounds.txt"} {
		raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range conflictMarkers(string(raw)) {
			t.Errorf("%s:%d: merge conflict marker", rel, line)
		}
	}
}
