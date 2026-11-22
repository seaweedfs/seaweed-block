package invcheck

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// specsWithoutBoundsLeft is the number of specs in docs/spec that have no
// row yet in the "Exhausted bounds" table of docs/spec/README.md. It only
// goes down: a new spec arrives with its row.
const specsWithoutBoundsLeft = 21

var stateCount = regexp.MustCompile(`\d`)

// A published spec says what was checked: docs/spec/README.md has an
// "Exhausted bounds" table with, for each spec, one configuration that TLC
// explored completely, the bounds of that configuration, its distinct state
// count, and what the model leaves out. Specs that predate the table are
// listed in testdata/specs-without-bounds.txt, and that list only shrinks.
func TestPublicSpecsStateTheirBounds(t *testing.T) {
	root := repoRoot(t)
	specDir := filepath.Join(root, "docs", "spec")

	entries, err := os.ReadDir(specDir)
	if err != nil {
		t.Fatal(err)
	}
	specs := map[string]bool{}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".tla") {
			specs[e.Name()] = true
		}
	}

	raw, err := os.ReadFile(filepath.Join(specDir, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	doc := strings.ReplaceAll(string(raw), "\r\n", "\n")
	start := strings.Index(doc, "\n## Exhausted bounds\n")
	if start < 0 {
		t.Fatal("docs/spec/README.md: heading \"## Exhausted bounds\" not found")
	}
	section := doc[start+1:]
	if end := strings.Index(section[1:], "\n## "); end >= 0 {
		section = section[:end+1]
	}

	withRow := map[string]bool{}
	for _, line := range strings.Split(section, "\n") {
		if !strings.HasPrefix(line, "| `") {
			continue
		}
		cells := strings.Split(strings.Trim(line, "|"), "|")
		if len(cells) != 5 {
			t.Errorf("Exhausted bounds: a row has %d cells, want 5 (spec, configuration, bounds, distinct states, not modelled): %s", len(cells), line)
			continue
		}
		for i := range cells {
			cells[i] = strings.TrimSpace(cells[i])
		}
		spec := strings.Trim(cells[0], "`")
		cfg := strings.Trim(cells[1], "`")
		if !specs[spec] {
			t.Errorf("Exhausted bounds: %s is not a file in docs/spec", spec)
			continue
		}
		if withRow[spec] {
			t.Errorf("Exhausted bounds: %s has two rows; keep the one configuration that best states the claim", spec)
		}
		withRow[spec] = true
		if _, err := os.Stat(filepath.Join(specDir, filepath.FromSlash(cfg))); err != nil {
			t.Errorf("Exhausted bounds: %s names configuration %q, which is not a file under docs/spec", spec, cfg)
		}
		if cells[2] == "" {
			t.Errorf("Exhausted bounds: %s states no bounds", spec)
		}
		if !stateCount.MatchString(cells[3]) {
			t.Errorf("Exhausted bounds: %s states no distinct state count", spec)
		}
		if cells[4] == "" {
			t.Errorf("Exhausted bounds: %s does not say what is not modelled", spec)
		}
	}

	var missing []string
	for s := range specs {
		if !withRow[s] {
			missing = append(missing, s)
		}
	}
	sort.Strings(missing)

	listRaw, err := os.ReadFile(filepath.Join("testdata", "specs-without-bounds.txt"))
	if err != nil {
		t.Fatal(err)
	}
	var listed []string
	for _, l := range strings.Split(strings.ReplaceAll(string(listRaw), "\r\n", "\n"), "\n") {
		if l = strings.TrimSpace(l); l != "" && !strings.HasPrefix(l, "#") {
			listed = append(listed, l)
		}
	}
	sort.Strings(listed)

	if strings.Join(missing, "\n") != strings.Join(listed, "\n") {
		t.Errorf("specs with no Exhausted bounds row:\n  %s\ntestdata/specs-without-bounds.txt lists:\n  %s\nA new spec needs a row in docs/spec/README.md; a spec that gained a row leaves the list.",
			strings.Join(missing, "\n  "), strings.Join(listed, "\n  "))
	}
	if len(listed) != specsWithoutBoundsLeft {
		t.Errorf("testdata/specs-without-bounds.txt has %d specs, specsWithoutBoundsLeft is %d: lower the constant when a spec leaves the list; it never goes up", len(listed), specsWithoutBoundsLeft)
	}
}
