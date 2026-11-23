package main_test

import (
	"fmt"
	"strings"
	"testing"
)

// diskFullVerdict decides a defect row. A known defect that still reproduces
// passes; an unknown one fails; a known one that no longer reproduces fails
// too, so the list cannot go stale and hide a later regression.
func diskFullVerdict(known map[int]bool, issue int, bad bool, msg string) (fail bool, line string) {
	switch {
	case bad && known[issue]:
		return false, fmt.Sprintf("KNOWN FINDING #%d: %s", issue, msg)
	case bad:
		return true, fmt.Sprintf("FINDING #%d: %s", issue, msg)
	case known[issue]:
		return true, fmt.Sprintf("FIXED #%d: no longer reproduces; remove it from knownDiskFullFindings so this row is strict", issue)
	}
	return false, ""
}

func TestDiskFullVerdict(t *testing.T) {
	known := map[int]bool{1: true}
	for _, c := range []struct {
		issue int
		bad   bool
		fail  bool
		line  string
	}{
		{1, true, false, "KNOWN FINDING #1"},
		{1, false, true, "FIXED #1"},
		{2, true, true, "FINDING #2"},
		{2, false, false, ""},
	} {
		fail, line := diskFullVerdict(known, c.issue, c.bad, "m")
		if fail != c.fail || !strings.HasPrefix(line, c.line) || (c.line == "" && line != "") {
			t.Errorf("issue %d bad %v: fail=%v line=%q", c.issue, c.bad, fail, line)
		}
	}
}
