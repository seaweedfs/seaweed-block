package engine

import (
	"os"
	"strings"
	"testing"
)

// T4d-1 fence: substring matching on SessionClosedFailed.Reason MUST
// be REMOVED from core/engine/apply.go. Engine branches on typed
// FailureKind only. This test greps the source to fail loudly if a
// future change re-introduces substring parsing.
//
// Per architect HIGH v0.1 #1 + v0.3 boundary fix: Reason is
// DIAGNOSTIC TEXT ONLY; engine MUST NOT parse it.

func TestEngine_SessionFailed_NoMoreSubstringMatch(t *testing.T) {
	source, err := os.ReadFile("apply.go")
	if err != nil {
		t.Fatalf("read apply.go: %v", err)
	}
	src := string(source)

	// Strip line comments so godoc mentions (e.g. "the substring
	// helper `isWALRecycledFailure` was removed at T4d-1") don't
	// trigger the fence. Block comments excluded the same way.
	stripped := stripGoComments(src)

	// Forbidden CODE patterns (not godoc): function defs + actual
	// call sites that would indicate substring-parsing fallback.
	forbidden := []string{
		`func isWALRecycledFailure`,    // function definition
		`func isStartTimeoutFailure`,   // function definition
		`func containsAny(`,            // function definition
		`isWALRecycledFailure(`,        // call site
		`isStartTimeoutFailure(`,       // call site
		`containsAny(`,                 // call site
		`strings.Contains(e.Reason`,    // direct substring on Reason
		`strings.Contains(reason`,      // bare-name variant
	}
	for _, f := range forbidden {
		if strings.Contains(stripped, f) {
			t.Errorf("FAIL: apply.go (code, not comments) contains forbidden substring-match pattern %q. Engine MUST branch on typed FailureKind only (T4d-1).", f)
		}
	}
}

// stripGoComments removes // line comments and /* ... */ block
// comments from Go source so the fence doesn't false-positive on
// godoc mentions of the removed helper names.
func stripGoComments(src string) string {
	var out strings.Builder
	i := 0
	inLineComment := false
	inBlockComment := false
	inString := false
	inRawString := false
	for i < len(src) {
		// Block comment end
		if inBlockComment {
			if i+1 < len(src) && src[i] == '*' && src[i+1] == '/' {
				inBlockComment = false
				i += 2
				continue
			}
			i++
			continue
		}
		// Line comment end
		if inLineComment {
			if src[i] == '\n' {
				inLineComment = false
				out.WriteByte('\n')
			}
			i++
			continue
		}
		// String literal — skip
		if inString {
			out.WriteByte(src[i])
			if src[i] == '\\' && i+1 < len(src) {
				out.WriteByte(src[i+1])
				i += 2
				continue
			}
			if src[i] == '"' {
				inString = false
			}
			i++
			continue
		}
		if inRawString {
			out.WriteByte(src[i])
			if src[i] == '`' {
				inRawString = false
			}
			i++
			continue
		}
		// Detect comment / string starts
		if i+1 < len(src) && src[i] == '/' && src[i+1] == '/' {
			inLineComment = true
			i += 2
			continue
		}
		if i+1 < len(src) && src[i] == '/' && src[i+1] == '*' {
			inBlockComment = true
			i += 2
			continue
		}
		if src[i] == '"' {
			inString = true
			out.WriteByte(src[i])
			i++
			continue
		}
		if src[i] == '`' {
			inRawString = true
			out.WriteByte(src[i])
			i++
			continue
		}
		out.WriteByte(src[i])
		i++
	}
	return out.String()
}
