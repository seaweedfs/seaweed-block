#!/usr/bin/env bash
# Usage: spec-trace-check.sh BODY_FILE [GO_DIFF] < changes
#
# Checks the spec trace in a PR description. changes are NUL-separated
# STATUS, PATH pairs (post-merge-review-changed.sh).
#
# - Feature:, Machine/INV:, Model: and Red-first: each start a line (after
#   optional indentation, a list marker or bold) and carry a value that is not
#   a template placeholder; "NONE:" needs a reason of at least 10 characters.
# - Machine/INV: starts with exactly one PR kind: "IMPL <machine>.<transition>",
#   "FIX <INV-ID>", "CHANGE <machine>", or one of the kinds a PR that changes
#   no behaviour may use, each checked against what it changes:
#   DOCS (docs, tests, and non-test Go whose hunks change only comments; GO_DIFF
#   is the git diff of the non-test Go files), TEST (only tests, testdata and
#   test helpers), CI (only .github/ and scripts/). NONE is not a kind.
# - Simplest: (20+ characters) is present when the PR adds, modifies or
#   deletes non-test Go under core/ or cmd/.
#
# Prints every problem and exits 1 if there is one; exits 0 if none, 2 on a
# usage error or a malformed change list.
set -euo pipefail

body_file=${1:?usage: spec-trace-check.sh BODY_FILE [GO_DIFF] < changes}
go_diff=${2-}
[[ -f $body_file ]] || { echo "spec-trace: body file $body_file missing" >&2; exit 2; }
body=$(tr -d '\r' <"$body_file")

# value KEY: the text after KEY on the first line that starts with KEY,
# trimmed. A key inside another word ("Not-Feature:") does not count.
value() {
	local line
	line=$(printf '%s\n' "$body" | grep -m1 -E "^[[:space:]]*([-*][[:space:]]+)?(\*\*)?$1" || true)
	[[ -n $line ]] || return 0
	line=${line#*"$1"}
	line=${line#\*\*}
	line=${line#"${line%%[![:space:]]*}"}
	printf '%s' "${line%"${line##*[![:space:]]}"}"
}

bad=0
problem() { echo "spec-trace: $1"; bad=1; }

# Each trace line is declared once: a second Machine/INV line could carry a
# second kind past the check.
for key in Feature: Machine/INV: Model: Red-first: Simplest:; do
	n=$(printf '%s\n' "$body" | grep -cE "^[[:space:]]*([-*][[:space:]]+)?(\*\*)?$key" || true)
	((n <= 1)) || problem "$key is declared $n times; declare it once"
done

for key in Feature: Machine/INV: Model: Red-first:; do
	v=$(value "$key")
	if [[ -z $v ]]; then
		problem "missing or empty $key"
	elif [[ $v == '<'* ]]; then
		problem "$key still holds the template placeholder"
	elif [[ $v == NONE:* ]]; then
		r=${v#NONE:}
		r=${r#"${r%%[![:space:]]*}"}
		((${#r} >= 10)) || problem "$key NONE needs a reason of at least 10 characters"
	fi
done

# One kind token, ending at the line end, a space, a comma or a slash.
end='($|[[:space:],/])'
kind_re="^(IMPL [A-Za-z0-9_]+[.][A-Za-z0-9_>.-]+|FIX INV-[A-Z0-9]+(-[A-Z0-9]+)*|CHANGE [A-Za-z0-9_]+|DOCS|TEST|CI)$end"
kind=""
mi=$(value Machine/INV:)
if [[ -n $mi && $mi != '<'* ]]; then
	if [[ ! $mi =~ $kind_re ]]; then
		problem "Machine/INV: must start with IMPL <machine>.<transition>, FIX <INV-ID>, CHANGE <machine>, DOCS, TEST or CI"
	else
		token=${BASH_REMATCH[1]}
		if [[ ${mi#"${BASH_REMATCH[0]}"} =~ (^|[^A-Za-z0-9_])(IMPL|FIX|CHANGE|DOCS|TEST|CI)([^A-Za-z0-9_]|$) ]]; then
			problem "Machine/INV: names more than one kind; a PR is exactly one kind"
		else
			kind=${token%% *}
		fi
	fi
fi

is_test() { [[ $1 == *_test.go || $1 == testdata/* || $1 == */testdata/* || $1 == */testutil/* ]]; }
is_doc() { [[ $1 == *.md || $1 == docs/* ]]; }
core=0
paths=()
while IFS= read -r -d '' status; do
	[[ $status == [ADMT] ]] || { echo "spec-trace: malformed change list: status ${status@Q}" >&2; exit 2; }
	IFS= read -r -d '' path || { echo "spec-trace: malformed change list: status $status without a path" >&2; exit 2; }
	[[ -n $path ]] || { echo "spec-trace: malformed change list: empty path" >&2; exit 2; }
	paths+=("$path")
	if [[ $path == core/*.go || $path == cmd/*.go ]] && [[ $path != *_test.go ]]; then
		core=1
	fi
done
# A record cut off before its NUL is malformed, not skipped.
[[ -z ${status-} ]] || { echo "spec-trace: malformed change list: unterminated record" >&2; exit 2; }
if ((core)); then
	s=$(value Simplest:)
	if ((${#s} < 20)) || [[ $s == '<'* ]]; then
		problem "Simplest: (20+ characters) is required when non-test Go under core/ or cmd/ changes"
	fi
fi

# comment_only: print each non-test Go file in GO_DIFF whose removed and added
# lines differ once line comments (outside strings) and spacing are dropped.
comment_only() {
	awk '
	function norm(s,   o, i, c, q, b) {
		o = ""; q = 0; b = 0
		for (i = 1; i <= length(s); i++) {
			c = substr(s, i, 1)
			if (!q && !b && c == "/" && substr(s, i + 1, 1) == "/") break
			if (q && c == "\\") { o = o c substr(s, i + 1, 1); i++; continue }
			if (c == "\"" && !b) q = !q
			if (c == "`" && !q) b = !b
			o = o c
		}
		gsub(/[ 	]+/, " ", o); sub(/^ /, "", o); sub(/ $/, "", o)
		return o
	}
	/^\+\+\+ / { f = substr($0, 7); next }
	/^--- / { next }
	/^[-+]/ {
		if (f !~ /\.go$/ || f ~ /_test\.go$/) next
		n = norm(substr($0, 2)); if (n == "") next
		k = f SUBSEP n
		if (substr($0, 1, 1) == "+") d[k]++; else d[k]--
	}
	END { for (k in d) if (d[k] != 0) { split(k, p, SUBSEP); bad[p[1]] = 1 } for (x in bad) print x }
	' "$1"
}

case $kind in
CI)
	for p in ${paths[@]+"${paths[@]}"}; do
		[[ $p == .github/* || $p == scripts/* ]] || problem "kind CI: $p is outside .github/ and scripts/"
	done
	;;
TEST)
	for p in ${paths[@]+"${paths[@]}"}; do
		is_test "$p" || problem "kind TEST: $p is not a test, testdata or test helper"
	done
	;;
DOCS)
	gofiles=()
	for p in ${paths[@]+"${paths[@]}"}; do
		if is_doc "$p" || is_test "$p"; then continue; fi
		if [[ $p == *.go ]]; then gofiles+=("$p"); continue; fi
		problem "kind DOCS: $p is neither docs, a test, nor Go"
	done
	if ((${#gofiles[@]})); then
		if [[ -z $go_diff || ! -f $go_diff ]]; then
			problem "kind DOCS: non-test Go changed but no Go diff was given to check it is comment-only"
		else
			# Every changed Go file must be in the diff, or its hunks went unchecked.
			for p in "${gofiles[@]}"; do
				grep -qxF -e "+++ b/$p" -e "--- a/$p" "$go_diff" || problem "kind DOCS: $p is not in the Go diff, so its hunks were not checked"
			done
			while IFS= read -r f; do
				[[ -n $f ]] && problem "kind DOCS: $f changes more than comments"
			done < <(comment_only "$go_diff")
		fi
	fi
	;;
esac

exit $bad
