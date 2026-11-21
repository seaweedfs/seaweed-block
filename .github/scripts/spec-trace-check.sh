#!/usr/bin/env bash
# Usage: spec-trace-check.sh BODY_FILE < changes
#
# Checks the spec trace in a PR description. changes are NUL-separated
# STATUS, PATH pairs (post-merge-review-changed.sh).
#
# - Feature:, Machine/INV:, Model: and Red-first: each start a line (after
#   optional indentation, a list marker or bold) and carry a value that is not
#   a template placeholder; "NONE:" needs a reason of at least 10 characters.
# - Machine/INV: starts with exactly one PR kind: "IMPL <machine>.<transition>",
#   "FIX <INV-ID>" or "CHANGE <machine>". NONE is not a kind.
# - Simplest: (20+ characters) is present when the PR adds, modifies or
#   deletes non-test Go under core/ or cmd/.
#
# Prints every problem and exits 1 if there is one; exits 0 if none, 2 on a
# usage error or a malformed change list.
set -euo pipefail

body_file=${1:?usage: spec-trace-check.sh BODY_FILE < changes}
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
kind_re="^(IMPL [A-Za-z0-9_]+[.][A-Za-z0-9_>.-]+|FIX INV-[A-Z0-9]+(-[A-Z0-9]+)*|CHANGE [A-Za-z0-9_]+)$end"
mi=$(value Machine/INV:)
if [[ -n $mi && $mi != '<'* ]]; then
	if [[ ! $mi =~ $kind_re ]]; then
		problem "Machine/INV: must start with IMPL <machine>.<transition>, FIX <INV-ID> or CHANGE <machine>"
	elif [[ ${mi#"${BASH_REMATCH[0]}"} =~ (^|[^A-Za-z0-9_])(IMPL|FIX|CHANGE)([^A-Za-z0-9_]|$) ]]; then
		problem "Machine/INV: names more than one kind; a PR is exactly one of IMPL, FIX or CHANGE"
	fi
fi

core=0
while IFS= read -r -d '' status; do
	[[ $status == [ADMT] ]] || { echo "spec-trace: malformed change list: status ${status@Q}" >&2; exit 2; }
	IFS= read -r -d '' path || { echo "spec-trace: malformed change list: status $status without a path" >&2; exit 2; }
	[[ -n $path ]] || { echo "spec-trace: malformed change list: empty path" >&2; exit 2; }
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

exit $bad
