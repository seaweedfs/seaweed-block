#!/usr/bin/env bash
# Usage: spec-trace-check.sh BODY_FILE < changes
#
# Checks the spec trace in a PR description. changes are NUL-separated
# STATUS, PATH pairs (post-merge-review-changed.sh).
#
# - Feature:, Machine/INV:, Model: and Red-first: are each present and
#   non-empty; "NONE:" needs a reason of at least 10 characters.
# - Machine/INV: starts with the PR's kind: "IMPL <machine>.<transition>",
#   "FIX <INV-ID>" or "CHANGE <machine>". NONE is not a kind.
# - Simplest: is present (20+ characters) when the PR changes non-test Go
#   under core/ or cmd/.
#
# Prints every problem and exits 1 if there is one; exits 0 if none, 2 on a
# usage error.
set -euo pipefail

body_file=${1:?usage: spec-trace-check.sh BODY_FILE < changes}
[[ -f $body_file ]] || { echo "spec-trace: body file $body_file missing" >&2; exit 2; }
body=$(tr -d '\r' <"$body_file")

# value KEY: the text after the first KEY in the body, leading space trimmed.
value() {
	local line
	line=$(printf '%s\n' "$body" | grep -m1 -F -- "$1" || true)
	[[ -n $line ]] || return 0
	line=${line#*"$1"}
	printf '%s' "${line#"${line%%[![:space:]]*}"}"
}

bad=0
problem() { echo "spec-trace: $1"; bad=1; }

for key in Feature: Machine/INV: Model: Red-first:; do
	v=$(value "$key")
	if [[ -z $v ]]; then
		problem "missing or empty $key"
	elif [[ $v == NONE:* ]]; then
		r=${v#NONE:}
		r=${r#"${r%%[![:space:]]*}"}
		((${#r} >= 10)) || problem "$key NONE needs a reason of at least 10 characters"
	fi
done

kind_re='^(IMPL [A-Za-z0-9_]+[.][^[:space:]]+|FIX INV-[A-Z0-9-]+|CHANGE [A-Za-z0-9_]+)'
mi=$(value Machine/INV:)
if [[ -n $mi && ! $mi =~ $kind_re ]]; then
	problem "Machine/INV: must start with IMPL <machine>.<transition>, FIX <INV-ID> or CHANGE <machine>"
fi

core=0
while IFS= read -r -d '' status && IFS= read -r -d '' path; do
	[[ $status == D ]] && continue
	if [[ $path == core/*.go || $path == cmd/*.go ]] && [[ $path != *_test.go ]]; then
		core=1
	fi
done
if ((core)); then
	s=$(value Simplest:)
	((${#s} >= 20)) || problem "Simplest: (20+ characters) is required when non-test Go under core/ or cmd/ changes"
fi

exit $bad
