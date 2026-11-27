#!/usr/bin/env bash
# docs-only-scope.sh BASE HEAD: classifies the files changed between BASE and
# HEAD for the process-level suite. It prints one line:
#   "docs_only N"            every one of the N changed files is under docs/
#   "run N [FIRST-OTHER]"    no file changed, or at least one is elsewhere
# and exits 0. Any error (git fails, the list cannot be read) exits nonzero,
# and the caller then runs the tests: an error never selects docs_only.
#
# The NUL-delimited list is written to a file first and read to its end, so
# no pipe can stop early and leave a writer's error looking like an answer.
set -euo pipefail
[ "$#" -eq 2 ] || { echo "usage: docs-only-scope.sh BASE HEAD" >&2; exit 2; }
list=$(mktemp)
trap 'rm -f "$list"' EXIT
if ! git diff --name-only --no-renames -z "$1" "$2" >"$list"; then
	echo "docs-only-scope: git diff $1 $2 failed" >&2
	exit 2
fi
n=0
other=""
while IFS= read -r -d '' f; do
	n=$((n + 1))
	case $f in
	docs/*) ;;
	*) [ -n "$other" ] || other=$f ;;
	esac
done <"$list"
if [ "$n" -gt 0 ] && [ -z "$other" ]; then
	echo "docs_only $n"
else
	echo "run $n${other:+ $other}"
fi
