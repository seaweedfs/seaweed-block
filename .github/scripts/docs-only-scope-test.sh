#!/usr/bin/env bash
# Tests docs-only-scope.sh against real commits, and that test.yml applies it
# only to the process suite of a pull request. Any failure exits nonzero.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
scope="$here/docs-only-scope.sh"
workflow="$here/../workflows/test.yml"
fail=0
check() { # name want got
	if [ "$2" = "$3" ]; then echo "ok   $1"; else echo "FAIL $1: want [$2], got [$3]"; fail=1; fi
}
repo=$(mktemp -d)
trap 'rm -rf "$repo"' EXIT
cd "$repo"
git init -q
git config user.email t@example.invalid
git config user.name t
git config core.autocrlf false
mkdir -p docs cmd
echo a >docs/a.md
echo r >README.md
echo g >cmd/main.go
git add -A
git commit -qm base
base=$(git rev-parse HEAD)
case_of() { # name want: commit the current tree, classify against base, reset
	git add -A
	git commit -qm "$1" --allow-empty
	check "$1" "$2" "$(bash "$scope" "$base" HEAD)"
	git reset -q --hard "$base"
}
echo b >docs/b.md; echo a2 >docs/a.md
case_of "two docs files" "docs_only 2"
echo b >docs/b.md; echo t >cmd/x_test.go
case_of "docs and a test file" "run 2 cmd/x_test.go"
case_of "no file changed" "run 0"
mkdir -p docs-old; echo x >docs-old/x.md
case_of "a docs-like prefix" "run 1 docs-old/x.md"
echo r2 >README.md
case_of "README outside docs" "run 1 README.md"
git mv docs/a.md cmd/a.md
case_of "a move out of docs" "run 2 cmd/a.md"
git rm -q docs/a.md
case_of "a docs deletion" "docs_only 1"
for i in $(seq 1 20000); do printf 'x\n' >"docs/f$i.md"; done
echo late >zz-late.go
case_of "20000 docs files and one other, listed last" "run 20001 zz-late.go"
if out=$(bash "$scope" "$base" no-such-revision 2>/dev/null); then
	check "an unknown revision is an error" "nonzero exit" "exit 0: $out"
else
	check "an unknown revision is an error" "nonzero exit" "nonzero exit"
fi
# test.yml: the scope step runs only for a pull request's process suite, and
# every process step it can skip names its output.
step=$(grep -n -A3 'name: Docs-only pull request' "$workflow" | grep "if:" || true)
check "scope step condition" "1" "$(printf '%s' "$step" | grep -c "matrix.suite == 'process' && github.event_name == 'pull_request'" || true)"
check "process steps gated" "4" "$(grep -c "if: matrix.suite == 'process' && steps.scope.outputs.docs_only != '1'" "$workflow" || true)"
check "no ungated process step" "0" "$(grep -c "if: matrix.suite == 'process'$" "$workflow" || true)"
exit "$fail"
