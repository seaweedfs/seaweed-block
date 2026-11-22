#!/usr/bin/env bash
# Refuses internal names in what a pull request makes public: the lines it
# adds, its commit messages, its title and its description. This repository is
# public; internal seat names and internal file-share paths do not belong in it.
#
# Usage: public-text-check.sh MERGE_BASE HEAD TITLE_FILE BODY_FILE
# Exit 0: clean. Exit 1: at least one hit (each printed). Exit 2: usage.
#
# The patterns live only here and in this check's own test; both files are
# excluded from the scan of added lines.
set -euo pipefail
[ "$#" -eq 4 ] || { echo "usage: public-text-check.sh MERGE_BASE HEAD TITLE_FILE BODY_FILE" >&2; exit 2; }
mb=$1; head=$2; title=$3; body=$4
for f in "$title" "$body"; do
  [ -f "$f" ] && [ -r "$f" ] || { echo "public-text: cannot read $f" >&2; exit 2; }
done
git rev-parse -q --verify "$mb^{commit}" >/dev/null && git rev-parse -q --verify "$head^{commit}" >/dev/null ||
  { echo "public-text: unknown commit $mb or $head" >&2; exit 2; }
# match: a grep error fails the check instead of reading as "no hit".
match() { local rc=0; grep -E -i -- "$pat" "$@" || rc=$?; [ "$rc" -le 1 ] || { echo "public-text: grep failed ($rc)" >&2; exit 2; }; }
pat='(claude|codex)0[0-9]|share[/\]rdma|V:[/\]share|diskstation'
out=$(mktemp); trap 'rm -f "$out"' EXIT
report() { # report WHERE < matching lines
  local where=$1 line
  while IFS= read -r line; do
    printf 'public-text: %s: %s
' "$where" "${line:0:160}" >>"$out"
  done
}
# Added lines of the diff, with file and line number.
git diff --no-color --no-renames -U0 "$mb" "$head" -- . \
  ':(exclude).github/scripts/public-text-check.sh' ':(exclude).github/scripts/public-text-check-test.sh' |
  awk '
    /^diff --git / { hdr = 1; next }
    hdr && /^\+\+\+ / { f = substr($0, 7); next }
    /^@@ /         { hdr = 0; split($3, a, ","); n = substr(a[1], 2) + 0; next }
    !hdr && /^\+/  { print f ":" n ": " substr($0, 2); n++ }
  ' | match | report "added line"
# Commit messages (subjects and bodies) of the pull request.
git log --format='%h %s%n%b' "$mb..$head" | match | report "commit message"
match "$title" | report "title"
match "$body" | report "description"
hits=$(wc -l <"$out" | tr -d ' ')
if [ "$hits" -gt 0 ]; then
  cat "$out"
  echo "public-text: $hits hit(s). Say what the code or test does instead of who asked for it; keep internal paths out." >&2
  exit 1
fi
echo "public-text: clean"
