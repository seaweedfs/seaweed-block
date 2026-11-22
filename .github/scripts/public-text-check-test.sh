#!/usr/bin/env bash
# Self-test of public-text-check.sh in a throwaway repository.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
t=$(mktemp -d); trap 'rm -rf "$t"' EXIT
cd "$t"; git init -q .; git config user.email t@example.invalid; git config user.name t
echo base > a.txt; git add a.txt; git commit -q -m "base"
mb=$(git rev-parse HEAD)
: > title; : > body
run() { bash "$here/public-text-check.sh" "$@" >out 2>&1; }
expect() { # expect RC NAME
  local want=$1 name=$2 rc=0
  shift 2
  run "$@" || rc=$?
  [ "$rc" = "$want" ] || { echo "public-text-check-test: $name: exit $rc, want $want"; cat out; exit 1; }
}
echo "plain line" >> a.txt; git commit -q -am "add a plain line"
expect 0 "clean change" "$mb" HEAD title body
seat="clau""de06"; path="V:/sha""re/x"; nas="share/rd""ma/y"
echo "// asked by $seat" >> a.txt; git commit -q -am "comment"
expect 1 "seat name in an added line" "$mb" HEAD title body
grep -q "a.txt:3" out || { echo "public-text-check-test: hit does not name file and line"; cat out; exit 1; }
git reset -q --hard HEAD~1
echo "ok" >> a.txt; git commit -q -am "review by $seat"
expect 1 "seat name in a commit subject" "$mb" HEAD title body
git reset -q --hard HEAD~1
echo "see $path" > title; expect 1 "share path in the title" "$mb" HEAD title body; : > title
echo "results under $nas" > body; expect 1 "share path in the description" "$mb" HEAD title body; : > body
echo "an old line with $seat" >> a.txt; git commit -q -am "old"; mb2=$(git rev-parse HEAD)
echo "new clean line" >> a.txt; git commit -q -am "new"
expect 0 "an existing line is not this change's" "$mb2" HEAD title body
# A file line that begins with "++ " shows in the diff as "+++ ...": an added line, not a header.
printf '%s
' "++ $seat" >> a.txt; git commit -q -am "plus line"
expect 1 "an added line that looks like a diff header" "$mb2" HEAD title body
git reset -q --hard HEAD~1
bs=$(printf '\')
printf '%s
' "${bs}${bs}nas${bs}sha""re${bs}rd""ma${bs}z" > title
expect 1 "backslash form of the share path in the title" "$mb2" HEAD title body; : > title
expect 2 "a missing title file is an error, not clean" "$mb2" HEAD no-such-title body
expect 2 "a missing description file is an error, not clean" "$mb2" HEAD title no-such-body
expect 2 "an unknown commit is an error, not clean" 0000000000000000000000000000000000000000 HEAD title body
echo "public-text-check-test: ok"
