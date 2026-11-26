#!/usr/bin/env bash
# Self-test of run-named-tests.sh with the gate (required-tests-check.py
# --status), through a real shell and test2json, with fake test binaries:
# a test that reports PASS and then exits nonzero, or hangs until the
# timeout, must fail the gate; an honest pass must not. Needs go (test2json).
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
fail=0

# fake NAME BEHAVIOUR: a test binary that prints a passing verbose run of
# whatever -test.run names, then behaves as told.
fake() {
	local f=$tmp/$1
	cat >"$f" <<EOF
#!/usr/bin/env bash
for a in "\$@"; do case "\$a" in -test.run=*) re=\${a#-test.run=} ;; esac; done
prev=""
for a in "\$@"; do [ "\$prev" = -test.run ] && re=\$a; prev=\$a; done
name=\${re#^}; name=\${name%\\\$}
echo "=== RUN   \$name"
echo "--- PASS: \$name (0.01s)"
echo "PASS"
$2
EOF
	chmod +x "$f"
	echo "$f"
}

# expect WANT_EXIT NAME BINARY
expect() {
	local want=$1 name=$2 bin=$3 got=0
	printf 'TestA\nTestB\n' >"$tmp/names"
	TEST_PKG=example/fake bash "$here/run-named-tests.sh" "$bin" "$tmp/names" "$tmp/events" "$tmp/status" 3s "$tmp" >/dev/null
	"${PYTHON:-python3}" -I "$here/required-tests-check.py" --status "$tmp/status" "$tmp/names" "$tmp/events" >"$tmp/out" || got=$?
	if [[ $got != "$want" ]]; then
		echo "FAIL $name: exit $got, want $want"
		cat "$tmp/out"
		fail=1
	fi
}

expect 0 "PASS and exit 0" "$(fake ok 'exit 0')"
expect 1 "PASS, then exit 1" "$(fake exit1 'exit 1')"
expect 1 "PASS, then a hang until the timeout" "$(fake hang 'sleep 30')"

if ((fail == 0)); then echo "run-named-tests: 3 cases ok"; fi
exit "$fail"
