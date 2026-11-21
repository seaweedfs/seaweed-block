#!/usr/bin/env bash
# Usage: post-merge-review-check.sh ALLOW_FILE < changes
#
# changes are NUL-separated STATUS, PATH pairs (post-merge-review-changed.sh).
# A path passes only if it matches an allow pattern and no "!" pattern, and
# a test file may be added or modified but not deleted (a move deletes its
# old name). Prints every change that does not pass and exits 1 if there is
# one; exits 0 if all pass, 2 on a usage error, a malformed change or a
# missing or empty allow list.
set -euo pipefail

allow_file=${1:?usage: post-merge-review-check.sh ALLOW_FILE < changes}
if [[ ! -f $allow_file ]]; then
	echo "post-merge-review: allow list $allow_file missing" >&2
	exit 2
fi
allow=()
deny=()
# A last line without a newline is read too: dropping it could drop a "!".
while IFS= read -r line || [[ -n $line ]]; do
	line=${line%%#*}
	line=${line%"${line##*[![:space:]]}"}
	line=${line#"${line%%[![:space:]]*}"}
	[[ -n $line ]] || continue
	if [[ $line == '!'* ]]; then
		deny+=("${line#!}")
	else
		allow+=("$line")
	fi
done <"$allow_file"
if ((${#allow[@]} == 0)); then
	echo "post-merge-review: empty allow list $allow_file" >&2
	exit 2
fi

denied=0
checked=0
malformed() {
	echo "post-merge-review: malformed change list: $1" >&2
	exit 2
}
while :; do
	# Every field ends in NUL: a field cut off at the end is malformed, not
	# skipped, and only the statuses git diff --no-renames writes are known.
	if ! IFS= read -r -d '' status; then
		[[ -z $status ]] || malformed "unterminated status ${status@Q}"
		break
	fi
	[[ $status == [ADMT] ]] || malformed "unknown status ${status@Q}"
	IFS= read -r -d '' path || malformed "status $status without a terminated path"
	[[ -n $path ]] || malformed "status $status with an empty path"
	checked=$((checked + 1))
	why="not on the allow list"
	for p in "${allow[@]}"; do
		# Unquoted on the right: a glob, where * also matches /.
		if [[ $path == $p ]]; then
			why=""
			break
		fi
	done
	for p in ${deny[@]+"${deny[@]}"}; do
		if [[ $path == $p ]]; then
			why="matches !$p"
			break
		fi
	done
	# A labelled PR may not delete a test (or turn it into something else).
	if [[ -z $why && $path == *_test.go && $status != [AM] ]]; then
		why="test file status $status: only adding or modifying a test is allowed"
	fi
	if [[ -n $why ]]; then
		printf 'DENY %s %q (%s)\n' "$status" "$path" "$why"
		denied=1
	fi
done
echo "post-merge-review: $checked changed path(s) checked against ${#allow[@]} allow and ${#deny[@]} deny pattern(s)"
exit "$denied"
