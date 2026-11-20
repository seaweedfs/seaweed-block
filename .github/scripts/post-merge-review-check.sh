#!/usr/bin/env bash
# Usage: post-merge-review-check.sh DENY_FILE < changed-paths
#
# Prints every changed path that matches a deny pattern and exits 1 if there
# is one; exits 0 if none match, 2 on a usage error or an empty deny list.
set -euo pipefail

deny_file=${1:?usage: post-merge-review-check.sh DENY_FILE < changed-paths}
patterns=()
while IFS= read -r line; do
	line=${line%%#*}
	line=${line%"${line##*[![:space:]]}"}
	line=${line#"${line%%[![:space:]]*}"}
	[[ -n $line ]] && patterns+=("$line")
done <"$deny_file"
if ((${#patterns[@]} == 0)); then
	echo "post-merge-review: empty deny list $deny_file" >&2
	exit 2
fi

denied=0
checked=0
while IFS= read -r path; do
	[[ -n $path ]] || continue
	checked=$((checked + 1))
	for p in "${patterns[@]}"; do
		# Unquoted on the right: a glob, where * also matches /.
		if [[ $path == $p ]]; then
			echo "DENY $path (matches $p)"
			denied=1
			break
		fi
	done
done
echo "post-merge-review: $checked changed path(s) checked against ${#patterns[@]} pattern(s)"
exit "$denied"
