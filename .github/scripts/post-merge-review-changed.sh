#!/usr/bin/env bash
# Usage: post-merge-review-changed.sh BASE HEAD
#
# Writes the paths a PR changes (BASE...HEAD), NUL-separated and unquoted, so
# any byte in a name reaches the check as itself. --no-renames lists both
# names of a moved file.
set -euo pipefail
base=${1:?usage: post-merge-review-changed.sh BASE HEAD}
head=${2:?usage: post-merge-review-changed.sh BASE HEAD}
exec git diff -z --name-only --no-renames "$base...$head"
