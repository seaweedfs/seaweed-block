#!/usr/bin/env bash
# Usage: post-merge-review-changed.sh BASE HEAD
#
# Writes what a PR changes (BASE...HEAD) as NUL-separated STATUS, PATH pairs,
# unquoted, so any byte in a name reaches the check as itself. --no-renames
# reports a move as a delete of the old name and an add of the new one.
set -euo pipefail
base=${1:?usage: post-merge-review-changed.sh BASE HEAD}
head=${2:?usage: post-merge-review-changed.sh BASE HEAD}
exec git diff -z --name-status --no-renames "$base...$head"
