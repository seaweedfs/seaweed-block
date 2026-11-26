#!/usr/bin/env bash
# Renders an alpha manifest written for the local image (sw-block:local,
# imagePullPolicy: Never) with the selected image, the way the stack itself is
# rendered: usage: render-k8s-alpha-image.sh IMAGE IN OUT
set -euo pipefail
[[ $# -eq 3 ]] || { echo "usage: $0 IMAGE IN OUT" >&2; exit 2; }
image_sed="$(printf '%s' "$1" | sed 's/[\/&]/\&/g')"
sed -e "s/sw-block:local/${image_sed}/g" \
  -e "s/imagePullPolicy: Never/imagePullPolicy: IfNotPresent/g" \
  "$2" >"$3"
grep -q "image: $1\$" "$3" || { echo "render-k8s-alpha-image: $3 does not name image $1" >&2; exit 1; }
