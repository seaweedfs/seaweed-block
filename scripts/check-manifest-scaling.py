#!/usr/bin/env python3
"""Check three-sample manifest benchmark scaling; no device or host access.

Run go test with -bench BenchmarkPrepareManifestUnchangedGroups -benchtime=20x
-count=3, save stdout, then pass that file to this checker. A 16x metadata
scale must grow less than 64x on the same serial runner. This is an explicit
qualification benchmark, not a wall-clock CI unit test.
"""
import re
import statistics
import sys
from pathlib import Path

if len(sys.argv) != 2:
    raise SystemExit("usage: check-manifest-scaling.py BENCHMARK_LOG")
samples = {256: [], 1024: [], 4096: []}
for count, ns in re.findall(
    r"BenchmarkPrepareManifestUnchangedGroups/(256|1024|4096)-\d+\s+\d+\s+(\d+) ns/op",
    Path(sys.argv[1]).read_text(encoding="utf-8-sig"),
):
    samples[int(count)].append(int(ns))
if any(len(values) != 3 for values in samples.values()):
    raise SystemExit("need exactly three samples per group count")
medians = {count: statistics.median(values) for count, values in samples.items()}
if min(medians.values()) <= 0:
    raise SystemExit("benchmark durations must be positive")
growth = medians[4096] / medians[256]
print("median_ns", medians, "growth_4096_over_256", growth, "limit", 64)
raise SystemExit(0 if growth < 64 else 1)
