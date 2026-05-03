# `internal/legacy/testrunner` Overview

This directory contains older scenario/performance assets and compatibility
material for the broader test-runner effort.

## Layout

| Directory | Purpose |
|---|---|
| `scenarios/` | YAML scenario examples and test data. |
| `perf/` | Performance and durability baseline notes. |

## Relationship To TestOps

`cmd/sw-testops` and `internal/testops` are the current lightweight scenario
runner in this repository. These older testrunner assets are useful references,
but should not force V2-style control semantics into the V3 code.

## Design Rules

- Reuse scenario structure where it helps.
- Do not port old authority, promotion, or recovery assumptions blindly.
- Prefer small registered scenarios with explicit artifacts over large opaque
  harnesses.
