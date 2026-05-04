# `internal/` Overview

This directory contains internal support libraries that should not become public
API.

## Packages

| Package | Purpose | Main Interfaces |
|---|---|---|
| `testops/` | Scenario registry and driver framework for repeatable test runs. | registry data, run request/result types, Go test driver, shell driver |
| `legacy/testrunner/` | Older scenario/performance assets retained as references. | YAML scenarios, perf notes |

## Design Rules

- Keep TestOps independent of product authority/control-plane internals.
- A scenario can call product binaries or shell scripts, but should not import
  storage/recovery packages to fake success.
- Result files should be enough for another engineer to understand what ran and
  where artifacts landed.
