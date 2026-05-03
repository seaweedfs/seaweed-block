# `testops/` Overview

This directory contains scenario registry data and run outputs for TestOps.

## Layout

| Directory | Purpose |
|---|---|
| `registry/` | JSON scenario registrations consumed by `cmd/sw-testops`. |
| `runs/` | Local run output directory. Generated results should not be treated as source unless explicitly checked in. |

## Registry Contract

Each scenario should identify:

- name
- driver
- command or package/test target
- minimum commit or evidence anchor when relevant
- expected artifact behavior

## Design Rules

- Register only scenarios with a clear pass/fail contract.
- Known-failing exploratory harnesses should stay outside the registry until
  they are useful as intentional red tests.
- A scenario result should distinguish infrastructure error from product test
  failure.

