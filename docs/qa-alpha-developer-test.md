# Alpha Developer QA Test

This is a QA task for testing the project as if QA were a new external
developer. The goal is not to prove every recovery/failover case. The goal is to
verify that the public alpha instructions are runnable, understandable, and
honest.

## Test Persona

Act like a developer who found the repository today and wants to try the
Kubernetes alpha.

Assumptions:

- no private design docs
- no internal gate names
- no prior knowledge of the project's history
- access to a Linux host or VM with Kubernetes, Docker, and iSCSI tooling

## Documents Under Test

Read only these first:

1. `README.md`
2. `deploy/k8s/alpha/README.md`
3. `docs/architecture.md`
4. `docs/roadmap.md`

Only open source files or scripts when the public docs tell you to.

## Primary Run

From the repository root:

```bash
bash scripts/run-k8s-alpha.sh
```

Expected high-level result:

- images build or the docs clearly explain how to build/import them
- Kubernetes manifests apply cleanly
- dynamic PVC is created
- pod reaches `Succeeded`
- pod log includes `/data/payload.bin: OK`
- cleanup leaves no active iSCSI session for the test target
- artifact path is printed

## Required Evidence

Capture these in the QA report:

- repository commit SHA
- host OS and Kubernetes distribution/version
- command run
- wall-clock duration
- pod final phase
- pod log checksum line
- blockmaster log path
- blockvolume log path
- blockcsi log path
- `iscsiadm -m session` before and after
- cleanup result

If the command fails, the report must include:

- first failing command or timeout point
- relevant pod describe output
- daemon logs if available
- whether the failure is documentation, host environment, manifest, or product
  behavior

## Usability Checks

Answer these questions directly:

- Could a new developer identify prerequisites without asking us?
- Did any command expose internal gate names or old branch names?
- Did any script output confuse alpha users with internal labels?
- Did the README overclaim production readiness?
- Did the docs explain iSCSI vs future NVMe-oF clearly?
- Did the docs explain current storage durability limits clearly?
- Was cleanup clear and safe?

## Pass Criteria

The developer-facing alpha path passes when:

- the public docs are enough to run the smoke test
- the run produces a checksum-verified pod write/read
- the system cleans up test resources
- failures, if any, are actionable without internal context
- the documentation does not claim production readiness

## Non-Claims

This QA task does not prove:

- multi-node durability
- failover while a pod remains mounted
- RF=3 quorum behavior
- production security
- performance
- snapshot/resize/clone support
- NVMe-oF integration

Those need separate tests.

## Suggested Report Format

```text
Alpha developer test: PASS/FAIL
Commit:
Host:
Kubernetes:
Command:
Duration:
Pod result:
Checksum evidence:
Artifacts:
Cleanup:

Usability notes:
- ...

Blocking issues:
- ...

Non-blocking polish:
- ...
```

