# testops

TestOps wrapper scenarios for sw-test-runner. The scenarios here invoke
existing bash QA harnesses but add a unified run bundle, provenance
pinning, fresh-build enforcement, and cleanup-as-action gating.

## Layout

```
testops/
├── README.md                                   ← this file
└── scenarios/
    └── iscsi-p8-compat-soak.yaml               ← P8 compat soak (OS fio + K8s fio + attach/detach)
```

## Prerequisite

A `sw-test-runner` binary >= v0.1.5. Install:

```bash
# Linux x86_64
curl -L https://github.com/pingqiu/sw-test-runner/releases/download/v0.1.5/sw-test-runner-linux \
  -o sw-test-runner && chmod +x sw-test-runner
```

(Or build from source: `go install github.com/pingqiu/sw-test-runner/cmd/sw-test-runner@v0.1.5`.)

## Run iSCSI P8 compat soak

```bash
sw-test-runner run testops/scenarios/iscsi-p8-compat-soak.yaml \
  --env product_root=$PWD \
  --results-dir /mnt/smb/work/share/g15d-k8s
```

What this does (instead of `bash scripts/run-iscsi-compat-soak.sh`):

- forces a fresh build of `blockmaster`, `blockvolume`, `sw-block:local`
  and `sw-block-csi:local` at scenario start — no stale-binary or
  stale-image risk;
- pins `git_sha`, `dirty` flag, every binary `sha256`, and every image
  digest in `provenance.json`;
- routes each leaf step's artifacts into `$bundle/phases/<name>/` so
  there is one artifact tree per run, not three;
- gates each step's pass marker via `grep_log` (a missing `PASS` line
  fails the scenario, not silently);
- asserts cleanup as a phase: `assert_no_active_iscsi_sessions` and
  `assert_no_processes pattern="blockmaster|blockvolume"` run after
  each step and at the end. A leaked session or stale daemon fails
  the run.

## Bundle layout

```
<results-dir>/<run-id>/
├── manifest.json           run_id, scenario_sha256, git_sha, host, command_line, status
├── provenance.json         git/dirty/framework_version/host kernel-os-arch/images/binaries
├── result.json             phase-by-phase status, durations, action results (secrets redacted)
├── result.xml              JUnit
├── result.html
├── scenario.yaml           frozen copy of the input
├── phases/
│   ├── os_fio_repeat/      bash leaf artifacts (run.log, fio.iter1.log, blockvolume.log, ...)
│   ├── k8s_fio/
│   └── k8s_attach_detach/
├── bin/
│   ├── blockmaster
│   └── blockvolume
└── artifacts/              scenario-published files (collect_artifacts on failure)
```

## What the scenario does NOT replace

- the leaf bash scripts (`run-iscsi-os-smoke.sh`,
  `run-k8s-alpha-fio.sh`, `run-k8s-attach-detach-loop.sh`) — they
  still own the actual iSCSI / K8s logic;
- `scripts/run-iscsi-compat-soak.sh` — keep it for environments where
  a sw-test-runner binary is unavailable; the wrapper YAML is the
  preferred path going forward.

## When to TestOps-ify

This directory is **not** the home for every test. Wrappers are worth
the maintenance overhead only when they buy something the bash
pipeline can't.

**TestOps-ify** when the test is:

- a release / soak gate that influences ship/no-ship decisions;
- multi-stage and crosses OS + K8s (artifact + cleanup contracts get
  scattered without a wrapper);
- prone to stale-binary / stale-process / stale-port pollution
  between runs;
- expected to run on a regular cadence (CI nightly, weekly soak).

**Don't TestOps-ify** when the test is:

- one-shot debugging or repro work;
- still red and in active development (the YAML lags the bash);
- a unit / component test (Go test or `bash -n` is enough);
- a single command that needs no orchestration on top.

Every wrapper here must have a matching QA assignment under
`internal/docs/qa-assignments/` that states:

- the goal in one sentence;
- the single CLI command;
- the artifact and provenance expectations;
- the cleanup gate;
- the QA pass conditions.

We do not rewrite leaf scripts. The wrapper YAML stays a thin
orchestration shell; common patterns get extracted to a shared
template only after **2-3** wrappers have proven the pattern is real.

## Adding a new soak

1. Drop a new `*.yaml` under `scenarios/`.
2. Reuse the framing pattern: `pin_build` → `pre_clean` →
   `<your steps>` → `cleanup` (always:true).
3. Route every leaf script's `SW_BLOCK_ARTIFACT_DIR` to
   `{{ bundle_dir }}/phases/<step_name>/`.
4. Gate every step with `grep_log` against the leaf's `PASS` marker.
5. End-state-assert with `assert_no_active_iscsi_sessions` +
   `assert_no_processes` + `kubectl_assert_not_exists`.
6. Pre_clean is best-effort (`ignore_error: true`) — don't refuse to
   start on a slightly dirty host. The hard gate is the trailing
   `cleanup` phase plus per-step asserts.
