# PM Evaluation Guide — Day-1 Activation to First Volume

Audience: product manager (or anyone outside the engineering team) who wants
to evaluate, by hand, the "install Seaweed Block on Kubernetes and run a
volume" experience that Phase 20 ships.

Time required: ~10 minutes if everything works, ~30 minutes if you also do
the optional teardown and re-evaluate the user-readable summaries.

You will NOT need to understand the internals. You will run a small documented
command sequence plus one optional cleanup command and judge whether what comes
back is good enough to ship.

## What you are evaluating

The Phase 20 product claim is:

> A new operator can stand up Seaweed Block on a Kubernetes cluster with ONE
> documented command, create a first PVC-backed volume, verify writer/reader
> data through Kubernetes, and collect a readable status report — without
> reading the source code or following more than the documented runbook.

Your job is to validate that claim against the live lab. Not the unit tests,
not the docs alone — the actual install you would run on a customer's
cluster.

## Test server context — fill this in before you start

Have engineering give you these eight values up front and keep them with this
evaluation. Per-tester context lives under `work/test_server/` (e.g.
`work/test_server/pm-eval-<date>.txt`); paste the filled-in block there so
the next evaluator does not have to chase the same answers down.

```text
Linux host:                          # e.g. 192.168.1.184 (m02)
SSH user / key:                      # e.g. testdev / /c/work/dev_server/testdev_key
repo path:                           # e.g. /tmp/seaweed_block
branch / commit:                     # e.g. feature/sw-block @ <git rev-parse HEAD>
kubeconfig path:                     # e.g. default in-cluster, or /etc/rancher/k3s/k3s.yaml
是否可以 sudo:                        # yes / no
是否是 1-node 还是 3-node lab:        # 1-node / 3-node (affects which evaluation
                                     #   questions you can answer — see the
                                     #   "what you need" table at the end)
是否允许我执行 uninstall/cleanup:     # yes / no  (step 5 is gated on this)
```

If any value is `unknown` or `ask later`, **stop** and get it nailed down
before running step 1 — running an install on the wrong host or without
cleanup permission is a much worse outcome than waiting five minutes for
clarification.

## Prerequisites you need someone to set up for you

Have engineering hand you a terminal logged in to the host you wrote in
`Linux host:` above:

```text
ssh -i <ssh key from above> <user>@<host>
```

Confirm three things before you start:

```bash
kubectl get nodes
```

Expect at least one Ready node. For Day-1 activation validation alone, a
single Ready node (m02) is enough — the activate script auto-discovers
however many k3s nodes are present and imports images to all of them. If
you also want to verify the full "install → ready for recovery" story, you
need three Ready nodes (m01 + m02 + tp01), because node-loss survival
(Phase 18) is what consumes the multi-node topology. For the install
experience itself, m02 alone is sufficient.

```bash
ls /tmp/seaweed_block/scripts/activate-k8s-alpha.sh
```

Should print the path. If not, ask engineering to sync the source tree.

```bash
sudo iscsiadm -m session
```

Should print `iscsiadm: No active sessions.` If it shows any sessions,
engineering needs to clean the lab first.

## Step 1 — Run the one Day-1 command

```bash
cd /tmp/seaweed_block
bash scripts/activate-k8s-alpha.sh /tmp/seaweed_block
```

**What you should see** (paraphrased — exact wording may shift):

- a preflight summary line ending `status=PASS`,
- image-build progress (this is the slow part, ~30 seconds),
- image-import-into-each-node lines naming `m01`, `m02`, `tp01`,
- an "alpha install applied" or equivalent confirmation,
- "StorageClass sw-block-dynamic applied" (or similar),
- a readiness wait that names blockmaster and CSI components reaching Ready,
- a final `activation-summary.txt` path and a short summary block.

**Pass criteria for step 1**:

- the command exits with status 0,
- the final summary block names: protocol, ack profile, what to do next
  (the "next step is create a volume" line),
- the summary lists the non-claims (no hosted dashboard, no mutating admin
  controls, etc. — see step 4),
- you did not have to open a second terminal or read anything other than
  this command's output.

**Fail criteria**:

- you have to manually `kubectl apply` anything yourself,
- the command leaves blockmaster or CSI pods in `CrashLoopBackOff` or
  `ImagePullBackOff`,
- the command exits non-zero,
- the summary does not tell you the next user-visible step.

## Step 2 — Read the user-readable activation summary

Find the path printed at the end of step 1, then:

```bash
cat <that-path>
```

(Typically: `cat /var/lib/sw-block/.../activation-summary.txt` or similar
under the run's artifact directory.)

**Pass criteria for step 2** — the summary, read by you with no engineering
help, should answer all five of these questions:

1. Is the cluster READY to accept a PersistentVolumeClaim right now?
2. What protocol is in use (iSCSI / NVMe)?
3. What replication and durability profile (RF, ack profile)?
4. What is the single most likely NEXT user action?
5. What does this install explicitly NOT do yet?

If you cannot answer all five, that is a documentation/product-summary gap.

## Step 3 — Create the first PVC and verify writer/reader data

Run the documented first-volume helper:

```bash
cd /tmp/seaweed_block
bash scripts/run-basic-app-example.sh /tmp/seaweed_block
```

**What you should see**:

- the example StorageClass and PVC are applied,
- PVC `sw-block-example-pvc` reaches `Bound`,
- writer pod logs `/data/demo.bin: OK`,
- writer pod is deleted,
- reader pod logs `/data/demo.bin: OK` against the same PVC,
- status evidence is collected,
- a final `first-volume-summary.txt` block is printed.

**Pass criteria for step 3**:

- `first_volume_status=ok`,
- `writer_verified=true`,
- `reader_verified=true`,
- `inventory_status=ok`,
- `cleanup_status=ok`,
- `status_report=status/report/index.html`.

If the writer or reader times out, preserve the artifact directory printed by
the script. It should contain `diagnostics/<writer-or-reader>/writer-describe.txt`
or equivalent Kubernetes event evidence so engineering can diagnose without
manual SSH guessing.

## Step 4 — Inspect cluster state and the local read-only report

The `sw-block` CLI is not installed to `/usr/local/bin` by the Day-1 script
(only the in-cluster blockmaster/CSI images are). You have two options to
invoke it from the repo:

```bash
# Option A — invoke via go run from the repo (no build step):
cd /tmp/seaweed_block
kubectl -n kube-system port-forward deploy/sw-blockmaster 9333:9333 &
sleep 5
go run ./cmd/sw-block ops cluster --master-api 127.0.0.1:9333
go run ./cmd/sw-block ops report --master-api 127.0.0.1:9333 --out /tmp/sw-block-report

# Option B — build a one-off binary to /tmp:
cd /tmp/seaweed_block
go build -o /tmp/sw-block ./cmd/sw-block
kubectl -n kube-system port-forward deploy/sw-blockmaster 9333:9333 &
sleep 5
/tmp/sw-block ops cluster --master-api 127.0.0.1:9333
/tmp/sw-block ops report --master-api 127.0.0.1:9333 --out /tmp/sw-block-report
```

**What you should see**:

- a cluster-level status line (`status=ok` or `status=blocked reason=...`),
- a list of three nodes with their IPs and Ready=true,
- zero or one volumes,
- a "next action" line.
- `/tmp/sw-block-report/index.html`,
- `/tmp/sw-block-report/cluster-evidence.json`,
- `/tmp/sw-block-report/timeline.jsonl`,
- `/tmp/sw-block-report/summary.txt`.

When done:

```bash
kill %1 2>/dev/null
```

**Pass criteria for step 4** — the CLI output and local HTML report are
human-readable, no engineering jargon required to interpret. If you would not
feel comfortable attaching `/tmp/sw-block-report` to a support ticket, that is
a gap. The report must stay read-only: no promote, repair, delete, rebuild, or
cleanup controls.

## Step 5 — Confirm the explicit non-claims

The product is alpha and must NOT claim things it can't do. Open this file:

```bash
cat /tmp/seaweed_block/docs/operations-v1.md | less
```

(Search for "non-claim", or scroll through the Stage sections.)

**Pass criteria for step 5** — the docs explicitly say the alpha install
does NOT provide any of these:

- hosted dashboard,
- mutating admin (promote / repair / rebuild buttons),
- transparent node-loss without pod recreate (Stage 2 is the multipath case;
  Stage 3 uses pod recreate),
- physical-host loss beyond what the topology supports,
- production HA outside the tested topology,
- performance / latency SLO,
- broad multi-distro compatibility.

If any of these is missing from the docs, or worse, if the install summary
implies something the docs disclaim, that is a customer-trust risk and a
blocker.

## Step 6 — Optional: tear it down and confirm the lab is clean

```bash
bash scripts/uninstall-k8s-alpha.sh /tmp/seaweed_block
```

Then verify:

```bash
kubectl get sc | grep sw-block || echo "no sw-block StorageClass"
kubectl get deploy -A | grep sw- || echo "no sw-block deployments"
sudo iscsiadm -m session
```

**Pass criteria for step 5**:

- StorageClass `sw-block-dynamic` is gone,
- no `sw-blockmaster` / `sw-block-csi-*` / `sw-blockvolume-*` deployments,
- `iscsiadm` shows "No active sessions".

If anything is left behind, file it as a cleanup gap.

## What you can evaluate at each lab shape

| Evaluation question | Minimum lab |
|---|---|
| "Can a customer install Seaweed Block on Kubernetes with one command?" | 1 node (m02 alone) |
| "Can a customer create a useful RF≥2 volume on it?" | ≥2 nodes — the launcher needs somewhere to place each replica |
| "Does the cluster survive losing a node?" | 3 nodes (RF=3 sync-quorum recovery — Phase 18) |

If your context block says `1-node`, you can complete steps 1–5 of this
guide and answer the first question plus the first-volume path. Skip
evaluations of recovery on a
1-node lab — they are not what this lab is configured to prove. If your
context block says `3-node`, you can answer all three questions.

## Overall ship recommendation rubric

After steps 1–5 (step 6 is optional but recommended), use this:

| Outcome | Ship? |
|---|---|
| Steps 1–5 all pass, summaries/report are reader-friendly | YES — Phase 20 is a real Day-1 install-to-first-volume experience |
| Steps 1–4 pass but step 5 non-claims weak | NO — fix docs first; risk of overpromising |
| Step 1 fails (manual fixups required) | NO — the one-command claim is not real yet |
| Step 2 fails (you need help reading the summary) | NO — product summary needs an iteration |
| Step 3 fails (PVC writer/reader does not pass) | NO — first-volume claim is not real yet |
| Step 4 fails (status/report is missing or unreadable) | NO — supportability claim is not real yet |
| Steps 1–5 pass but step 6 leaves residue | YES with a known follow-up bug; cleanup is operational, not customer-blocking |

## PM result template (required fields)

Record this block in `work/test_server/pm-eval-<date>.txt` after your run.
Do not file a PM verdict without these paths.

```text
PM verdict:                           # YES / NO / YES with P1 fixes
Run date/time:
Evaluator:
Linux host:
Repo branch/commit:

activation-summary.txt path:
cluster-evidence.json path:
first-volume-summary.txt path:
status report directory:
delete-storageclass.log path:
artifact directory from step 1:

Top P0 blockers:
Top P1 friction:
Top P2 polish:
```

## What QA has already verified for you

So you know what you're stepping into:

- The runner-native first-volume scenario
  `testops/scenarios/activation-day1-first-volume-chain.yaml` was run on the
  same 3-node lab on 2026-05-17. Run id `20260517-212358-bfba`.
- It passed 5/5 phases and 27/27 actions in 42.5 seconds.
- The first-volume path verified PVC Bound, writer checksum, reader checksum,
  product `cluster-evidence.json`, inventory bundle, and strict cleanup.
- The new `status/report/` directory contained `index.html`,
  `cluster-evidence.json`, `timeline.jsonl`, and `summary.txt`.
- iSCSI sessions and `sw-block` processes were zero after cleanup.

If your manual run reproduces those numbers (and especially if step 1's
summary block reads sensibly to you), you have product-level corroboration
that the engineering close report (`activation-day1-install-validation.md`
companion in this same directory) is honest.

## When to escalate to engineering

Escalate during your run if:

- step 1's command appears to hang for more than 3 minutes (something in
  the chain is stuck),
- step 1 exits non-zero with no clear human-readable error,
- step 2's summary file is missing or empty,
- step 3's first-volume helper does not print `first_volume_status=ok`,
- step 4's `sw-block ops cluster` cannot reach the port-forward
  (check `kubectl get svc -n kube-system blockmaster` — should exist),
- step 5 surfaces a docs/install contradiction (the install summary claims
  something the docs disclaim).

Provide engineering with the artifact directory printed at the end of
step 1 (typically `/mnt/smb/work/share/g15d-k8s/...-activation-day1/`).
That directory contains every log they need for triage.
