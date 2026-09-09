# Developer CI/CD guide

This page explains how to add or change CI/CD safely in `dpone`. It belongs in Developer docs because workflow changes are implementation changes: they affect release safety, contributor UX, security posture, and public artifacts.

## Principles

- Keep the default PR gate deterministic and credential-free.
- Keep vendor credentials in manual or scheduled gates only.
- Use least-privilege workflow permissions.
- Prefer explicit artifacts over hidden logs.
- Make every new workflow reproducible locally or document why it cannot be.
- Update user docs and runbooks in the same PR as workflow changes.
- Keep `master` as the default branch in workflow triggers and docs.
- Cancel superseded work only inside one pull request; queue scheduled work in
  a separate stable group when every retained run matters.
- Treat skipped or unavailable provider evidence as `UNVERIFIED`, never PASS.
- Keep pull-request-reachable repository code read-only. Put any narrowly
  approved write/OIDC capability in a closed source-free action-only finalizer.

## When to add a new workflow

Add a workflow only when one of these is true:

| Case | Prefer |
| --- | --- |
| Fast deterministic quality check | Extend `.github/workflows/ci.yml`. |
| Documentation build/deploy behavior | Extend `.github/workflows/pages.yml`. |
| Public package publishing | Extend `.github/workflows/release.yml`. |
| Long-running source/sink or connector certification | Manual/scheduled workflow. |
| Vendor credentials required | Manual/scheduled workflow with explicit secrets and docs. |
| Security/supply-chain posture | Dedicated security workflow with least-privilege permissions. |

Do not add a new always-on PR workflow for slow, flaky, vendor-dependent, or credential-dependent tests.

## New workflow checklist

Before opening a PR:

- Choose the trigger: `push`, `pull_request`, `workflow_dispatch`, `schedule`, or tag.
- Choose minimum permissions. Start with `contents: read`.
- Decide whether the workflow must run on `master`, PRs, tags, schedule, or manual dispatch.
- Add local reproduction commands to [Workflow reference](cicd/workflows.md).
- Add failure recovery to [Failure runbooks](cicd/runbooks.md).
- Add artifact names and retention expectations.
- Add or update tests that protect the docs/workflow contract.
- Confirm no job writes secrets to logs or artifacts.
- Run the pinned changed-workflow actionlint gate and its queue-waiver removal
  tripwire.
- Confirm release-sensitive workflows are byte-identical unless a separate
  approved release change owns them.
- Run the semantic privilege scanner and treat both `FAIL` and `UNVERIFIED` as
  blocking. A write-scope allowlist entry cannot authorize a reachable route.

## Changing the default CI gate

Default CI changes affect all contributors. Keep them boring and reproducible.

Required local gate:

```bash
uv sync --locked --all-extras
uv run ruff check .
uv run ruff format --check .
uv run mypy --config-file mypy.ini
uv run pytest -m "not integration_live" --cov=src/dpone --cov-report=xml
uv build
```

Rules:

- Do not require Docker for the default quality matrix unless the job is isolated like `postgres-xmin`.
- Do not require cloud/vendor credentials.
- Keep optional integration markers opt-in.
- If coverage minimum changes, document the rationale in the PR.
- Keep project synchronization locked. A stale `uv.lock` must fail and be
  repaired in a reviewed lock change; do not replace `--locked` with
  `--frozen` or an unlocked sync.

## PR-reachable privilege checklist

Use this checklist whenever a workflow trigger, job, `needs` chain, reusable
workflow call, `workflow_run`, runner, environment, secret, permission, or
condition changes.

1. **Discover** — determine whether any route begins at `pull_request` or the
   forbidden `pull_request_target`. File-local actionlint is not a transitive
   authority proof.
2. **Prepare** — use an immutable checkout and run
   `uv sync --locked --all-extras` once. This prerequisite may access the
   network, write the environment or uv cache, and use stderr; it is outside
   the scanner process contract. No GitHub credential is required by the
   scanner.
3. **Configure** — start with `permissions: {}` or the smallest explicit read
   map. In a specified mapping omitted permissions become `none`. Do not use
   `read-all`, `write-all`, PR secrets, environments, or self-hosted runners.

   The smallest copyable PR workflow keeps repository execution read-only and
   disables persisted checkout credentials:

   ```yaml
   name: PR checks
   on:
     pull_request:
       branches: [master]
   permissions:
     contents: read
   jobs:
     check:
       runs-on: ubuntu-latest
       steps:
         - uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0
           with:
             persist-credentials: false
         - uses: astral-sh/setup-uv@37802adc94f370d6bfd71619e3f0bf239e1f3b78
           with:
             version: "0.11.28"
         - uses: actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1
           with:
             python-version: "3.12"
         - run: uv sync --locked --all-extras
         - run: uv run --no-sync pytest tests/test_runtime_optional_import_safety.py -q
   ```

   The [workflow reference](cicd/workflows.md#semantic-pr-privilege-boundary)
   distinguishes the legacy policy and
   `evals/agent/workflow-security.schema.json` from the semantic policy,
   `evals/agent/workflow-security-privileged-policy.schema.json`, and
   `evals/agent/workflow-security-privileged-report.schema.json`. Start local
   reuse from the [literal-only caller/callee pair](#calling-a-local-reusable-workflow-safely).
4. **Isolate** — repository commands, checkout, setup, install, cache, and
   scripts stay in read-only jobs. A privileged PR-route job is acceptable only
   when it is one exact mandatory closed profile.
5. **Execute** — run both public repository surfaces:

   ```bash
   uv run --locked --no-sync --offline --no-python-downloads python -B \
     tools/agent_policy/workflow_security_privileged.py \
     --root . \
     --format text
   uv run --locked --no-sync --offline --no-python-downloads python -B \
     tools/agent_policy/workflow_security.py . --format json
   ```

   These are prepared-environment convenience commands. For exact standalone
   exit and stream evidence, bypass the uv wrapper:

   ```bash
   .venv/bin/python -B tools/agent_policy/workflow_security_privileged.py \
     --root . \
     --format text
   ```

   The no-network/no-file-mutation guarantee begins when that scanner process
   starts.
6. **Observe** — for the direct scanner invocation, standalone `PASS` exits
   `0`; `FAIL` and `UNVERIFIED` emit a report on stdout and exit `1`. The
   umbrella keeps exactly `status`, `errors`, and `warnings` in JSON and maps
   every semantic non-pass into `errors` after existing errors.
7. **Diagnose and recover** — follow the finding's route, permission source,
   and `recovery_command_id` in the
   [semantic privilege runbook](cicd/runbooks.md#semantic-pr-privilege-boundary).
   Never widen the fixture or policy to hide an unknown route.
8. **Operate** — keep the standalone scan and compatibility umbrella in the
   normal workflow-control review, retain governance artifacts for 90 days,
   and investigate every profile drift or non-deterministic replay.
9. **Upgrade** — treat a CodeQL pin update as a separately reviewed closed-profile
   contract change. Synchronize the workflow, semantic policy, policy-schema
   `const`, trusted report binding, producer-owned fixtures/contracts, and
   certification evidence; run profile/schema/report/mutation tests, two
   byte-identical scans, and hosted exact-head CodeQL. Any governance finalizer
   or merge-closure fingerprint change requires a prior approved ADR 0037
   amendment; the consuming PR cannot approve its own new exception.

Safe governance evidence topology is read-only producer code followed by the
exact source-free two-action finalizer. Emergency containment may disable the
finalizer, but that deliberately leaves attestation unavailable and the
mandatory profile red. It never authorizes restoring OIDC or attestation
permissions to the `quality` job or another job that runs repository code.

## Calling a local reusable workflow safely

Closed v1 proves a local reusable call when every input is a YAML literal with
the callee's exact declared type. Save this read-only caller as
`.github/workflows/pr-report.yml`:

```yaml
name: PR reusable report
on:
  pull_request:
permissions: {}
jobs:
  report:
    name: Render report
    permissions:
      contents: read
    uses: ./.github/workflows/reusable-report.yml
    with:
      strict: true
      attempts: 2
      label: pull-request
```

Save its callee as `.github/workflows/reusable-report.yml`:

```yaml
name: Reusable report
on:
  workflow_call:
    inputs:
      strict:
        type: boolean
        required: true
      attempts:
        type: number
        default: 1
      label:
        type: string
        required: true
permissions: {}
jobs:
  report:
    permissions:
      contents: read
    runs-on: ubuntu-latest
    steps:
      - run: printf '%s %s\n' "${{ inputs.label }}" "${{ inputs.attempts }}"
```

The caller job may contain only `name`, `uses`, `with`, `secrets`, `needs`,
`if`, `concurrency`, and `permissions`. `strategy` and input expressions such
as `${{ matrix.target }}` are valid GitHub extensions but are `UNVERIFIED` in
closed v1; do not disguise expression text as a string literal. Boolean is not
a number, extra inputs are rejected, and every required input must be present.

`secrets: inherit` is rejected. An explicit named-secret map is structurally
recognized, but a secret reachable from `pull_request` is still a
`PRIVILEGE_PR_SECRET_OR_ENVIRONMENT` `FAIL`; structural acceptance never means
the route is safe. Keep PR callers secret-free, or move the operation to a
separately reviewed non-PR route.

**Execute:** after committing both files to an otherwise safe immutable
checkout, run the prepared-environment scanner from the repository root:

```bash
uv run --locked --no-sync --offline --no-python-downloads python -B \
  tools/agent_policy/workflow_security_privileged.py --root . --format text
```

**Observe:** the local report is `status=PASS` with `finding_count=0`; it
creates no file or artifact. When GitHub runs the caller, the reusable job log
contains `pull-request 2`; this example has no artifact upload or persisted
output. Local `PASS` classifies checked-out workflow bytes only and is not
hosted-run evidence.

**Recover:** for `invalid workflow_call inputs`, restore the exact literal
types and required names shown above, keep the PR caller secret-free, and rerun
the same command. Follow the
[semantic privilege runbook](cicd/runbooks.md#semantic-pr-privilege-boundary)
for the finding-specific recovery and evidence checklist; do not widen policy
or add an override.

## Choosing concurrency behavior

Use a PR-number group with conditional cancellation for superseded contributor
work. This prevents one PR from cancelling another and retains push/manual
exact-commit runs:

```yaml
concurrency:
  group: ci-${{ github.event_name == 'pull_request' && github.event.pull_request.number || github.run_id }}
  cancel-in-progress: ${{ github.event_name == 'pull_request' }}
```

Use a separate source-free wrapper for scheduled work that must queue rather
than cancel. The Airflow nightly wrapper owns the stable
`airflow-pack-compat-nightly` group, `queue: max`, and
`cancel-in-progress: false`; the reusable implementation owns the matrix.
Never combine `queue: max` with active cancellation.

Pages is different: its complete build, freshness verification, and deploy
sequence uses one PR/ref group. PR heads may cancel; non-PR runs serialize
without a queue key. Do not move concurrency down to only the deploy job,
because completion order could publish an older site last.

## Pinned actionlint for changed workflows

PR 3A validates only changed workflows in its implementation allowlist with
actionlint `1.7.12`; unrelated repository-wide findings are not reclassified.
An already installed binary is acceptable only when the first line is exact:

```bash
test "$(actionlint -version | sed -n '1p')" = "1.7.12"
```

Otherwise download the official archive for the local platform and verify it
before extracting. These are the approved release bytes:

| Platform | Archive | SHA-256 |
| --- | --- | --- |
| Linux amd64 | `actionlint_1.7.12_linux_amd64.tar.gz` | `8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8` |
| Darwin arm64 | `actionlint_1.7.12_darwin_arm64.tar.gz` | `aba9ced2dee8d27fecca3dc7feb1a7f9a52caefa1eb46f3271ea66b6e0e6953f` |

CI additionally streams only the `actionlint` archive member into a new
`$RUNNER_TEMP/actionlint-1.7.12` directory and verifies Linux binary size
`6074530` plus SHA-256
`c872d6db8c6bf83a8eaa704fc93999f027d55dffbc63b8a6abdccb47df5f4cd4`
before execution. It never extracts unrelated members or overwrites a repository
path.

Run the non-queue paths together with no ignore:

```bash
actionlint \
  -no-color -format '{{json .}}' -shellcheck '' -pyflakes '' \
  .github/workflows/ci.yml \
  .github/workflows/airflow-pack-compat.yml \
  .github/workflows/pages.yml \
  .github/workflows/dependency-review.yml
```

This invocation must exit `0`, write exactly `[]` followed by one newline to
stdout, and leave stderr empty. The nightly wrapper is the only path allowed a
temporary diagnostic waiver while released 1.7.12 lacks
`concurrency.queue` support:

```bash
actionlint \
  -no-color -format '{{json .}}' -shellcheck '' -pyflakes '' \
  -ignore '^unexpected key "queue" for "concurrency" section\. expected one of "cancel-in-progress", "group"$' \
  .github/workflows/airflow-pack-compat-nightly.yml
```

The same file must first be run without `-ignore`: require exit `1`, empty
stderr, and one exact `syntax-check` object for the original filepath/message.
The waived run must exit `0` with exact `[]\n` stdout and empty stderr. Extra
diagnostics or stream bytes fail the gate.

When a pinned released actionlint understands `queue`, the unwaived run exits
`0`. Treat that as `ACTIONLINT_QUEUE_WAIVER_OBSOLETE`; remove the ignore and the
old expected-error branch in the same approved update. A version upgrade changes
the version, applicable archive checksums, source URL, and version assertion
atomically. Floating `latest`, an upstream branch, or an unreleased commit is
not allowed.

## CI-hygiene release boundary

Changes to ordinary CI do not authorize edits to release behavior. Keep these
workflows byte-identical to the approved implementation base unless a separate
release specification owns the change:

- `.github/workflows/release.yml`;
- `.github/workflows/runtime-image.yml`;
- `.github/workflows/certification-release-summary.yml`;
- `.github/workflows/route-certification-release.yml`;
- `.github/workflows/route-release-finalize.yml`.

`uv build` and isolated `uv pip install --python ...` commands are not project
synchronization; do not mechanically rewrite them. Preserve required-check
names, protection, release credentials, tags, environments, and publication
semantics.

## Adding a manual integration gate

Use this pattern for broad matrix, stress, benchmark, or connector certification workflows:

```yaml
on:
  workflow_dispatch:
    inputs:
      run_mode:
        type: choice
        options: [mock_contract, mock_local, vendor_live]

permissions:
  contents: read
```

Design requirements:

- All input names must be documented.
- Every run writes artifacts even on failure with `if: always()`.
- `mock_contract` and `mock_local` must not require external vendor credentials.
- `vendor_live` must clearly list required secrets.
- Use focused filters such as `DPONE_MATRIX_CASE_ID` for debugging.

## Adding secrets

Before adding a secret:

1. Ask whether Trusted Publishing, OIDC, or local mock mode can avoid the secret.
2. Scope the secret to the smallest provider/project/package possible.
3. Use it only in the workflow/job that needs it.
4. Redact diagnostics and artifacts.
5. Add rotation instructions to the relevant runbook.

Never add secrets to ordinary PR CI from forks.

## Adding artifacts

Artifacts should answer: what ran, with what inputs, what passed/failed, and what evidence exists.

Recommended artifact content:

- command and workflow inputs;
- source/sink/strategy coverage;
- row counts and checksums for data movement tests;
- warnings and skipped cases with reason;
- environment summary without secrets;
- JUnit XML when pytest is involved.

Recommended paths:

```text
test_artifacts/integration_matrix/
test_artifacts/connectors/
test_artifacts/benchmarks/
test_artifacts/route_certification/
test_artifacts/route_certification_release/
test_artifacts/route_release_finalize/
test_artifacts/route_readiness/
test_artifacts/route_execution/
test_artifacts/route_rc_execution/
test_artifacts/route_state/
test_artifacts/cdc_apply/
test_artifacts/cdc_handoff/
test_artifacts/cdc_schema/
test_artifacts/cdc_schema_apply/
test_artifacts/cdc_promotion/
test_artifacts/cdc_runtime/
test_artifacts/cdc_poison_quarantine/
test_artifacts/cdc_compare_repair/
test_artifacts/certification/suite/
test_artifacts/observability/
test_artifacts/supply-chain/
```

## Adding certification suite automation

Use [Certification suite automation](certification-suite.md) when a workflow
needs to combine matrix correctness, benchmark regression, lineage, dbt lineage,
and evidence checksums.

Recommended flow:

```bash
uv run dpone ops certification-run \
  --artifact-dir test_artifacts/certification/current

uv run dpone ops benchmark-baseline \
  --output-dir test_artifacts/benchmarks/current \
  --metrics-json "$METRICS_JSON" \
  --baseline-json "$BASELINE_JSON"

uv run dpone ops certification-suite \
  --output-dir test_artifacts/certification/suite \
  --suite-id "$GITHUB_RUN_ID" \
  --certification-report test_artifacts/certification/current/certification_report.json \
  --benchmark-baseline test_artifacts/benchmarks/current/benchmark_baseline.json \
  --require-benchmark \
  --format json
```

Rules:

- Keep this workflow manual or scheduled unless it is credential-free and fast.
- Upload `test_artifacts/certification/suite/` with `if: always()`.
- For critical source -> sink routes, generate route evidence first and then run
  `uv run dpone ops route-certification-pack`; upload
  `test_artifacts/route_certification/` and any embedded
  `test_artifacts/route_readiness/` outputs with `if: always()`.
- For new or changed source -> sink onboarding UX, run
  `uv run dpone ops connection-doctor`,
  `uv run dpone ops source-discover`,
  `uv run dpone ops route-bootstrap`, and
  `uv run dpone ops route-doctor` against credential-free schema fixtures.
  Upload `test_artifacts/onboarding/` with `if: always()` and treat
  `route_doctor.json` as the pre-readiness user-facing go/no-go artifact; see
  [Route bootstrap and doctor](route-bootstrap-doctor.md).
- For route acceptance changes, run
  `uv run dpone ops route-conformance run` for each changed
  `source -> sink -> strategy` pair and then
  `uv run dpone ops route-conformance release-gate` over the produced
  artifacts. Upload `test_artifacts/route_conformance/` with `if: always()`.
  Ordinary OSS CI can use deterministic credential-free synthetic datasets;
  Docker-live/vendor-live jobs should attach live evidence separately. See
  [Route Conformance Lab](route-conformance-lab.md).
- For protected Docker-live route acceptance, run
  `DPONE_VENDOR_LIVE=1 DPONE_LIVE_ROUTE_CONFORMANCE=1 uv run dpone ops route-conformance live-run`
  with `--adapter vendor_live` or `--adapter docker` against disposable
  Postgres, MSSQL, and ClickHouse services. The built-in protected bindings
  cover `postgres -> mssql` and `mssql -> clickhouse` for
  `incremental_merge`. Upload `route_conformance_live.json`,
  `live_source_snapshot.json`, `live_sink_snapshot.json`, and the embedded
  `route_conformance.json` with `if: always()`. Keep the default adapter
  `in_memory` for ordinary OSS CI and register real Postgres, MSSQL, or
  ClickHouse adapters only in opt-in jobs.
- For route release candidates, run `uv run dpone ops route-rc-execute`
  without `--execute` first and upload `route_rc_execution.json`. Add
  `--execute` only behind an explicit manual workflow input and after Docker or
  vendor-live services are ready.
- When retry/resume, state commit, fencing, repair, resync, or route execution
  ordering changes, run `uv run dpone ops route-execution-ledger` for the
  affected route and upload `test_artifacts/route_execution/` with
  `if: always()`. The required route readiness evidence is
  `route_execution_ledger`. Use `--store-backend sqlite --store-uri
  test_artifacts/route_execution/shared/route_execution_ledger.sqlite3` when a
  workflow has more than one local runner or needs atomic compare-and-swap lease
  fencing evidence; see
  [Route execution ledger](route-execution-ledger.md).
- When source state, offset, xmin, LSN, cursor, or checkpoint promotion changes,
  run `uv run dpone ops route-state-promote` after route execution ledger
  evidence and upload `test_artifacts/route_state/` with `if: always()`. The
  required route readiness evidence is `state_promotion`. Use
  `--state-backend sqlite --state-uri
  test_artifacts/route_state/shared/route_state_store.sqlite3` when a workflow
  has more than one local runner or needs atomic compare-and-swap state evidence;
  see [Route state promotion](route-state-promotion.md).
- For route release candidates, run `uv run dpone ops route-release-gate` after
  `route_readiness.json`, `route_certification_pack.json`,
  `route_execution_ledger.json`, `state_promotion.json`, and any route-specific
  benchmark/schema/CDC evidence exist. Upload `test_artifacts/route_release/`
  with `if: always()` and treat `route_release_gate.json` as the final
  route-scoped go/no-go artifact; see
  [Route release gate](route-release-gate.md).
- For Docker-live or vendor-live route release candidates, run
  `uv run dpone ops route-live-certification` after the upstream live checks
  have produced route evidence. Upload `test_artifacts/route_live/` with
  `if: always()` and pass the live bundle to `route-release-gate`:

  ```bash
  --artifact route_live_evidence_bundle=<route_live_certification.json> \
  --require route_live_evidence_bundle
  ```

  See
  [Route live certification](route-live-certification.md).
- For route releases, run `uv run dpone ops route-certify` after refresh
  execution, snapshot capture, exact verification, readiness, checklist, and
  evidence-chain artifacts exist. Use `--profile oss_ci` in ordinary CI and
  `--profile vendor_live` only in opt-in Docker-live/vendor-live jobs that also
  attach `route_live_evidence_bundle`. Upload `test_artifacts/route_certify/`
  with `if: always()` and treat `route_certification_bundle.json` as the final
  route promotion artifact; see [Route certify](route-certify.md).
- For release candidates that claim first-class route certification, run
  `uv run dpone ops route-certify-release` after every required route has a
  `route_certification_bundle.json`. Upload
  `test_artifacts/route_certification_release/` with `if: always()` and treat
  `route_certification_release.json` as the release-level go/no-go artifact;
  see [Route certify release](route-certify-release.md).
- For final route-certified release review, run
  `uv run dpone ops route-release-finalize` after route bundles are in a stable
  bundle root. Upload `test_artifacts/route_release_finalize/` with
  `if: always()` and treat `route_release_finalizer.json` plus
  `route_certification_history_index.json` as the final route release evidence;
  see [Route release finalize](route-release-finalize.md).
- For route release candidates that need the full train in one receipt, run
  `uv run dpone ops route-rc-orchestrator`. Upload
  `test_artifacts/route_rc/` with `if: always()` and treat
  `route_rc_orchestration.json` plus the nested `release_evidence_pack.json` as
  the review bundle; see
  [Route release candidate orchestrator](route-rc-orchestrator.md).
- For release candidates that merge a stacked train, export every PR with
  `gh pr view --json number,title,baseRefName,headRefName,state,mergeStateStatus,isDraft,url,statusCheckRollup`,
  run `uv run dpone ops release-rc-collect`, and then run the generated
  `uv run dpone ops release-rc-finalize` command after route, docs, package,
  and release evidence exists. Upload `test_artifacts/release_rc_collect/` and
  `test_artifacts/release_rc_finalizer/` with `if: always()` and block tags
  unless `release_rc_finalizer.json` is `rc_ready`; see
  [Release RC collector](release-rc-collector.md) and
  [Release RC finalizer](release-rc-finalizer.md).
- For CDC promotion routes, generate or export a credential-free fixture, run
  `uv run dpone ops cdc-apply-certification`, then run or review the embedded
  `uv run dpone ops cdc-handoff` report; upload `test_artifacts/cdc_apply/`
  with `if: always()`.
- When CDC evidence is produced by another specialized job, generate snapshot
  boundary, CDC window, retention, apply correctness, delete semantics, typed
  hash, and schema drift artifacts first, then run `uv run dpone ops cdc-handoff`; upload
  `test_artifacts/cdc_handoff/` with `if: always()`.
- When CDC telemetry or SLO behavior changes, run
  `uv run dpone ops cdc-observability-evidence` with the handoff JSON, apply
  certification JSON, metrics JSON, and optional SLO profile; upload
  `test_artifacts/cdc_observability/` with `if: always()`.
- When CDC recovery behavior changes, run
  `uv run dpone ops cdc-recovery-evidence` with handoff, apply,
  observability, scenario, and optional policy JSON; upload
  `test_artifacts/cdc_recovery/` with `if: always()`.
- When CDC schema evolution or target DDL governance changes, run
  `uv run dpone ops cdc-schema-evolution-evidence` with handoff, apply,
  observability, recovery, schema-change, and optional policy JSON; upload
  `test_artifacts/cdc_schema/` with `if: always()`.
- When CDC target DDL apply or typed serving refresh behavior changes, run
  `uv run dpone ops cdc-schema-apply --mode dry_run` and, for approved local
  Docker routes, `uv run dpone ops cdc-schema-apply --mode apply`; upload
  `test_artifacts/cdc_schema_apply/` with `if: always()`; see
  [CDC schema apply](cdc-schema-apply.md). The opt-in live check must verify
  additive column DDL, backfill SQL, `cdc_schema_apply_result.json`, and
  `typed_refresh/cdc_typed_materialization.json` when `--typed-refresh` is set.
- When a CDC stream is ready for release or environment promotion, run
  `uv run dpone ops cdc-promotion-gate` with apply, handoff, observability,
  recovery, and schema evolution JSON; upload `test_artifacts/cdc_promotion/`
  with `if: always()`. The gate records `production_ready` and
  `promote_offsets`; it does not promote offsets itself.
- When runtime CDC apply behavior changes, run
  `uv run dpone ops cdc-runtime-run` with a bounded events JSON and local
  checkpoint JSON; upload `test_artifacts/cdc_runtime/` with `if: always()`.
  This verifies that offsets commit only after durable sink apply.
- When CDC poison classification, quarantine, or replay behavior changes, run
  `uv run dpone ops cdc-runtime-run --poison-mode quarantine_and_continue`,
  `uv run dpone ops cdc-quarantine-inspect`, and
  `uv run dpone ops cdc-replay-execute`; upload
  `test_artifacts/cdc_poison_quarantine/` with `if: always()`. See
  [CDC poison quarantine and replay](cdc-poison-quarantine.md). The check must
  prove `cdc_poison_quarantine.json`, `cdc_quarantine_inspection.json`,
  `cdc_replay_execution.json`, `duplicate_events_skipped`, and no offset
  mutation during replay.
- When CDC compare, repair planning, or repair execution changes, run
  `uv run dpone ops cdc-compare-repair` and, for approved bounded plans,
  `uv run dpone ops cdc-repair-execute`; upload
  `test_artifacts/cdc_compare_repair/` with `if: always()`. See
  [CDC compare and repair](cdc-compare-repair.md). The check must prove
  `cdc_compare_repair.json`, `cdc_repair_plan.json`,
  `cdc_repair_execution.json`, source-to-ClickHouse CDC log current-state
  comparison, and no offset mutation during repair.
- When CDC retention gap auto-resync changes, run
  `uv run dpone ops cdc-retention-check`, `uv run dpone ops cdc-resync-plan`,
  and, for approved bounded plans, `uv run dpone ops cdc-resync-execute`;
  upload `test_artifacts/cdc_retention_resync/` with `if: always()`. See
  [CDC retention gap auto-resync](cdc-retention-resync.md). The check must
  prove `cdc_retention_check.json`, `cdc_resync_plan.json`,
  `cdc_resync_actions.json`, `cdc_resync_execution.json`, Docker-live
  `mssql -> clickhouse` probe coverage when credentials are available, and no
  offset mutation during resync.
- When live CDC runtime adapters change, run the local smoke above and an
  opt-in vendor-live `uv run dpone ops cdc-runtime-run --mode live` check for
  `mssql -> clickhouse`; see [CDC live runtime adapters](cdc-live-runtime-adapters.md).
  The live check must verify SQL Server read, ClickHouse apply, and no offset
  commit on non-durable sink receipts.
- When ClickHouse CDC serving materialization changes, run
  `uv run dpone ops cdc-materialize-clickhouse` against a fixture or opt-in
  Docker route and upload `test_artifacts/cdc_materialization/` with
  `if: always()`; see [ClickHouse CDC materialization](cdc-clickhouse-materialization.md).
  The check must verify active-row and tombstone delete modes.
- When ClickHouse CDC typed serving materialization changes, run
  `uv run dpone ops cdc-materialize-clickhouse-typed` against a fixture or
  opt-in Docker route and upload `test_artifacts/cdc_typed_materialization/`
  with `if: always()`; see
  [ClickHouse CDC typed materialization](cdc-clickhouse-typed-materialization.md).
  The check must verify declared column types, active-row mode, tombstone mode,
  `--fail-on-parse-errors`, `--schema-drift-mode strict`, and
  `cdc_typed_parse_quarantine.json` for malformed payloads.
- Upload `test_artifacts/observability/` with `if: always()` when a workflow exports runtime metrics.
- Add required secrets only to vendor-live jobs.
- Update [Developer certification suite](developer-certification-suite.md) when adding evidence types.

## Adding observability artifacts

Use [Runtime observability](observability.md) when a workflow needs
Prometheus/OpenTelemetry evidence for a run.

Recommended flow:

```bash
uv run dpone run "$MANIFEST" --format json > test_artifacts/runs/run_report.json

uv run dpone observability metrics-export \
  --run-report test_artifacts/runs/run_report.json \
  --output-dir test_artifacts/observability/current \
  --label ci_run_id="$GITHUB_RUN_ID" \
  --label branch="$GITHUB_REF_NAME" \
  --format json
```

Rules:

- Keep labels secret-free and low-cardinality.
- Keep collector upload optional; artifact generation must work locally.
- Link any new failure mode to [Failure runbooks](cicd/runbooks.md).
- Update [Developer observability guide](developer-observability.md) when adding renderers or metric sources.

## Adding supply-chain evidence

Use [Supply-chain evidence](supply-chain.md) when a release workflow needs SBOM,
provenance, signature envelope, and checksum evidence.

Recommended flow:

```bash
uv build
uv run twine check dist/*

subjects=()
for artifact in dist/*.whl dist/*.tar.gz; do
  subjects+=(--subject "$artifact")
done

uv run dpone supply-chain attest \
  --release "$GITHUB_REF_NAME" \
  "${subjects[@]}" \
  --repository "$GITHUB_SERVER_URL/$GITHUB_REPOSITORY" \
  --commit-sha "$GITHUB_SHA" \
  --builder-id "github-actions:$GITHUB_RUN_ID" \
  --signing-key "$DPONE_LOCAL_ATTESTATION_KEY" \
  --signing-key-id github-actions \
  --output-dir test_artifacts/supply-chain/current \
  --format json

mkdir -p test_artifacts/supply-chain/github-attestations
for artifact in dist/*.whl dist/*.tar.gz; do
  gh attestation verify "$artifact" \
    --repo "$GITHUB_REPOSITORY" \
    --source-ref "$GITHUB_REF" \
    --source-digest "$GITHUB_SHA" \
    --format json \
    > "test_artifacts/supply-chain/github-attestations/$(basename "$artifact").github-attestation.json"
done
```

Rules:

- Upload `test_artifacts/supply-chain/` with `if: always()`.
- Keep `DPONE_LOCAL_ATTESTATION_KEY` in the release environment secrets and fail
  the release when it is missing.
- Do not store local HMAC signing keys in artifacts or logs.
- Use and verify GitHub Artifact Attestations or Sigstore/cosign for public
  identity-backed trust before PyPI publication.
- Run `uv run python tools/agent_policy/governance_gate.py --base-ref origin/master --output test_artifacts/agent-policy/agent_governance_gate.json`
  when CI/CD, agent policy, release evidence, or supply-chain controls change.
- Pull-request CI additionally passes the trusted event head through
  `--head-commit`. This keeps the signed receipt bound to reviewed head `H`
  while GitHub attestation truthfully retains the synthetic merge commit `M`
  from `refs/pull/<current PR number>/merge` as provenance.
- Update [Developer supply-chain guide](developer-supply-chain.md) when adding formats or signers.

## Docs contract tests

When CI/CD documentation changes, add or update docs contract tests. A good test verifies:

- public workflow files are named in docs;
- default branch remains `master`;
- runbooks mention every required workflow;
- developer docs link to CI/CD implementation guidance.

## PR description template for CI/CD changes

```markdown
## CI/CD change

- Workflow(s):
- Trigger(s):
- Permissions:
- Secrets required:
- Artifacts produced:
- Local reproduction:
- Failure runbook updated:
- Docs updated:
```

## Related docs

- [CI/CD index](ci-cd.md)
- [Workflow reference](cicd/workflows.md)
- [Failure runbooks](cicd/runbooks.md)
- [Release and Pages automation](cicd/release-and-pages.md)
- [Testing](testing/overview.md)
- [Connector certification](connector-certification.md)
