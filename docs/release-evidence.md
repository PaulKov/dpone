# Release evidence

This page documents diagnostic evidence packs, route/production certification,
and the legacy source-repository paired-tag campaign. It is not the ordinary
PyPI publication gate. Use [Release](release.md) for the active external
controller and [Agent release protocol](agent-release-protocol.md) for scoped
readiness. Publication observation, source readiness, and production
certification are distinct claims; one does not prove the others.

## Contents

- [When it is required](#when-it-is-required)
- [Diagnostic pack contract](#diagnostic-pack-contract)
- [Canonical pre-tag workflow](#canonical-pre-tag-workflow)
- [Diagnostic CLI](#diagnostic-cli)
- [Pre-release checklist](#pre-release-checklist)
- [Runbook](#runbook)
- [Developer notes](#developer-notes)

## When it is required

Select evidence from the changed scope and the certification claim. Normal
source PR requirements and exact-commit merge evidence remain mandatory for
readiness. Documentation-only patches need normal CI/docs checks; changed
runtime/data paths additionally need appropriate focused proof. Do not make
every diagnostic pack or unfinished CI-shadow task a PyPI publication blocker.
The external controller does not run the campaign described below, and a
successful upload does not certify these domains.

| Certification profile | Release evidence role |
| --- | --- |
| `local_live` | Feature-level disposable-service confidence before broader release evidence is needed. |
| `real_local` | Behavioral baseline for minor and major release candidates. The raw workflow retains JUnit-derived local evidence while release-only assemblers stay disabled. Separate CLI performance/state/pack artifacts remain diagnostic. The canonical fixed `native_transfer` campaign subsumes this baseline. |
| `native_transfer` | Route-heavy certification profile. The `Release candidate evidence` workflow executes it as a strict superset of `real_local`, adding both critical native routes, CDC/state/reconciliation, and observed benchmark evidence. It is not the controller's ordinary upload gate. |
| `vendor_live` | Optional managed/provider proof when a release changes BigQuery, managed APIs, external Kafka, or other credentialed systems. |
| `connector_certification_schedule` | Async scheduled release-evidence gate for connector confidence. Record the workflow run ID, status, and artifacts in the release review when connector behavior is relevant. |

### Legacy tagged-release contract

The following release-type table and pre-tag ordering describe the legacy
source-workflow campaign, not the ordinary PyPI publication gate. Its historical
minor/major rule must not be applied automatically to the new controller path.
The detailed source workflows still exist; do not infer that they are disabled.

| Legacy release type | Required gate in that campaign |
| --- | --- |
| Every release, including docs-only patches | Exact-commit `agent_pr_merge_receipt.json`, preserved source archive, and the [merge-receipt runbook](agent-pr-merge-receipt-runbook.md) reconciliation. |
| Patch with docs-only changes | Default CI plus docs strict gate. |
| Patch with runtime/CLI/connector changes | Default CI plus focused live or matrix evidence. |
| Minor release | Provider-bound `Release candidate evidence` PASS for the exact commit. Route-heavy releases use its fixed `native_transfer` profile. |
| Major release | Provider-bound `Release candidate evidence` PASS for the exact commit; add `vendor_live` where external providers are affected. |

For that legacy minor-release campaign, do not tag or publish until the newest
exact-commit `Release candidate evidence` dispatch is successful and
its unique, unexpired artifact validates. "Newest" means the unique maximum
GitHub `created_at` among exact workflow dispatches, selected before the
current attempt or status is inspected. A green standalone
`release_evidence_pack.json` is
diagnostic and cannot by itself authorize a tag.

The manual live-certification workflow does not synthesize release evidence.
If performance, state/reconciliation, checklist, evidence-chain, or packaging
producers were not executed, record those domains as `UNVERIFIED`. A plan,
placeholder, missing file, caller-supplied metric, checklist boolean, or
hand-authored JSON object cannot satisfy a gate.

## Diagnostic pack contract

This legacy/general-purpose pack contract remains useful for local composition
and route-scoped review. It is not the provider-bound publication authority.

`dpone ops release-evidence-pack` defaults to these required artifacts:

| Artifact name | Producer | Purpose |
| --- | --- | --- |
| `service_markers` | local service marker tests | Confirms the explicitly selected Postgres, MySQL, MSSQL, and Kafka marker cases passed. Starting ClickHouse, Schema Registry, or MinIO is not itself certification; claim those services only through their separate exact-route evidence. |
| `certification_pack` | `dpone ops certification-pack` | Source -> sink matrix, observability, performance, and state/reconciliation evidence summary. |
| `performance_certification` | `dpone ops performance-certification` | Throughput, duration, memory, and failure-rate thresholds. |
| `live_state_reconciliation` | `dpone ops live-state-reconciliation` | State backends, XMin/Kafka/CDC state checks, and physical-delete reconciliation proof. |
| `pre_release_checklist` | `dpone ops pre-release-checklist` | CLI, run, Python API, lineage, matrix, contracts, docs, and package release checklist. |
| `evidence_chain` | `dpone ops evidence-chain` and `dpone ops evidence-chain-verify` | Tamper-evident artifact checksum chain. |
| `agent_governance_gate` | `tools/agent_policy/governance_gate.py` | Agent-control, release-attestation, Scorecard, and SLSA self-assessment receipt for governance or workflow changes. |
| `github_attestation_receipts` | GitHub CLI in `.github/workflows/release.yml` | Per-artifact GitHub Artifact Attestation verification receipts for wheel and source distributions. |

Additional release gates can be attached with repeated `--artifact name=path`
and made mandatory with repeated `--require name`.

Critical source -> sink release candidates should also attach the route-level
live bundle:

```bash
: "${DPONE_WIDE_CERT_ROOT:?export the exact mktemp root from the wide certification how-to}"
dpone ops release-evidence-pack \
  --release vX.Y.Z-rc1 \
  --profile native_transfer \
  --artifact service_markers=test_artifacts/live_certification/service_markers.json \
  --artifact certification_pack=test_artifacts/live_certification/certification-pack/connector_certification_pack.json \
  --artifact performance_certification=test_artifacts/live_certification/performance-certification/performance_certification.json \
  --artifact live_state_reconciliation=test_artifacts/live_certification/live-state-reconciliation/live_state_reconciliation.json \
  --artifact pre_release_checklist=test_artifacts/live_certification/pre-release/pre_release_checklist.json \
  --artifact evidence_chain=test_artifacts/live_certification/evidence-chain/evidence_chain_index.json \
  --artifact strategy_certification_bundle=test_artifacts/strategy_certification/mssql_clickhouse/strategy_certification_bundle.json \
  --artifact route_live_evidence_bundle=test_artifacts/route_live/mssql_to_clickhouse/route_live_certification.json \
  --artifact agent_pr_merge_receipt=test_artifacts/release/vX.Y.Z-rc1/agent_pr_merge_receipt.json \
  --require service_markers \
  --require certification_pack \
  --require performance_certification \
  --require live_state_reconciliation \
  --require pre_release_checklist \
  --require evidence_chain \
  --require strategy_certification_bundle \
  --require route_live_evidence_bundle \
  --require agent_pr_merge_receipt \
  --output-dir test_artifacts/live_certification/release-evidence \
  --format json
```

When the route release candidate needs one reproducible release train, use
`dpone ops route-rc-orchestrator` and attach both `route_rc_orchestration.json`
and the nested `release_evidence_pack.json`.
Run `dpone ops route-rc-execute` against that orchestration receipt to create
`route_rc_execution.json`; dry-run receipts are acceptable for ordinary OSS CI,
while `--execute` receipts are required when the release review claims a
Docker-live command train was actually run.

## Canonical pre-tag workflow

**Legacy tagged-release contract.** This anchor is retained for existing
route/certification references. The instructions through the next section
describe that campaign and its historical PyPI mutation design, not current
controller operation. Do not execute them to repair an ordinary PyPI release.
The source workflow no longer uploads to PyPI; the external publisher builds
its own archives and has no automatic handoff or `skip-existing` recovery.
Use [the current runbook](release.md) for publication and retrospective checks.

Before merging the release-source PR, ensure its immutable `Agent PR receipt`
check is successful on the reviewed head. The merge-closure receipt verifies
that pre-merge fact and cannot be reconstructed after an admin merge; a
missing receipt requires a new, reviewed integration commit before tagging.

After the corrective release PR is merged, use the new integration commit, not
the earlier release PR commit:

```bash
release_tag=vX.Y.Z
git fetch origin master --tags
release_commit="$(git rev-parse origin/master)"

gh workflow run release-candidate-evidence.yml \
  --repo PaulKov/dpone \
  --ref master \
  -f commit_sha="${release_commit}" \
  -f release="${release_tag}"
```

The dispatch fails unless input `commit_sha`, workflow `GITHUB_SHA`,
checked-out `HEAD`, and current `refs/heads/master` are the same full SHA.
The fixed profile is `native_transfer`; it is not a caller-selectable way to
shrink the campaign. Wait for the fixed terminal job/check
`Release candidate evidence` and record its run ID and attempt. The successful
job uploads exactly one closed artifact named
`release-candidate-evidence-<commit>-<run-id>-<attempt>` containing:

The exhaustive PostgreSQL→MSSQL wide certification is deliberately separate
from this package-tag gate. It remains available through its manual workflow
dispatch and the nightly schedule, where it retains its complete fail-closed
suite and provider-bound artifact. A failed or unavailable wide campaign must
be investigated, but it does not block publishing a package whose exact-SHA
`native_transfer` candidate is green.

- `release_candidate_evidence_receipt.json`;
- `release_candidate_evidence_manifest.json`;
- `release_candidate_evidence_pack.json`;
- `release_candidate_evidence_exit_code.txt`;
- the exact observed inputs below `sources/` that the manifest names.

Required source roles are fixed by repository policy:

| Role | Closed artifact path |
| --- | --- |
| `exact_commit_checks` | `sources/preflight/exact_commit_checks.json` |
| `merge_receipt` | `sources/preflight/exact_commit_merge_receipt.json` |
| `service_markers` | `sources/live/service_markers.json` |
| `mysql_route_cells` | `sources/live/mysql/mysql_local_route_cells.json` |
| `native_transfer_fixtures` | `sources/live/native_transfer_live_fixtures.json` |
| `mssql_clickhouse_junit` | `sources/live/refresh-executor/mssql-clickhouse/junit_evidence.json` |
| `mssql_clickhouse_execution` | `sources/live/refresh-executor/mssql-clickhouse/route_refresh_execution.json` |
| `mssql_clickhouse_verification` | `sources/live/refresh-executor/mssql-clickhouse/route_refresh_verification.json` |
| `postgres_mssql_junit` | `sources/live/refresh-executor/postgres-mssql/junit_evidence.json` |
| `postgres_mssql_execution` | `sources/live/refresh-executor/postgres-mssql/route_refresh_execution.json` |
| `postgres_mssql_verification` | `sources/live/refresh-executor/postgres-mssql/route_refresh_verification.json` |
| `cdc_state_junit` | `sources/live/cdc-state/junit_evidence.json` |
| `stress_benchmark` | `sources/live/benchmarks/postgres_mssql_native_fast_path.json` |

The `exact_commit_checks.policy_sha256` field preserves the exact-commit gate
producer contract: exactly 64 lowercase hexadecimal characters without a
`sha256:` prefix. This field-specific representation is not a general digest
relaxation; authority, provider, manifest, archive, and receipt digests retain
their tagged `sha256:<64 lowercase hex>` contracts.

The reusable live job also retains raw diagnostic artifact
`release-candidate-live-<commit>-<run-id>-<attempt>`. Do not use that raw
archive as publication evidence; both tag consumers select only the closed
`release-candidate-evidence-...` authority artifact.

The receipt binds repository, commit, proposed release, profile, workflow,
run/attempt, policy and manifest digest. The manifest closes the source paths,
sizes and SHA-256 digests. Artifact retention is availability, not permanent
durability: `retention-days: 90` is only a request and repository policy may
cap it. Provider-reported expiry, deletion, or unavailable bytes change the
decision to `UNVERIFIED` and require a new complete dispatch.

List the matching runs, then watch the selected run to terminal status:

```bash
gh run list \
  --repo PaulKov/dpone \
  --workflow release-candidate-evidence.yml \
  --branch master \
  --commit "${release_commit}" \
  --event workflow_dispatch \
  --limit 10
evidence_run_id=123456789  # replace with the selected numeric run ID
gh run watch "${evidence_run_id}" --repo PaulKov/dpone --exit-status
```

Confirm the run corresponds to the proposed release and record its run ID and
attempt in the release review. The publishing preflights still reselect and
verify provider state; this operator observation is not a substitute. Do not
pick a dispatch by the highest run or check ID. Before tagging, inspect every
exact-SHA dispatch of the fixed workflow and require the unique maximum
`created_at` to pass; equal creation times fail closed. A newer queued,
running, cancelled, failed, or malformed pre-tag dispatch therefore blocks
every older PASS, and rerunning an older dispatch cannot make it newer.

Only after that exact check and artifact are green may the annotated tag be
created on `release_commit`. Advancing `master` afterward does not invalidate
the frozen commit's evidence, but the evidence cannot authorize a different
tagged commit. Both `.github/workflows/release.yml` and
`.github/workflows/runtime-image.yml` independently select the newest eligible
exact-SHA dispatch and validate the same artifact. Eligibility is frozen by
the paired tag-run cutoff described below. They never fall back to an older
eligible PASS after a newer eligible pending, failed, cancelled, timed-out,
malformed, missing, or expired execution for that commit.

The tag push must create exactly one provider run for each publication
workflow. Every gate authenticates both runs against the exact repository,
workflow path, `push` event, tag, and commit. It also authenticates its own
positive `GITHUB_RUN_ID` and `GITHUB_RUN_ATTEMPT`, requiring the current run to
remain `in_progress` with no conclusion. The earlier GitHub `created_at` of the
two tag-push runs is the shared publication cutoff. Only exact-SHA evidence
dispatches with provider `created_at` no later than that cutoff are eligible.
The gate selects the unique maximum `created_at` within that set, then reads
that dispatch's current attempt and status. The selected run must also have
completed, its terminal check must have completed, and its authority artifact
must have been created no later than the cutoff. Equal eligible creation times
fail closed. A post-cutoff dispatch is ignored and cannot supersede or deny
the frozen tag. The annotated tagger timestamp is metadata only and is never
release authority.

GitHub may index the two tag-triggered runs at slightly different times. The
verifier handles only that benign absence with bounded semantic polling: one
initial observation plus at most 30 ten-second sleeps, for 31 observations and
about 300 seconds of sleep budget, excluding request time. Each observation
re-fetches and authenticates the
caller's exact current run detail and re-lists both fixed workflow paths. Runs
for another tag are ignored. It retries only when a path has zero exact-tag
matches or the current run's mutable `run_attempt`/`updated_at` list projection
lags its authenticated detail. Persistent mutable drift fails after the same
bound. A malformed exact match, more than one exact match, or immutable
`id`/`created_at` drift fails immediately.

The package workflow cannot be dispatched manually. Both workflows run a
read-only preflight and then a fresh verification inside every privileged
mutation block, immediately before that block's first irreversible write:
release artifact attestation, PyPI upload, GitHub Release creation, GHCR
digest/attestation publication, and GHCR alias promotion. No repository or
untrusted command runs between a fresh gate and the block's first write. A
multi-write block does not gate every attestation separately: later attest
writes stay inside the same block, with no intervening checked-out repository
code able to supersede the frozen evidence authority.

For PyPI, the fresh provider check is followed by a distinct public-state gate
immediately before Trusted Publishing. It rehashes the exact closed candidate
set and accepts public state only when all four exact-version endpoints are
absent or expose an exact non-yanked candidate subset by filename, SHA-256, and
size. Its deterministic receipt binds the GitHub release attempt, raw candidate
inventory digest, endpoint observations, and per-file classifications. This
machine PASS is the only authority for the publisher's `skip-existing`
recovery behavior; an operator comparison cannot substitute for it.

Tag the frozen candidate explicitly rather than whichever commit happens to be
checked out:

```bash
git tag -a "${release_tag}" "${release_commit}" -m "Release ${release_tag}"
git push origin "${release_tag}"
```

## Diagnostic CLI

The commands below remain useful for local investigation and composing
behavioral reports. They are not the provider-bound pre-tag authority. Values
shown inline are examples only; copying them does not prove that a benchmark,
state transition, reconciliation, checklist item, or release gate executed.
The legacy `benchmark-slo-gate` command remains a diagnostic producer; only
its observed output validated inside the provider-bound workflow contributes
to release authority.

The canonical campaign freezes `row_count=25000`, four partitions, and three
required benchmark metrics: source preparation, Postgres -> MSSQL full refresh,
and MSSQL -> ClickHouse full refresh. Every metric must be at least 500 rows/s;
an average cannot hide a slow phase. The producer records seconds to three
decimal places and rows/s to two, and the verifier requires the two rounding
intervals to be mathematically consistent with the same 25,000 rows.

The PostgreSQL -> MSSQL leg runs through the standard ETL processor with an
externally provisioned disposable SQL Server fence/receipt catalog. This keeps
the performance smoke on the same admission, source-authority, BCP staging and
atomic-finalization path as production instead of calling the sink directly.
After that governed load succeeds, the producer creates a unique clustered
boundary on `dbo.bench_orders(id)` inside
`postgres_to_mssql.target_load_finalize`. This benchmark-local prerequisite
lets the unchanged source-shape policy prove an indexed, high-confidence range
scan and emit the four regular partition files required for MSSQL ->
ClickHouse evidence. A load or index error stops the campaign; a lazy
`PhysicalChunkedFileExportArtifact` with zero observable partitions or bytes
remains invalid release evidence.

```bash
dpone ops live-certification-plan \
  --profile real_local \
  --row-count 25000 \
  --output-dir test_artifacts/live_certification/plan \
  --format json
```

The `real_local` plan starts disposable services through
`docker/docker-compose.integration.yml` and produces case-level test evidence.
Run the following domain-specific commands with observed inputs before
assembling the final evidence pack.

These producers emit behavioral `evidence_status=PASS` only when their own
checks pass. That status makes the artifact usable in an evidence pack; it is
not production authorization. Generic production gates additionally require a
separate verified production authority and therefore reject raw certification
JSON, even when it self-declares `VERIFIED`. Route promotion uses the
cryptographically re-verified route-certification matrix described below.

```bash
dpone ops performance-certification \
  --profile real_local \
  --row-count 25000 \
  --metrics-json '{"throughput_rows_per_second":1000,"duration_seconds":60,"memory_peak_mb":1024,"failure_rate":0}' \
  --minimum-json '{"throughput_rows_per_second":500,"failure_rate":0}' \
  --maximum-json '{"duration_seconds":120,"memory_peak_mb":2048}' \
  --output-dir test_artifacts/live_certification/performance-certification \
  --format json
```

```bash
dpone ops live-state-reconciliation \
  --profile real_local \
  --artifact state=test_artifacts/live_certification/state_evidence.json \
  --artifact reconciliation=test_artifacts/live_certification/reconciliation_evidence.json \
  --require state \
  --require reconciliation \
  --output-dir test_artifacts/live_certification/live-state-reconciliation \
  --format json
```

```bash
dpone ops release-evidence-pack \
  --release vX.Y.Z \
  --profile real_local \
  --artifact service_markers=test_artifacts/live_certification/service_markers.json \
  --artifact certification_pack=test_artifacts/live_certification/certification-pack/connector_certification_pack.json \
  --artifact performance_certification=test_artifacts/live_certification/performance-certification/performance_certification.json \
  --artifact live_state_reconciliation=test_artifacts/live_certification/live-state-reconciliation/live_state_reconciliation.json \
  --artifact pre_release_checklist=test_artifacts/live_certification/pre-release/pre_release_checklist.json \
  --artifact evidence_chain=test_artifacts/live_certification/evidence-chain/evidence_chain_index.json \
  --artifact agent_pr_merge_receipt=test_artifacts/release/vX.Y.Z/agent_pr_merge_receipt.json \
  --require service_markers \
  --require certification_pack \
  --require performance_certification \
  --require live_state_reconciliation \
  --require pre_release_checklist \
  --require evidence_chain \
  --require agent_pr_merge_receipt \
  --output-dir test_artifacts/live_certification/release-evidence \
  --format json
```

`agent_pr_merge_receipt` is an explicit required artifact on every invocation
used for release. It is not an optional profile extra: the argument above makes
the executable evidence pack fail when the exact-commit receipt is absent or
not `PASS`. Passing any `--require` replaces the profile default list, so every
release command above repeats the complete profile set before adding the merge
receipt. Keep `source-agent-pr-receipt.zip` from `agent-pr-receipt` and
`agent_pr_merge_check.json` from the separate `agent-pr-merge-check` artifact
beside the release gate report for audit and follow the
[merge-receipt runbook](agent-pr-merge-receipt-runbook.md).

## Pre-release checklist

Before a minor or major release, collect evidence for:

| Area | Required proof |
| --- | --- |
| CLI UX | `dpone --help`, registered command smoke tests, JSON/Markdown output checks, and no unexpected non-zero exit for valid options. |
| Runtime execution | `dpone run path/to/manifest.yaml` smoke and equivalent Python API execution through `dpone.api`. |
| Hierarchical lineage | Nested-object row IDs, parent row IDs, root row IDs, and list indexes are deterministic across retries. |
| Source -> sink strategies | Integration matrix artifacts for each supported source, sink, and strategy. |
| Docker-live routes | Opt-in MSSQL + ClickHouse and Postgres + MSSQL Docker evidence for release-candidate routes. |
| Contracts and guardrails | Schema contracts, runtime data contracts, policy checks, quarantine, and state-commit-after-load behavior. |
| Documentation | YAML examples parse, links are valid, MkDocs strict build passes, architecture/CI/CD/testing docs are current. |
| Package | Build, twine check, fresh install smoke, and PyPI version availability check. |
| Supply chain and agent governance | GitHub Artifact Attestation receipts, SLSA self-assessment, OSSF Scorecard signal, `agent_governance_gate.json`, and the exact-commit `agent_pr_merge_receipt.json` plus check projection and preserved source archive for every release. |

Generate a diagnostic machine-readable checklist only after the named checks
have actually run:

```bash
dpone ops pre-release-checklist \
  --release vX.Y.Z \
  --release-type minor \
  --check cli_help_surface=true \
  --check cli_output_contracts=true \
  --check run_cli_manifest=true \
  --check run_python_api_manifest=true \
  --check nested_hierarchical_identity=true \
  --check nested_parent_child_integrity=true \
  --check source_sink_strategy_matrix=true \
  --check source_sink_artifacts=true \
  --check docker_live_routes=true \
  --check contracts_guardrails=true \
  --check documentation_yaml_examples=true \
  --check documentation_links=true \
  --check documentation_mkdocs=true \
  --check ci_cd_quality=true \
  --check package=true \
  --output-dir test_artifacts/live_certification/pre-release \
  --format json
```

The booleans summarize already-produced evidence; they are not permission to
assert a check. The canonical workflow derives its checklist from validated
source roles and rejects caller-supplied booleans. Attach a standalone
`pre_release_checklist.json` to diagnostic packs only.

## Runbook

Failure: `performance_certification.not_passed`.

1. Inspect the failed metric and threshold in `performance_certification.json`.
2. Check native fast paths, batch size, partitioning, staging backend, and target capacity.
3. Do not lower thresholds unless a reviewed benchmark-baseline update explains why.

Failure: `live_state_reconciliation.not_passed`.

1. Re-run state backend integration markers for Postgres and MSSQL.
2. Confirm XMin, Kafka offsets, CDC offsets, and run state advanced only after load success.
3. Re-run physical-delete reconciliation for changed/deleted rows.

Failure: `release_evidence_pack.not_passed`.

1. Open `blockers` in `release_evidence_pack.json`.
2. Missing required artifacts block release.
3. Red required artifacts must be fixed at the originating gate.
4. Do not edit generated evidence by hand.

Failure: the `Release candidate evidence` check is missing, red, or pending.

1. Confirm the dispatch used `--ref master`, the current full master SHA, and
   the proposed `vX.Y.Z` release.
2. Before tagging, open the exact-SHA workflow dispatch with the unique maximum
   provider `created_at`. Do not reuse an older PASS after a newer dispatch
   queues, starts, or fails. Equal creation times are ambiguous. The selected
   run's current `run_attempt` and status are authoritative; rerunning an older
   dispatch cannot supersede the newer dispatch.
3. Fix the originating check. If C is still current `master`, start a complete
   new attempt for C. If `master` advanced, select its new tip as the candidate
   and dispatch evidence for that new commit. Later `master` advancement alone
   does not invalidate evidence that was already green for frozen C.

Failure: release-candidate evidence is expired, ambiguous, or digest-invalid.

1. Preserve the provider/run details for audit; never reconstruct authority
   JSON or copy an artifact across attempts.
2. If frozen C is still current `master`, run the complete workflow again for
   C. If `master` advanced, choose its new current commit as the release
   candidate and run a new campaign; do not backfill authority for old C.
3. Do not create or push the tag until the newest dispatch, its current
   terminal check, and the unique artifact all validate.
4. If an annotated tag already exists when evidence becomes unavailable, do
   not move or replace that tag. Keep both publishing workflows blocked and
   recover with a newly reviewed version, candidate commit, campaign, and tag.

Failure: publication-run boundary is invalid.

1. Confirm the tag push created exactly one `release.yml` run and one
   `runtime-image.yml` run for the same repository, tag, and commit. Manual,
   foreign, duplicate, or mismatched runs are not authority.
2. Confirm the gate selected the unique newest exact-SHA dispatch created no
   later than the earlier provider `created_at` of those two runs. That
   selected run, terminal check, and artifact must all have completed by the
   same cutoff. A post-cutoff evidence dispatch is ignored. Do not use the
   tagger timestamp or a caller-supplied time as a substitute.
3. A transient zero-exact-match discovery race already gets 31 observations
   with at most 30 ten-second sleeps (about 300 seconds of sleep budget;
   request time is additional). Unrelated other-tag runs are ignored. The
   verifier retries only zero exact-tag matches or a lagging current-run
   `run_attempt`/`updated_at` list projection. Persistent mutable drift,
   malformed exact matches, and duplicate exact matches fail closed. If
   bounded polling expires,
   inspect both runs, then use **Re-run failed jobs** on the existing tag-push
   run after the pair is visible. Never manually dispatch `release.yml`, use a
   full rerun after immutable package artifacts exist, move,
   delete-and-recreate, or force-push the tag.
4. If the exact immutable pair cannot be authenticated, recover with a newly
   reviewed version, candidate commit, pre-tag campaign, and annotated tag.

Failure: `pre_release_checklist.not_passed`.

1. Open `pre_release_checklist.json` and fix every `*.missing` or
   `*.not_passed` blocker.
2. Re-run the actual gate, not only the checklist command.
3. Regenerate `release-evidence-pack` after the checklist is green.

Failure: docs are red.

1. Run `uv run dpone docs check-docs`.
2. Run `uv run mkdocs build --strict`.
3. Fix broken links, invalid YAML examples, outdated CLI reference, or stale architecture docs.

## Developer notes

| Concern | Module |
| --- | --- |
| Artifact validation | `dpone.ops.artifact_validation` |
| Performance evidence | `dpone.ops.performance_certification` |
| State/reconciliation evidence | `dpone.ops.live_state_reconciliation` |
| Release evidence pack | `dpone.ops.release_evidence_pack` |
| Pre-release checklist | `dpone.ops.pre_release_checklist` |
| Live certification plan | `dpone.ops.live_certification` |
| Pre-tag producer | `.github/workflows/release-candidate-evidence.yml` |
| Provider-bound verifier | `tools/agent_policy/release_candidate_evidence_gate.py` |
| PyPI prepublication verifier | `tools/pypi_prepublication_gate.py` |

Keep release evidence services small and dependency-light. They summarize
artifacts; they do not execute database loads directly.
The PyPI verifier is a separate stdlib-only source-free bundle for the
privileged publisher: shared codec/contracts are reused, while local-byte and
HTTP adapters remain isolated from policy composition.
The provider-bound publication boundary is defined by
[ADR 0049](adr/0049-provider-bound-release-candidate-evidence.md) and the
[approved feature design](feature-design-release-candidate-evidence-gate.md).

## Native transfer release profile

Use `--profile native_transfer` for diagnostic packs that certify critical
native transfer routes such as `Postgres -> MSSQL` or `MSSQL -> ClickHouse`.
It keeps the standard `real_local` requirements and additionally requires
`strategy_certification_bundle`. In the canonical pre-tag workflow this
profile is a fixed strict superset: both critical routes and every base role
must pass; the caller cannot substitute one route or a smaller required set.

For MSSQL -> ClickHouse releases that change binary/native transfer behavior,
also attach the BCP native wide-type certification evidence produced by
`tools/mssql_clickhouse_bcp_native_type_certification.py`. The expected
minimum release proof is 10,000 rows, a canonical 201-column MSSQL source, the
202-column dbt result, all supported primitive/text/binary/temporal families,
exact all-row typed hash equality, exact ClickHouse type/nullability schema,
duplicate-key count `0`, and eager cleanup under the configured worker storage
policy.

The legacy wide-type result alone is not exact release attribution. Generate
the local receipt and verify it with the separately supplied release, route,
transport, source relation, and target relation shown in the
[wide release-certification how-to](mssql-clickhouse-wide-release-certification.md).
The verifier
checks the closed JSON Schemas, candidate commit/source snapshot, retained dbt
manifest/run-results bytes, full source/output typed data generation, observed
backend (`native_accelerated` for `required`, `python_reference` for `off`), and
cleanup evidence. It rejects dirty or changed source, a relabelled subject,
unknown/secret fields, duplicate JSON keys, partial hashes, or mutated artifact
bytes.

The final local proof is the create-once producer-generated three-slot campaign index with
exactly `bcp_native_required`, `bcp_native_off`, and `parquet_s3_pull`. Generate
it outside the checkout first, then attach the unmodified output to review.
These artifacts deliberately remain `production_certification=UNVERIFIED`;
the generic certification trust boundary rejects them as production or
`vendor_live` evidence.

These local schema-bound artifacts never become production evidence, even when
their behavioral checks pass. Production promotion still requires the normal
canonical route bundle plus its independently verified cryptographic route
attestation; a local campaign or receipt cannot replace that authority.

```bash
dpone ops release-evidence-pack \
  --release vX.Y.Z \
  --profile native_transfer \
  --artifact service_markers=test_artifacts/release/service_markers.json \
  --artifact certification_pack=test_artifacts/live_certification/certification-pack/connector_certification_pack.json \
  --artifact performance_certification=test_artifacts/live_certification/performance-certification/performance_certification.json \
  --artifact live_state_reconciliation=test_artifacts/live_certification/live-state-reconciliation/live_state_reconciliation.json \
  --artifact pre_release_checklist=test_artifacts/pre-release/pre_release_checklist.json \
  --artifact evidence_chain=test_artifacts/evidence-chain/evidence_chain_index.json \
  --artifact strategy_certification_bundle=test_artifacts/strategy_certification/postgres_mssql/strategy_certification_bundle.json \
  --artifact local_mssql_clickhouse_wide_campaign="${DPONE_WIDE_CERT_ROOT:?run the wide certification how-to first}/campaign/mssql_clickhouse_wide_release_campaign.json" \
  --artifact local_mssql_clickhouse_temporal_matrix="${DPONE_WIDE_CERT_ROOT}/native-temporal-matrix.json" \
  --artifact agent_pr_merge_receipt=test_artifacts/release/vX.Y.Z/agent_pr_merge_receipt.json \
  --require service_markers \
  --require certification_pack \
  --require performance_certification \
  --require live_state_reconciliation \
  --require pre_release_checklist \
  --require evidence_chain \
  --require strategy_certification_bundle \
  --require agent_pr_merge_receipt \
  --output-dir test_artifacts/release-evidence/native-transfer \
  --format json
```

The command above finishes the behavioral native-transfer evidence pack. It
does not make the route production-certified. For production promotion, place
the canonical `release-set.json`, `route_certification_bundle.json`,
`route-attestation.json`, `route-attestation-verification.json`,
`route-attestation.sigstore.json`, and `route-attestation-policy.json` in the
same explicit evidence directory. Also place the policy-referenced
`trusted_root.json` there with the exact digest declared by the policy, then run
the cryptographic publisher:

Create the release-bound route bundle as shown in
[Route certification matrix](route-certification-matrix.md#evidence-set), then
build and verify the deployment attestation with the exact commands in
[Airflow route attestation](airflow-route-attestation.md). Keep the generated
Sigstore bundle, policy, and trusted root beside that proof set. Do not hand-edit
generated proof bytes; the policy and trusted root are separately reviewed,
platform-owned inputs.

```bash
dpone certify routes \
  --commit-sha "$(git rev-parse HEAD)" \
  --evidence-dir test_artifacts/routes/mssql-clickhouse-prod-a \
  --output-dir test_artifacts/route-certification-matrix-prod-a \
  --format json
```

Accept production promotion only when the exact route row is
`production-certified`. A consistency receipt, behavioral evidence pack, or
local Docker campaign cannot substitute for Sigstore/policy re-verification.

If `strategy_certification_bundle` is missing, the pack is red with
`strategy_certification_bundle.missing`. Because any explicit `--require`
sequence replaces the profile defaults, release examples repeat the whole
profile set before adding the exact merge receipt.
