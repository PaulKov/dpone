# Live certification

Live certification proves connector and strategy behavior against disposable
local services or real vendor systems. It is manual by design and complements
the default non-live CI gate.

## Contents

- [Profiles](#profiles)
- [Local live flow](#local-live-flow)
- [Vendor live flow](#vendor-live-flow)
- [Benchmark and SLO gate](#benchmark-and-slo-gate)
- [Performance, state, and release evidence](#performance-state-and-release-evidence)
- [Artifacts](#artifacts)
- [GitHub Actions](#github-actions)
- [Runbook](#runbook)
- [Developer notes](#developer-notes)

## Profiles

| Profile | Credentials | Services | Purpose |
| --- | --- | --- | --- |
| `local_live` | no external credentials | Postgres, MySQL, MSSQL, ClickHouse, Kafka, Schema Registry, MinIO | Execute disposable-service markers, MySQL route cells, and the complete credential-free contract matrix; retain case-level JUnit-derived evidence. |
| `real_local` | no external credentials | Postgres, MySQL, MSSQL, ClickHouse, Kafka, Schema Registry, MinIO | Execute the same disposable stack and complete contract matrix as a release-candidate behavioral baseline; release-only evidence domains remain separate gates. |
| `type_matrix_certification` | no external credentials | Postgres, MSSQL, ClickHouse | Execute critical-route contract and local fixture tests and retain their JUnit-derived results; planning artifacts are not certification evidence. |
| `native_transfer` | no external credentials | Postgres, MySQL, MSSQL, ClickHouse, Kafka, Schema Registry, MinIO | Execute critical native-transfer fixtures. Strategy bundles and release evidence packs are separate, explicit producers. |
| `vendor_live` | yes | real managed/vendor systems | Prove provider/API behavior, quota handling, external auth, and managed-system compatibility before a release. |

Use `local_live` during feature work, `type_matrix_certification` for critical
type/DDL/temporal-fidelity changes, `native_transfer` for critical native route
certification, `real_local` before minor/major releases, and `vendor_live` when
provider/API behavior must be proven against managed systems. The `vendor_live`
profile is intentionally manual and requires configured CI secrets or local
secret providers.

## Local live flow

Generate the plan:

```bash
dpone ops live-certification-plan \
  --profile local_live \
  --row-count 25000 \
  --output-dir test_artifacts/live_certification/plan \
  --format json
```

Start local services:

```bash
docker compose -f docker/docker-compose.integration.yml up -d \
  postgres mysql mssql clickhouse kafka schema-registry minio

docker exec dpone-it-mssql bash -c \
  'exec /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -b -Q "$1"' \
  _ "IF DB_ID(N'dpone') IS NULL CREATE DATABASE [dpone]"
```

Run service markers:

```bash
DPONE_RUN_INTEGRATION=1 \
DPONE_IT_MYSQL_HOST=127.0.0.1 \
DPONE_IT_MYSQL_PORT_FORWARD=53306 \
DPONE_IT_MSSQL_HOST=127.0.0.1 \
DPONE_IT_MSSQL_PORT=51433 \
DPONE_KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:59092 \
DPONE_SCHEMA_REGISTRY_URL=http://127.0.0.1:58081 \
uv run pytest \
  tests/integration/mysql/test_mysql_source_integration.py \
  tests/integration/postgres/test_postgres_connector_integration.py \
  tests/integration/mssql/test_mssql_optional_integration.py \
  tests/integration/kafka/test_kafka_optional_integration.py \
  -q -rs \
  --junitxml=test_artifacts/live_certification/local_service_markers_junit.xml

uv run python tools/ci/assert_junit_executed.py \
  --junit test_artifacts/live_certification/local_service_markers_junit.xml \
  --min-passed 8 \
  --max-skipped 0 \
  --evidence-json test_artifacts/live_certification/service_markers.json \
  --profile local_live \
  --commit-sha "$(git rev-parse HEAD)"
```

Run the seven local MySQL route cells:

```bash
DPONE_RUN_INTEGRATION=1 \
uv run pytest \
  tests/integration/mysql/test_mysql_to_mssql_native_transfer_integration.py::test_mysql_to_mssql_full_refresh_bcp_staging \
  tests/integration/mysql/test_mysql_to_mssql_native_transfer_integration.py::test_mysql_to_mssql_incremental_merge_watermark \
  tests/integration/mysql/test_mysql_to_postgres_native_transfer_integration.py::test_mysql_to_postgres_full_refresh_csv_copy \
  tests/integration/mysql/test_mysql_to_postgres_native_transfer_integration.py::test_mysql_to_postgres_incremental_merge_watermark \
  tests/integration/mysql/test_mysql_to_clickhouse_native_transfer_integration.py::test_mysql_to_clickhouse_full_refresh_tsv \
  tests/integration/mysql/test_mysql_to_clickhouse_native_transfer_integration.py::test_mysql_to_clickhouse_incremental_merge_watermark \
  tests/integration/mysql/test_mysql_to_kafka_native_transfer_integration.py::test_mysql_to_kafka_full_refresh_csv_produce \
  -q -rs \
  --junitxml=test_artifacts/live_certification/mysql/mysql_local_route_cells_junit.xml

uv run python tools/ci/assert_junit_executed.py \
  --junit test_artifacts/live_certification/mysql/mysql_local_route_cells_junit.xml \
  --min-passed 7 \
  --max-skipped 0 \
  --evidence-json test_artifacts/live_certification/mysql/mysql_local_route_cells.json \
  --profile local_live \
  --commit-sha "$(git rev-parse HEAD)"
```

A missing or skipped MySQL cell fails the gate. The workflow also records the
exact `GITHUB_SHA` beside the JUnit report; Docker credentials are never copied
into retained route evidence.

Run the source -> sink matrix:

```bash
DPONE_RUN_INTEGRATION=1 \
DPONE_RUN_INTEGRATION_MATRIX=1 \
DPONE_MATRIX_RUN_MODE=mock_contract \
DPONE_MATRIX_ROW_COUNT=25000 \
DPONE_MATRIX_ARTIFACT_DIR=test_artifacts/live_certification/matrix \
uv run pytest -m integration_matrix tests/integration/matrix -q \
  --junitxml=test_artifacts/live_certification/matrix/junit.xml
```

The workflow keeps the derived JUnit execution receipt at
`test_artifacts/live_certification/matrix_execution.json`, outside
`DPONE_MATRIX_ARTIFACT_DIR`. That directory is reserved for matrix case and
`__behavior.json` inputs consumed by the strict certification report builder.

For a release-candidate local gate, keep the generic matrix complete and
credential-free. The `real_local` profile applies to retained evidence from the
separate service-backed route fixtures:

```bash
DPONE_RUN_INTEGRATION=1 \
DPONE_RUN_INTEGRATION_MATRIX=1 \
DPONE_MATRIX_RUN_MODE=mock_contract \
DPONE_MATRIX_ROW_COUNT=25000 \
DPONE_MATRIX_ARTIFACT_DIR=test_artifacts/live_certification/matrix \
uv run pytest -m integration_matrix tests/integration/matrix -q \
  --junitxml=test_artifacts/live_certification/matrix/junit.xml
```

Stop services after artifacts are collected:

```bash
docker compose -f docker/docker-compose.integration.yml down -v
```

## Vendor live flow

Generate a vendor plan:

```bash
dpone ops live-certification-plan \
  --profile vendor_live \
  --include-vendor-live \
  --row-count 10000 \
  --output-dir test_artifacts/live_certification_vendor/plan \
  --format json
```

Run vendor tests only when secrets and cost controls are configured:

```bash
uv run dpone ops managed-credentials-readiness \
  --profile vendor_live \
  --required-env APPSFLYER_API_TOKEN \
  --required-env GOOGLE_ADS_DEVELOPER_TOKEN \
  --output-dir test_artifacts/live_certification_vendor/managed-credentials \
  --format json

DPONE_RUN_INTEGRATION_LIVE=1 \
uv run pytest -m integration_live tests/integration -q
```

`managed-credentials-readiness` records only credential variable names and
presence/missing status. It never reads, prints, hashes, or stores credential
values. The GitHub `vendor_live` workflow runs this gate before the backfill
matrix, so missing managed credentials fail fast before any external API or
database test spends quota.

Never put vendor credentials in manifests, workflow logs, or certification
artifacts. If a managed system needs a stronger probe than environment presence
(for example, API `whoami` or bucket sentinel access), implement that as a
connector capability probe with redacted evidence rather than extending this
generic readiness command.

## Benchmark and SLO gate

Use `benchmark-slo-gate` to combine performance regression checks and
operational SLO objectives:

```bash
dpone ops benchmark-slo-gate \
  --metrics-json '{"throughput_rows_per_second":120000,"duration_seconds":40,"freshness_lag_seconds":120,"failure_rate":0}' \
  --baseline-json '{"throughput_rows_per_second":{"value":100000,"direction":"higher"},"duration_seconds":{"value":60,"direction":"lower"}}' \
  --objectives-json '{"throughput_rows_per_second":{"min":100000},"freshness_lag_seconds":{"max":300},"failure_rate":{"max":0}}' \
  --output-dir test_artifacts/live_certification/benchmark-slo \
  --format json
```

Generated files:

| File | Purpose |
| --- | --- |
| `benchmark_slo_gate.json` | Machine-readable combined benchmark and SLO decision. |
| `benchmark_slo_gate.md` | Human-readable report and runbook. |
| `benchmark/benchmark_baseline.json` | Underlying performance baseline report. |
| `benchmark/benchmark_baseline.md` | Underlying performance runbook. |

The gate is red when either benchmark regression or SLO evaluation is red.

## Performance, state, and release evidence

The commands below produce local behavioral/diagnostic artifacts. Inline
metrics and checklist booleans are examples, not observations and not pre-tag
authority. Minor and major releases use the provider-bound workflow described
in [Release evidence](release-evidence.md#canonical-pre-tag-workflow), which
derives every claim from executed source roles.

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

```bash
dpone ops release-evidence-pack \
  --release vX.Y.Z \
  --profile real_local \
  --artifact service_markers=test_artifacts/live_certification/service_markers.json \
  --artifact certification_pack=test_artifacts/live_certification/certification-pack/connector_certification_pack.json \
  --artifact performance_certification=test_artifacts/live_certification/performance-certification/performance_certification.json \
  --artifact live_state_reconciliation=test_artifacts/live_certification/live-state-reconciliation/live_state_reconciliation.json \
  --artifact evidence_chain=test_artifacts/live_certification/evidence-chain/evidence_chain_index.json \
  --artifact pre_release_checklist=test_artifacts/live_certification/pre-release/pre_release_checklist.json \
  --require service_markers \
  --require certification_pack \
  --require performance_certification \
  --require live_state_reconciliation \
  --require evidence_chain \
  --require pre_release_checklist \
  --output-dir test_artifacts/live_certification/release-evidence \
  --format json
```

A standalone green `release_evidence_pack.json` is diagnostic. Minor and major
releases must not be tagged until the newest visible exact-commit
`Release candidate evidence` dispatch and its unique unexpired
artifact validate. Newest is the exact-SHA dispatch with the unique maximum
provider `created_at`, selected before its current attempt and status are read.
After tag creation, the publication gate freezes eligibility to dispatches
created no later than the paired-run cutoff and ignores later dispatches. Patch
releases may use the same provider-bound gate when runtime, CLI, schema,
strategy, connector, or documentation behavior changes.

## Artifacts

Observed workflow artifacts are the JUnit reports and their derived JSON
receipts. The remaining paths below are produced only by their named, separate
CLI gates; absence means `UNVERIFIED`, never implicit success:

```text
test_artifacts/live_certification/
  plan/live_certification_plan.json
  matrix/certification_report.json
  observability/runtime_metrics.json
  benchmark-slo/benchmark_slo_gate.json
  performance-certification/performance_certification.json
  live-state-reconciliation/live_state_reconciliation.json
  certification-pack/connector_certification_pack.json
  artifact-index/artifact_index.json
  evidence-chain/evidence_chain_index.json
  release-evidence/release_evidence_pack.json
```

Attach `benchmark_slo_gate.json`, `connector_certification_pack.json`, and
`evidence_chain_index.json` to diagnostic or industrial-readiness evidence.
For minor/major publication, the top-level decision is the provider-bound
`release_candidate_evidence_receipt.json`, closed manifest, and pack from one
exact attempt. Do not create these files from static workflow payloads.

## GitHub Actions

The repository includes two deliberately separate manual workflows:

- `.github/workflows/live-certification.yml` is the raw feature/route runner.
  It never authorizes a release tag and its release-only placeholder assemblers
  remain disabled.
- `.github/workflows/release-candidate-evidence.yml` is the exact-`master`
  pre-tag authority. Its fixed `native_transfer` campaign is a strict
  `real_local` superset and uploads one provider-bound attempt artifact.

`.github/workflows/live-certification.yml` supports:

- `local_live`: starts Docker services from `docker/docker-compose.integration.yml`,
  runs service markers, seven MySQL route cells, and matrix tests. JSON status
  is derived from the exact JUnit cases and `GITHUB_SHA`.
- `real_local`: uses the same Docker stack and complete `mock_contract` matrix,
  while service-backed fixtures retain `real_local` evidence. `benchmark-slo-gate`,
  `performance-certification`, `live-state-reconciliation`,
  `pre-release-checklist`, evidence-chain, and `release-evidence-pack` outputs
  are `UNVERIFIED` until their real producers are run separately.
- `vendor_live`: runs real provider tests only when `run_vendor_live=true` and
  secrets are configured; selected tests must produce non-skipped JUnit cases.
- route live certification: use `dpone ops route-live-certification` after
  Docker-live or vendor-live route artifacts exist, then feed
  `route_live_evidence_bundle` to `dpone ops route-release-gate`.
- route release candidate orchestrator: use
  `dpone ops route-rc-orchestrator` when the release candidate should produce
  route certification, route live certification, route release gate, and release
  evidence pack outputs in one receipt.
- route release candidate executor: use `dpone ops route-rc-execute` to dry-run
  `route_rc_orchestration.json` receipts by default, or set
  `execute_route_rc_commands=true` in the manual workflow to run the same
  commands against the disposable Docker-live services before teardown.

## Runbook

Failure: local services fail to start.

1. Inspect Docker service logs.
2. Verify SQL Server memory and password complexity.
3. Verify Kafka is healthy before Schema Registry.
4. Re-run `local_live` after fixing runner capacity.

Failure: matrix case is red.

1. Open `<case_id>__behavior.json`.
2. Re-run with `DPONE_MATRIX_CASE_ID=<case_id>`.
3. Check the source -> sink guide for that pair.
4. Do not update expected behavior until the strategy contract is reviewed.

Failure: `benchmark-slo-gate` is red.

1. Compare current metrics with committed baselines.
2. Check native fast path, partitioning, batch size, finalizer policy, and target
   capacity.
3. Do not loosen SLOs to hide a regression.
4. Update baselines only after an intentional reviewed performance change.

The 25,000-row release-candidate stress receipt is deliberately single-partition
until PostgreSQL exports can share one MVCC snapshot across workers. Its
authority requires this exact safe shape (`enabled: false`, one artifact,
no export/load worker override) and rejects both stale parallel evidence and
an accidental relaxation of the receipt.

Failure: evidence chain is red.

1. Rebuild artifact index.
2. Verify required artifacts exist and have stable checksums.
3. Re-run `dpone ops evidence-chain`.
4. Treat checksum drift as an audit event.

Failure: `release-evidence-pack` is red.

1. Open `release_evidence_pack.json` and inspect `blockers`.
2. If an artifact is missing, re-run the originating gate instead of editing the pack.
3. If an artifact is red, fix the source failure first.
4. Rebuild the diagnostic pack after its sources pass. Even a green standalone
   pack does not authorize a tag; complete the canonical provider-bound pre-tag
   workflow before minor or major publication.

## Developer notes

| Extension | Module |
| --- | --- |
| Live certification plan model | `dpone.ops.live_certification` |
| Combined benchmark/SLO gate | `dpone.ops.benchmark_slo_gate` |
| Performance certification | `dpone.ops.performance_certification` |
| State/reconciliation certification | `dpone.ops.live_state_reconciliation` |
| Release evidence pack | `dpone.ops.release_evidence_pack` |
| Matrix registry | `dpone.integration_matrix` |
| Manual workflow | `.github/workflows/live-certification.yml` |
| Pre-tag workflow | `.github/workflows/release-candidate-evidence.yml` |
| Provider verifier | `tools/agent_policy/release_candidate_evidence_gate.py` |

Keep this layer dependency-light. It should orchestrate evidence and commands,
not import heavy database or Kafka clients.

### Native transfer profile route coverage

The raw `native_transfer` profile executes fixture evidence for both critical
native routes, `Postgres -> MSSQL` and `MSSQL -> ClickHouse`. Its retained
JUnit/route outputs remain diagnostic; the disabled placeholder steps do not
build trusted evidence indexes or a release pack. For publication,
the separate `Release candidate evidence` workflow validates both routes,
CDC/state/reconciliation, observed benchmark, exact checks, and merge closure
as one fixed strict superset. It derives the closed manifest and pack without
caller-selected metrics, booleans, roles, or profile.
