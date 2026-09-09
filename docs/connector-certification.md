# Connector certification

Connector discovery keeps implementation maturity, public release phase, route
support, route certification, and evidence status separate.

Connector level is intentionally separate from route level. Use the
[six-dimensional route certification matrix](route-certification-matrix.md)
before claiming that one source, sink, strategy, transport, schema-evolution
mode, and Airflow/runtime mode are production-certified together.

## Status taxonomy

| Axis | Values | Meaning |
| --- | --- | --- |
| Connector maturity | `certified`, `experimental`, `community` | implementation ownership and confidence |
| Release phase | `stable`, `beta`, `alpha` | public lifecycle of the connector surface |
| Route support | `supported`, `conditional`, `not_supported` | runtime catalog support for source/sink/strategy |
| Route certification | `experimental`, `route-certified`, `production-certified`, `enterprise-certified` | evidence level for a complete six-dimensional variant |
| Evidence | `PASS`, `FAIL`, `SKIP`, `UNVERIFIED` | current proof status |

Connector maturity never certifies every route. A route is:

```text
source × sink × strategy × transport × schema evolution × Airflow/runtime mode
```

Missing, stale, mocked, skipped, malformed, or foreign-commit evidence is
`UNVERIFIED`.

The generic credential-free `dpone ops certification-run` mock profile verifies
deterministic behavior only. A non-empty successful run can have
`passed: true`, but its `evidence_status` is always `UNVERIFIED`; an empty
selection is not a vacuous pass. `dpone ops evidence-bundle` therefore records
the certification item as non-passing until approved current `PASS` evidence
is supplied. Mock output cannot satisfy a go-live or ops policy gate.

Certification suite, connector pack, release gate, release summary, release
promotion, change-request, post-deploy, production-maturity, native-transfer
runtime, evidence bundle, and unified-evidence readers share the same typed
trust rule: behavioral success requires literal `passed: true`, while
certification trust also requires `evidence_status: PASS`. Status-less legacy
certification artifacts are `UNVERIFIED`, not implicitly certified.
`PASS` is rejected when the same payload contains blockers, violations,
findings, errors, or a failed required stage. Release consumers also bind the
bundle to the requested release, re-read every required stage, verify its
SHA-256, and reject missing or forged artifact-index entries. An empty evidence
bundle or duplicated required evidence name is never a successful go-live
receipt.

## Project evidence authority

Capability discovery never scans the repository for certification artifacts.
Platform owners can opt one project into current route evidence through the
single bounded `dpone.yaml` authority:

```yaml
schema: dpone.project.v1
capability_discovery:
  certification_evidence:
    matrix_path: test_artifacts/routes/publication/route-certification-matrix.json
    expected_commit: 0123456789abcdef0123456789abcdef01234567
    evidence_dirs:
      - test_artifacts/routes/mssql-clickhouse
    max_age_hours: 168
```

All paths are project-relative regular files or directories. Absolute paths,
`..`, duplicate directories, malformed types, and more than 128 evidence
directories fail closed with
`DPONE_CAPABILITY_EVIDENCE_CONFIG_INVALID`. The matrix still has to match the
exact commit and every proof is reread through the canonical evidence reader.
Without this optional section, connector and route support remain discoverable
but route evidence is honestly `UNVERIFIED`.

If the configured evidence authority is invalid, `connectors list` and
`recipe list` return `passed: false`, expose
`DPONE_CAPABILITY_EVIDENCE_CONFIG_INVALID` in `issues[]`, and exit `1`.
Supported routes remain in the diagnostic payload, but the configuration must
be repaired before the discovery command is considered successful.

## Discover current status

```bash
dpone connectors list --format json
dpone recipe list \
  --source mssql \
  --sink clickhouse \
  --strategy incremental_merge \
  --format json
```

The first command describes connectors. The second describes one route's
support, certification variants, evidence, install extras, limitations, and
beginner recipe.

## Run the report gate

```bash
dpone connectors certify \
  --artifact-dir test_artifacts/connector-certification \
  --format json
```

The command always renders the completed report. It writes only:

- `connector-certification.json` (authoritative certification evidence);
- `connector-certification.md` (regenerable human projection).

Each file is UTF-8 and atomically replaced under one process lock; unrelated
files in the directory are preserved. The pair is not a crash-atomic
transaction: after a process or host crash the Markdown projection may lag the
JSON evidence and must be regenerated. Readers trust only the JSON file. A
report with `passed: false` exits `1`. `--report-only` changes only that
completed-report exit to `0`.

`--fail-on-missing` is a deprecated strict-default no-op. Do not combine it
with `--report-only`.

Capability-specific profiles are evidence requests, not synthetic tests:

```bash
dpone connectors certify \
  --profile static \
  --capability native_transfer.stream \
  --format json
```

The `static` profile validates declarations but keeps its planned conformance
cases `unverified`. Its report may expose `behavior_passed: true` when the
declarations are technically eligible, but `passed` remains `false` and
`evidence_status` remains `UNVERIFIED`; it exits `1` unless `--report-only` is
explicit. Empty capability requests and unknown profiles also fail closed. A
capability becomes certified only from actual current evidence produced by the
corresponding live profile.

## Connector matrix

The canonical discovery snapshot currently exposes these built-in connector
families:

| Connector id | Roles | Maturity | Release phase |
| --- | --- | --- | --- |
| `postgres` | source, sink | experimental | beta |
| `mysql` | source | experimental | beta |
| `mssql` | source, sink | experimental | beta |
| `clickhouse` | source, sink | experimental | beta |
| `kafka` | source, sink | experimental | beta |
| `bigquery` | sink | experimental | beta |
| `rest` | source (`api` endpoint family) | experimental | beta |

Managed API providers are implementations of the `rest` provider identity; they
do not create competing route identities.

## Required test markers

- `unit`: default tests that require no external services.
- `integration`: local disposable infrastructure.
- `integration_live`: real vendor services and real credentials.
- `nightly`: scheduled broad validation.

## GitHub Actions gates

The public OSS repository uses two certification layers:

- `.github/workflows/ci.yml` is the **required** pull request / merge quality gate
  for non-live checks (together with the other branch-protection contexts).
- `.github/workflows/connector-certification.yml` is the recurring **schedule /
  manual hygiene** gate. It is **not** a required status check for merge or
  release. Do not add it to branch protection required contexts.

The recurring gate runs on a daily schedule and can also be started manually from GitHub Actions.
It is an async scheduled release-evidence gate: release reviews should record
the run ID, status, and artifact links when connector confidence is relevant,
but unrelated GitOps-only releases may publish while the scheduled run is still
running if that async status is called out explicitly.

### Required for merge vs schedule hygiene

| Surface | Trigger | Failure meaning |
| --- | --- | --- |
| `ci.yml` (+ other required checks) | PR / push | Blocks merge. |
| Connector certification `offline` | schedule + dispatch | Must stay green; offline regressions are real product/contract debt. |
| Connector certification `local-live` | schedule + dispatch (opt-in) | Must stay green for disposable Docker markers; ClickHouse load-governance `partition_replace` cells may be explicitly `xfail` until REPLACE PARTITION structure parity lands. |
| Connector certification `vendor-live` | schedule (default) | Hygiene only: records secret readiness; skips the suite when no vendor secrets exist; when some secrets exist, credential-gated pytest skips are allowed, but hard failures/errors still fail the job. |
| Connector certification `vendor-live` | `workflow_dispatch` with `run_vendor_live=true` | Strict operator evidence: complete secrets required and zero skipped tests (`--max-skipped 0`). |

It produces:

- `connector-certification-offline`: the `dpone certify` matrix in JSON and Markdown.
- local live results for the explicitly selected disposable Postgres, MySQL,
  MSSQL, and Kafka marker cases. Service startup alone does not certify
  ClickHouse, Schema Registry, or MinIO; their status comes from separate
  exact-route evidence or remains `UNVERIFIED`.
- vendor live results for provider/API integration directories when the corresponding GitHub secrets are configured, plus `secret-readiness.json` evidence.

The jobs are intentionally domain-scoped:

- `offline-certification` runs credential-free connector contracts and publishes connector capability artifacts.
- `local-live-certification` installs ODBC Driver 18 plus `mssql-tools18`,
  starts the local Docker service set, materializes the test MSSQL database
  with `sqlcmd`, and runs explicit local service tests. For `local_live` and
  `real_local`, all seven MySQL -> PostgreSQL/MSSQL/ClickHouse/Kafka route cells
  must pass with zero skips; their JUnit and exact-commit JSON are retained.
- `vendor-live-certification` runs only provider/API directories. It must not run local Docker route tests from `tests/integration/mssql`, `tests/integration/clickhouse`, `tests/integration/kafka`, or source -> sink matrix folders. Manual dispatch requires at least one passed test and zero skipped tests; the daily schedule allows credential-gated skips when secrets are incomplete and records that as `UNVERIFIED` hygiene evidence rather than a false certification pass. Hard pytest failures still fail the job on both triggers.

Live certification is intentionally opt-in at the test level: if an external system or credential is not configured, that connector's integration test reports a skip rather than silently using fake data.

## Certification checklist

For each connector, add or update:

- Documentation page.
- Example manifest.
- Credential model.
- Offline unit tests.
- Optional dependency smoke test.
- Integration test profile.
- Live test instructions.
- Known limitations.
- Changelog entry when level changes.
