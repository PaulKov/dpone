# Feature design: MySQL → ClickHouse batch route v1

- Status: IMPLEMENTED
- Owner: dpone maintainers
- Issue: follow-up to #395 / #396; promote mysql→clickhouse
- Target release: next minor after implementation merges
Last verified: 2026-07-21
Approval: maintainer APPROVED 2026-07-21

## Executive summary

MySQL source is production for `mysql → mssql` (#395) and `mysql → postgres`
(#396). Landing MySQL OLTP into **ClickHouse** is still **Experimental**: there
is no type profile, no ClickHouse-safe TabSeparated wire (docs currently show
Postgres-oriented `csv`, while the non-Postgres default remains
`mssql-delimited` / BCP-shaped text), no Docker IT, and no Batch ETL claim.

This feature promotes `mysql → clickhouse` to **Batch ETL supported** by adding
`mysql_to_clickhouse_analytics_v1`, sink-aware **TabSeparated** export (CH `\N`
nulls and escapes) into the existing ClickHouse staging / HTTP bulk stack,
hermetic contracts, and Docker `integration_mysql` × ClickHouse live evidence.
Binlog CDC and MySQL sink remain out of scope.

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
|---|---|---|---|
| Data engineer | Land MySQL tables into ClickHouse analytics | Experimental; wrong export wire risk | Example plan/run with `dpone[mysql,clickhouse]` |
| Platform engineer | Reuse MySQL Airflow conn | Docs warn not certified | Matrix Batch ETL + doctor/plan green |
| Operator | Trust watermark + staging finalize | No live evidence | Docker IT full_refresh + incremental_merge PASS |

Journey: matrix guide → install extras → connections →
`examples/batch/landing_mysql_to_clickhouse.batch.yaml` → `dpone plan` →
`dpone batch run` → quality gates → staging-first recovery.

## Scope

### In scope

- Type profile `mysql_to_clickhouse_analytics_v1` + `PairTypeMatrixService` pair
- Sink-aware MySQL export: **ClickHouse TabSeparated** (reuse
  `ClickHouseTabSeparatedCodec` / equivalent) — not Postgres CSV and not MSSQL
  BCP scalars
- Schema mapping MySQL COLUMN_TYPE → ClickHouse DDL when `sink_type=clickhouse`
- Default/public guidance for this pair: CH TSV wire (document exact
  `export_format` / artifact label used in manifests vs internal wire)
- Fail-closed: `mssql-delimited` and Postgres-only CSV claims when sink is
  ClickHouse (clear plan/runtime errors)
- Strategies via existing ClickHouse sink (staging-first finalize):
  `full_refresh`, `incremental_append`, `incremental_merge`, `replace`,
  `partition_replace`, `snapshot_reconciliation` as already supported by sink
- Watermark/`incremental_column` extract; state on ClickHouse target where
  applicable (parity with other CH routes)
- Docs/matrix/type-mapping/certification/example/CHANGELOG
- Hermetic contracts + Docker IT (`integration_mysql` + ClickHouse)

### Non-goals

- Named native fast-path catalog entry (stretch; optional advise symmetry with
  `postgres_copy_to_clickhouse_http_tsv`)
- mssql→CH-class lossless knobs (epoch/binary/offset) in v1
- MySQL sink/state, binlog/GTID CDC
- Six-dim attestation / Conformance Lab
- Promoting kafka/bigquery MySQL rows in the same PR
- Claiming delete-correctness from cursor incremental alone

### Assumptions and constraints

- Reuse ADR 0005 staging-first ClickHouse contracts; no new ADR
- MySQL source from #395/#396 reused; change is export path + types + evidence
- Optional deps: `dpone[mysql]` + `dpone[clickhouse]`
- Prefer **TSV / FORMAT TabSeparated** over inventing a CH CSV sink path

## Approaches considered

| Approach | Pros | Cons | Decision |
|---|---|---|---|
| **A. CH TabSeparated + existing CH staging/HTTP bulk** (recommended) | Matches postgres→CH industrial wire; reuses codec/sink | Third MySQL export dialect | **Adopt** |
| B. Reuse Postgres CSV into CH | Smallest MySQL change | Wrong null/bool/delimiter vs CH bulk; slow path | **Reject** |
| C. Feed `mssql-delimited` into CH bulk | Accidental format name overlap | BCP scalars / nulls corrupt CH | **Reject** (fail-closed) |
| D. Named `mysql_stream_to_clickhouse_http_tsv` only | Advise polish | Not required for Batch claim | Defer / stretch |

## Public contract

### CLI

No new commands.

### Python API

New type mapper under `dpone.type_system.source_sink`. Connector may gain
`export_clickhouse_tsv_to_file` (or parameterized export).

### Manifest/schema

No new `source.type`. Recommended mysql→clickhouse options must document the
public export format that produces CH-safe TSV (exact token TBD during
implementation: either a dedicated value or documented internal wire with
fail-closed validation against `sink.type=clickhouse`).

`export_format: mssql-delimited` + ClickHouse sink → fail closed at plan and
runtime.

### Artifacts and evidence

`FileExportArtifact` with CH-safe TSV format label consumed by existing
ClickHouse staging/bulk. Docker IT: PASS or explicit SKIP.

### Compatibility and migration

Additive. mysql→mssql and mysql→postgres wires unchanged. Operators on
Experimental CH manifests must switch to the documented CH-safe export.

## Detailed algorithm

1. Validate manifest: `source.type=mysql`, `sink.type=clickhouse`.
2. Resolve credentials; construct connectors lazily.
3. Negotiate export: require CH-safe TabSeparated wire for this pair.
4. Map schema via `mysql_to_clickhouse_analytics_v1` (bool/`tinyint(1)`,
   unsigned widen, datetime→DateTime64, JSON→String/JSON, ENUM/SET/spatial →
   explicit contract).
5. Extract full or watermark (`incremental_column >` target MAX / state).
6. Stream SELECT → TabSeparated file (`\N` nulls, CH escapes) with
   authoritative `rows_exported`.
7. ClickHouse staging load + set-based finalize.
8. Quality gates; commit watermark only after sink success.
9. On failure: existing CH staging rollback semantics; no watermark advance.

### Pseudocode

```text
creds_src, creds_sink = resolve(...)
src = MySQLSource(MySQLConnector(creds_src), state=ch_state)
extract = src.extract(load_config)
artifact = export_clickhouse_tsv(extract, type_profile=mysql_to_clickhouse_analytics_v1)
sink = ClickHouseSink(...)
result = sink.load(load_config, artifact)  # staging + finalize
if result.ok:
    state.commit(watermark)
```

### State machine

Staging-first: `Planned → Extracting → Staging → Finalizing → Quality → Succeeded`,
failure → safe retry without watermark advance.

### Edge cases

- Empty extract: success, zero rows; watermark unchanged
- Missing incremental column for incremental modes: fail closed
- Unknown `columns`: fail closed
- Wrong export format for CH: fail closed
- Optional dep missing: clear install hint

## Architecture

| Component | Change | Responsibility |
|---|---|---|
| MySQL connector / strategies | extend | CH TSV export + schema map for CH |
| `MySQLClickHouseTypeMapper` | new | `mysql_to_clickhouse_analytics_v1` |
| `PairTypeMatrixService` | extend | Register mysql×clickhouse |
| ClickHouse sink/staging | reuse | TabSeparated bulk + finalize |
| Docs/examples/CHANGELOG | update | Batch ETL claim |

### ADR

Not required — within ADR 0005 + existing CH staging contracts.

## Market comparison

Research date: 2026-07-21. Official / primary docs posture (same family as #395/#396).

| System | Relevant capability | Adopt/reject | Source |
|---|---|---|---|
| Airbyte MySQL → ClickHouse | CDC/cursor × dest sync modes | Adopt full/cursor/append/merge map; defer CDC | Airbyte MySQL + CH destination docs |
| dlt sql_database → clickhouse | Cursor incremental + dispositions | Adopt cursor + merge/replace mental model | dlthub.com |
| ClickHouse native | HTTP/client insert, TabSeparated/CSVWithNames | **Adopt TabSeparated** as certified wire | ClickHouse docs |
| Fivetran MySQL | Binlog/Teleport | Defer binlog | fivetran.com |
| Beam / gusty / Cosmos | Not MySQL→CH EL product | **N/A** | — |

## Measurable differentiation

```yaml
axis: staging-first MySQL→ClickHouse with CH-safe TSV and explicit type profile
scenario: MySQL OLTP → ClickHouse landing, incremental_merge, staging finalize
baseline: ad-hoc dump/CSV without typed matrix + staging evidence
metric: idempotent rerun correctness + authoritative rows_exported
target: Docker IT PASS when mysql+clickhouse available; merge accumulation via staging
procedure: integration_mysql × clickhouse IT markers; SKIP if unavailable
artifact: pytest integration evidence
limitations: no binlog delete SLO; named fast-path optional
```

## Security, privacy, and operations

- Existing credential resolvers; never log secrets
- Runbook: `pip install "dpone[mysql,clickhouse]"`

## Test and certification plan

| Layer | Scenario | Environment | Expected |
|---|---|---|---|
| Unit | type map; TSV ≠ BCP/CSV scalars; fail-closed wrong format | hermetic | PASS |
| Contract | matrix profile mysql→clickhouse | hermetic | PASS |
| Integration | full_refresh + incremental_merge accumulation | Docker mysql+ch | PASS or SKIP |
| Compatibility | mysql→mssql / mysql→postgres unchanged | hermetic + existing IT | PASS |

## Documentation plan

- New: this spec; optional `examples/batch/landing_mysql_to_clickhouse.batch.yaml`
- Update: `docs/source-sink/mysql-to-clickhouse.md`, matrix, type-mapping,
  certification, CHANGELOG, example yaml

## Rollout and rollback

Additive claim promotion. Rollback = revert PR.

## Agent execution plan

| Role | Owned paths | Forbidden |
|---|---|---|
| Integrator | CHANGELOG, matrix docs, shared type registry | unrelated Airflow UX |
| Runtime | mysql export/schema for CH; `mysql_clickhouse.py`; matrix service | MySQL sink; CDC |
| Tests | mysql contracts + new IT | live without Docker |
| Docs | mysql-to-clickhouse guide, examples | false Batch claims for kafka/bq |

## Approval checklist

- [x] User problem and CJM are clear.
- [x] Algorithm and failure semantics are implementable without guessing.
- [x] Public contracts and compatibility are explicit.
- [x] Architecture and alternatives are justified.
- [x] Relevant market research uses current official sources.
- [x] Claimed differentiation is measurable.
- [x] Tests, evidence, docs, rollout, and rollback are complete.
- [x] Path ownership and integration plan are conflict-safe.
- [x] Maintainer changed status to `APPROVED` (2026-07-21).
- [x] Implementation evidence (2026-07-21): hermetic
  `tests/test_runtime_mysql_contracts.py` PASS; Docker IT
  `tests/integration/mysql/test_mysql_to_clickhouse_native_transfer_integration.py`
  PASS with `DPONE_RUN_INTEGRATION=1` (mysql + clickhouse).
