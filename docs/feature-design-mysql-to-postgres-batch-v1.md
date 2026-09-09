# Feature design: MySQL → Postgres batch route v1

- Status: IMPLEMENTED
- Owner: dpone maintainers
- Issue: follow-up to #395 (mysql→mssql); promote mysql→postgres
- Target release: next minor after implementation merges
Last verified: 2026-07-21
Approval: maintainer APPROVED 2026-07-21
Evidence: hermetic `tests/test_runtime_mysql_contracts.py` +
`test_postgres_consume_with_staging_materializes_file_export_artifact`; Docker IT
`tests/integration/mysql/test_mysql_to_postgres_native_transfer_integration.py`
(full_refresh CSV→staging + incremental_merge watermark with 2→3 row accumulation)

## Executive summary

After #395, dpone has a MySQL **source family** and one certified batch route
(`mysql → mssql`). Operators who land MySQL OLTP into **Postgres** still see
**Experimental** matrix/guide status: there is no type profile, no
Postgres-safe CSV export wire (current `export_format=csv` still formats through
MSSQL BCP helpers), no Docker IT, and no Batch ETL claim.

This feature promotes `mysql → postgres` to **Batch ETL supported** by adding
`mysql_to_postgres_native_v1`, sink-aware true CSV export into the existing
Postgres `COPY … FROM STDIN` staging stack, hermetic contracts, and Docker
`integration_mysql` × Postgres live evidence. Binlog CDC and MySQL sink remain
out of scope.

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
|---|---|---|---|
| Data engineer | Land MySQL tables into Postgres with merge/append/refresh | Experimental only; risk of BCP-shaped CSV into COPY | Example plan/run with `dpone[mysql,postgres]` |
| Platform engineer | Same MySQL Airflow conn as mssql route | Docs warn “not certified” | Matrix Batch ETL + doctor/plan green |
| Operator | Trust incremental watermark | No live evidence | Docker IT full_refresh + incremental_merge PASS |

Journey: matrix guide → install extras → connections →
`examples/batch/landing_mysql_to_postgres.batch.yaml` → `dpone plan` →
`dpone batch run` → quality → staging-first recovery on failure.

## Scope

### In scope

- Type profile `mysql_to_postgres_native_v1` + `PairTypeMatrixService` pair
- Sink-aware MySQL export: **true CSV** for Postgres (csv module / BulkTextCodec
  CSV mode) **without** `format_mssql_bcp_scalar`; preserve `mssql-delimited` for
  mysql→mssql unchanged
- Default/public guidance: `export_format: csv` for mysql→postgres
- Strategies via existing Postgres sink: `full_refresh`, `incremental_append`,
  `incremental_merge`, `replace`, `partition_replace`, `snapshot_reconciliation`
- Watermark/`incremental_column` extract; state on Postgres target (same pattern
  as mssql→postgres / postgres→postgres examples)
- Docs/matrix/type-mapping/certification/example/CHANGELOG promotion
- Hermetic contracts + Docker IT (`integration_mysql` + existing Postgres IT)

### Non-goals

- Named native fast-path catalog entry (optional stretch; default
  `streaming_rows` + file→COPY is enough for Batch claim parity with
  mssql→postgres)
- MySQL sink/state, binlog/GTID CDC
- Six-dim attestation / Conformance Lab / safe-sample production I/O
- Promoting clickhouse/kafka/bigquery MySQL rows in the same PR
- Claiming delete-correctness from cursor incremental alone

### Assumptions and constraints

- Reuse ADR 0005 staging-first Postgres COPY contracts; no new ADR
- MySQL source from #395 is reused; change is export path + types + evidence
- Optional deps already exist: `dpone[mysql]` + `dpone[postgres]`

## Approaches considered

| Approach | Pros | Cons | Decision |
|---|---|---|---|
| **A. CSV + existing Postgres COPY staging** (recommended) | Matches mssql→postgres Batch bar; small diff; uses proven sink | No fancy named fast path | **Adopt** |
| B. Named `mysql_stream_to_postgres_copy` fast path | Advise symmetry with mysql→mssql | Extra strategy-intelligence surface; not required for Batch claim | Defer / stretch |
| C. Row-by-row INSERT only | Simple | Rejected industrially; poor throughput | **Reject** |
| D. Wait for MySQL sink first | Broader family | Blocks Postgres landing users who already have MySQL source | **Reject for this PR** |

## Public contract

### CLI

No new commands. Existing `plan` / `batch run` / `run` / connection-doctor.

### Python API

No new top-level public packages. New type mapper module under
`dpone.type_system.source_sink`. Connector may gain
`export_csv_to_file` (or parameterized export) without changing legacy namespaces.

### Manifest/schema

No new `source.type`. Recommended mysql→postgres options:

```yaml
source:
  type: mysql
  options:
    export_format: csv
    incremental_column: updated_at
sink:
  type: postgres
  strategy:
    mode: incremental_merge
```

`export_format: mssql-delimited` on a postgres sink must fail closed or be
rejected at plan/capability negotiation (do not silently load BCP text via COPY).

### Artifacts and evidence

Reuse `FileExportArtifact` format `csv` and Postgres staging evidence.
Docker IT: PASS or explicit SKIP; never treat SKIP as PASS.

### Compatibility and migration

Additive for operators. **Behavior fix:** `export_format=csv` must stop using
MSSQL BCP scalar formatting (correctness bug for any csv consumer). mysql→mssql
`mssql-delimited` path must remain bit-compatible.

## Detailed algorithm

1. Validate manifest: `source.type=mysql`, `sink.type=postgres`.
2. Resolve MySQL + Postgres credentials; construct connectors lazily.
3. Negotiate export: require Postgres-safe `csv` (default for this pair).
4. Map schema via `mysql_to_postgres_native_v1` (bool/`tinyint(1)`, unsigned
   widen, datetime→timestamp, JSON→jsonb, ENUM/SET/spatial → documented
   fail-closed or text contract).
5. Extract full or watermark (`incremental_column >` Postgres MAX / state).
6. Stream SELECT → true CSV file/stream with authoritative `rows_exported`.
7. Postgres staging `COPY … FROM STDIN (FORMAT csv)` + set-based finalize.
8. Quality checks; commit watermark on Postgres only after sink success.
9. On failure: existing Postgres staging rollback; no watermark advance.

### Pseudocode

```text
creds_src, creds_sink = resolve(...)
src = MySQLSource(MySQLConnector(creds_src), state=postgres_state)
extract = src.extract(load_config)  # full or watermark
artifact = export_csv(extract, type_profile=mysql_to_postgres_native_v1)
sink = PostgresSink(...)
result = sink.load(load_config, artifact)  # COPY staging + finalize
if result.ok:
    state.commit(watermark)
```

### State machine

Same staging-first machine as mysql→mssql / postgres sinks:
`Planned → Extracting → Staging → Finalizing → Quality → Succeeded`, with
failure returning to safe retry without watermark advance.

### Edge cases

- Empty extract: success, zero rows; watermark unchanged
- Missing incremental column for incremental modes: fail closed
- Unknown `columns`: fail closed (parity with #395)
- `export_format=mssql-delimited` + postgres sink: fail closed
- Optional dep missing: clear `dpone[mysql]` / `dpone[postgres]` message
- Cold start: missing target table tolerated for watermark bootstrap (parity #395)

## Architecture

| Component | Change | Responsibility |
|---|---|---|
| `MySQLConnector` / mysql strategies | extend | Sink-aware CSV vs mssql-delimited export |
| `MySQLPostgresTypeMapper` | new | `mysql_to_postgres_native_v1` |
| `PairTypeMatrixService` | extend | Register mysql×postgres |
| `PostgresStagingManager` | reuse | COPY CSV load |
| Docs/examples/CHANGELOG | update | Batch ETL claim |

### ADR

Not required — within ADR 0005 + existing Postgres COPY contracts.

### Quality budget

New mapper module under `max_sloc: 400`; keep export split by format, not
`part_1`/`part_2` hacks.

## Market comparison

Research date: 2026-07-21. Official / primary docs.

| System | Relevant capability | Observed design | Strength | Limitation | Adopt/reject | Source |
|---|---|---|---|---|---|---|
| Airbyte MySQL → Postgres | CDC binlog + Standard cursor; Postgres Full Refresh / Incremental Append(+Deduped) | Sync mode = source method × dest write | Delete-aware CDC; chunking | Cursor weak on deletes | Adopt full/cursor/append/merge map; **defer CDC** | [Airbyte MySQL](https://github.com/airbytehq/airbyte/blob/master/docs/integrations/sources/mysql.md), [Airbyte Postgres dest](https://github.com/airbytehq/airbyte/blob/master/docs/integrations/destinations/postgres.md) |
| dlt sql_database | Cursor incremental + replace/append/merge to postgres | SQLAlchemy/PyMySQL extract, destination dispositions | Closest OSS batch peer | No opinionated Postgres COPY staging contract | Adopt cursor + merge dispositions | [dlt sql_database](https://dlthub.com/docs/dlt-ecosystem/verified-sources/sql_database/configuration) |
| PostgreSQL COPY | `COPY … FROM STDIN` FORMAT csv | Server-side bulk load via client stream | Canonical bulk path; non-superuser STDIN | CSV NULL/empty-string rules | **Adopt** STDIN CSV into staging | [PostgreSQL 18 COPY](https://www.postgresql.org/docs/18/sql-copy.html) |
| Fivetran MySQL | Binlog / Teleport | Managed warehouse sync | Delete-aware | Not self-hosted COPY wire | Defer binlog; adopt fail-closed method semantics | fivetran.com docs (same posture as #395) |
| Informatica / SSIS | Bulk load utilities | Enterprise bulk | Throughput | Package-centric | Light adopt bulk+staging | N/A deep product parity |
| Apache Beam / gusty / Cosmos | JdbcIO / DAG / dbt | Not MySQL→Postgres EL product | — | — | **N/A** | — |

## Measurable differentiation

```yaml
axis: staging-first MySQL→Postgres with Postgres-safe CSV and explicit type profile
scenario: MySQL OLTP → Postgres landing, incremental_merge, COPY staging
baseline: ad-hoc mysqldump/psql or cursor ELT without typed matrix + staging evidence
metric: idempotent rerun correctness + authoritative rows_exported
target: correct merge on rerun; Docker IT PASS when mysql+postgres available
procedure: integration_mysql × postgres IT markers; SKIP if unavailable
artifact: pytest integration evidence
limitations: no binlog delete SLO; no named fast-path advise band in v1
```

## Security, privacy, and operations

- Existing credential resolvers; never log secrets
- TLS extras from #395 unchanged
- Runbook: `pip install "dpone[mysql,postgres]"`

## Test and certification plan

| Layer | Scenario | Environment | Expected |
|---|---|---|---|
| Unit | type map unsigned/bool/json; csv ≠ bcp scalars | hermetic | PASS |
| Contract | matrix profile mysql→postgres; export format fail-closed | hermetic | PASS |
| Integration | full_refresh + incremental_merge | Docker mysql+postgres | PASS or SKIP |
| Compatibility | mysql→mssql bcp path unchanged | hermetic + existing IT | PASS |
| Live vendor | optional | UNVERIFIED if missing | |

## Documentation plan

- New: this spec; `examples/batch/landing_mysql_to_postgres.batch.yaml`
- Update: `docs/source-sink/mysql-to-postgres.md`, matrix, type-mapping,
  certification, CHANGELOG, example yaml

## Rollout and rollback

Additive claim promotion. Rollback = revert PR. No migration of existing
mssql routes. Operators on Experimental manifests with `export_format: csv`
gain **correct** CSV (breaking only the previous incorrect BCP-shaped csv —
treated as bugfix).

## Agent execution plan

| Role | Owned paths | Forbidden |
|---|---|---|
| Integrator | CHANGELOG, matrix docs, mkdocs if needed, shared type registry | unrelated Airflow UX |
| Runtime | mysql connector/strategies export split; `mysql_postgres.py`; schema_type_matrix | MySQL sink; CDC |
| Tests | `tests/test_runtime_mysql_contracts.py`, new IT | live without Docker |
| Docs | mysql-to-postgres guide, examples | claiming other sinks Batch |

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
