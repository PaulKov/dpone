# MSSQL -> MSSQL

This guide is a copy/paste-ready starting point for loading data from **MSSQL** into **MSSQL** with `dpone`.

**Status:** Batch ETL supported

Identity profile `mssql_to_mssql_identity_v1` is hermetically contracted.
Vendor-live evidence uses a **wide** typed fixture across schemas
(`dpone_src` → `dpone_it`) and covers Docker MSSQL strategy mechanics. Current
generic admission allows complete/stateless `full_refresh`, `replace`,
`partition_replace`, and bounded `backfill`. Legacy
single-column `incremental_append`/`incremental_merge` evidence does not prove
a complete source boundary and those modes now fail closed. The fixture lives in
`tests/integration/mssql/test_mssql_to_mssql_vendor_live_integration.py`.
Binary columns use hex character BCP. Bare `tinyint` / legacy `datetime`
tokens remain hermetic-only (shared DDL mapper dialect collisions with
MySQL/ClickHouse spellings). See
[Route live wide certification](../testing/route-live-wide-certification.md).

### Vendor-live IT (manual)

```bash
docker compose -f docker/docker-compose.integration.yml up -d mssql
export DPONE_RUN_INTEGRATION=1 DPONE_RUN_INTEGRATION_LIVE=1
uv run pytest tests/integration/mssql/test_mssql_to_mssql_vendor_live_integration.py -q
```

## When to use this path

Use this path when MSSQL is the system of record or ingestion boundary and MSSQL is the landing, warehouse, event-log, or downstream replication target.

## Copy/paste manifest

```yaml
# yaml-language-server: $schema=../../src/dpone/schema/etl-batch-manifest.schema.json
kind: dpone.batch.v1

defaults:
  name: mssql_to_mssql_example
  source:
    type: mssql
    connection_id: mssql_source
    options:
      batch_size: 50000
      export_format: csv
  sink:
    type: mssql
    connection_id: mssql_target
    table:
      schema: dbo
      name: orders
    strategy:
      mode: full_refresh

quality:
  gates:
    - id: source_target_rows
      type: row_count_reconciliation
      severity: error
      tolerance:
        mode: pct
        value: 0.1

schemas:
  dbo:
    tables:
      - orders
```

Run it locally:

```bash
dpone plan examples/source-sink/mssql-to-mssql.yaml --format md
dpone run examples/source-sink/mssql-to-mssql.yaml
```

The checked source file is `examples/source-sink/mssql-to-mssql.yaml`; CI
compares its parsed YAML with this block.

If you change the strategy to `full_refresh` and empty output is invalid,
row-count reconciliation is not enough: it can pass a `0` source / `0` target
comparison. Add an explicit non-empty target gate:

```yaml
quality:
  gates:
    - id: target_min_rows
      type: min_rows
      side: target
      threshold: 1
      severity: error
```

## Supported load strategies

Wide vendor-live evidence covers the strategies below (see Status blurb and
manual IT command above). Notes describe runtime contracts.

| Strategy | Status | Notes |
|---|---|---|
| `full_refresh` | Supported | Uses staging first, then applies the target-specific finalization plan. |
| `incremental_append` | Fail-closed | Target-derived single-column `MAX + >` is not a complete source boundary. |
| `incremental_merge` | Fail-closed | A `unique_key` protects target identity but does not repair the source cursor. |
| `replace` | Supported | Uses staging first, then applies the target-specific finalization plan. |
| `partition_replace` | Supported | Replaces target partitions represented by staging `partition.column`; see Load strategies for native/fallback paths. |
| `snapshot_diff` | Not available from this source | `MSSQLSource` does not expose this strategy. |
| `scd2` | Not available from this source | `MSSQLSource` does not expose this strategy. |
| `backfill` | Supported | Bounded predicate reload; vendor-live certifies inner `replace`. |

See [Load strategies](../load-strategies.md) for the detailed algorithm for each strategy.
MSSQL CDC is a source capability, not a load strategy. It uses typed CDC
offsets and advances source state only after sink success; certify the exact
route and environment before enabling it.

## Runtime algorithm

This sink does not currently implement `StagedLoadPort`, so this route records
`governance_finalization=legacy_post_finalize`. Blocking gates run only after
the sink has mutated or finalized the target. A failure prevents source-state
advancement but cannot roll back that target mutation; inspect and repair or
deduplicate the target before retrying. See
[Load governance](../load-governance.md#runtime-lifecycle).

```mermaid
flowchart TD
    A["Resolve manifest and registry entries"] --> B["Create MSSQL source"]
    B --> C["Plan bounded extract"]
    C --> D["Read through BCP queryout or pyodbc streaming cursor"]
    D --> E["Emit ExtractResult with schema and artifact"]
    E --> F["Plan schema evolution"]
    F --> G["Create MSSQL staging or event batch"]
    G --> H["Load through BCP into staging followed by set-based MERGE/INSERT/REPLACE"]
    H --> I["Apply finalization strategy"]
    I --> J["Run quality and reconciliation checks (legacy_post_finalize)"]
    J --> K["Advance state only after success"]
```

## Strategy behavior

- `full_refresh`: extract the selected source boundary, load into staging, and replace the target according to the target's safe finalization path.
- `incremental_append` / `incremental_merge`: rejected by generic MSSQL
  admission before target or source I/O when they use the built-in
  single-column target watermark.
- `replace`: reload a bounded predicate window through staging and then atomically replace the matching target slice.
- `snapshot_diff`: not exposed by `MSSQLSource`.
- `scd2`: not exposed by `MSSQLSource`.
- `partition_replace`: extract a complete partition slice, load it into staging, and replace only partitions represented by `partition.column`.

Snapshot reconciliation is separate from the load strategy. Runtime planning
reports that capability as `reconciliation.mode=snapshot`; in the official
`dpone.batch.v1` authoring schema, enable it with `reconciliation: true`.

## Schema evolution and type mapping

Schema evolution is enabled by default and runs before the staging/final load path:

1. Read source schema from `ExtractResult.schema`.
2. Introspect the MSSQL target schema.
3. Apply safe additions and widening operations.
4. Fail breaking changes by default.
5. If configured, route incompatible type changes to `__dpone__nc__<column>`.

Use [Schema evolution](../schema-evolution.md) and [Type mapping matrix](../type-mapping-matrix.md) when adding columns or changing source types.

## Self-service golden path

Copy-paste CJM for the checked-in example (wide vendor-live certified route):

```bash
dpone doctor --profile local
pip install "dpone[mssql]"
dpone plan examples/source-sink/mssql-to-mssql.yaml --format md
dpone schema type-matrix --source mssql --sink mssql --format md
dpone run examples/source-sink/mssql-to-mssql.yaml
```

Landing convention (vault/GitOps-oriented): `examples/batch/landing_mssql_to_mssql.batch.yaml`.

See [Route live wide certification](../testing/route-live-wide-certification.md) for the maintainer vendor-live IT evidence path (SKIP ≠ PASS).

## Runbook

1. Start with `dpone doctor --profile local` and fix missing extras or native clients.
2. Run `dpone plan examples/source-sink/mssql-to-mssql.yaml --format md` and review source boundary, staging path, schema evolution, state, and quality gates.
3. Run a small bounded window first.
4. Inspect the run artifact under `.dpone/runs/mssql_to_mssql`.
5. Do not schedule the built-in single-column incremental route; use complete
   full or bounded replacement runs until a source-atomic cursor is available.
6. For delete-aware jobs, run reconciliation in report-only mode before enabling physical deletes.
7. Promote the manifest through GitOps after the plan and artifact are reviewed.

## Cross-links

- [Source -> sink matrix](../source-sink-matrix.md)
- [Load strategies](../load-strategies.md)
- [Schema evolution](../schema-evolution.md)
- [Type mapping matrix](../type-mapping-matrix.md)
- [Reconciliation and CDC](../cdc.md)
- [Performance guide](../performance.md)

## Type contracts and physical design

This flow supports the shared dpone type-governance stack:

- [Type inference](../type-inference.md) for source metadata, sampled profiling, confidence, and empty string vs `NULL` behavior.
- [Schema contracts](../schema-contracts.md) for explicit logical column types, enforcement modes, and `__dpone__nc__*` variant columns.
- [Physical design](../physical-design.md) for target-specific DDL such as concrete SQL types, indexes, partitioning, compression, ClickHouse `LowCardinality`, and BigQuery clustering.

Use `dpone schema infer --manifest ...` and `dpone schema physical-plan --manifest ...` before enabling new table DDL in production.
