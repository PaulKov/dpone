# Manifest basics

dpone pipelines are driven by YAML manifests. A manifest says where data comes from, where it goes, how it should be loaded, and where state should be stored.

## Minimal shape

```yaml
source:
  type: postgres
  connection_id: postgres_oltp
  connection_type: env
  table:
    schema: public
    name: orders

sink:
  type: mssql
  connection_id: mssql_dwh
  connection_type: env
  table:
    schema: landing
    name: orders
  strategy:
    mode: incremental_append
```

## Core sections

| Section | Purpose |
| --- | --- |
| `source` | Extract data from a database, API, or Kafka topic. |
| `sink` | Load rows into a database, warehouse, or Kafka topic. |
| `state` | Store offsets, XMin cursors, CDC offsets, Kafka offsets, and run metadata. |
| `quality` | Run checks such as not-null, uniqueness, freshness, and row-count deltas. |
| `observability` | Configure run artifacts, OpenTelemetry, and Prometheus output. |

## Strategy section

```yaml
sink:
  strategy:
    mode: incremental_merge
    unique_key: order_id
```

Common strategies:

| Strategy | Meaning |
| --- | --- |
| `full_refresh` | Replace the target dataset with the current source snapshot. |
| `incremental_append` | Append only new rows. |
| `incremental_merge` | Upsert rows by unique key. |
| `replace` | Replace a configured target slice. |
| `partition_replace` | Replace target partitions represented by staged partition values. |
| `xmin` | PostgreSQL XMin incremental extraction. |
| `cdc` | Consume source change data capture when enabled. |

Read [Load strategies](../load-strategies.md) for exact semantics and source/sink support.

## Schema evolution defaults

Schema evolution is enabled by default for safe changes:

```yaml
sink:
  options:
    schema_evolution:
      enabled: true
      on_breaking: fail
      on_type_change: fail
```

Use `on_type_change: new_column` only when you intentionally want incompatible source values written to `__dpone__nc__<column>`.

## Batch manifests

Variant C batch manifests let one YAML file describe many related processes with defaults, variables, and overrides.

```yaml
layer_defaults:
  target_schema: landing

vars:
  batch_size: 50000

schemas:
  - source_schema: public
    tables:
      - source_table: orders
      - source_table: customers
```

Schema: [src/dpone/schema/etl-batch-manifest.schema.json](https://github.com/PaulKov/dpone/blob/master/src/dpone/schema/etl-batch-manifest.schema.json)

Deep dive: [Variant C manifests](../manifests-variant-c.md)

## Validation and planning

```bash
dpone manifest validate manifests/orders.yaml
dpone plan manifests/orders.yaml
```

Use `plan` before the first write to inspect source queries, staging tables, schema evolution DDL, reconciliation behavior, state transitions, and quality gates.
