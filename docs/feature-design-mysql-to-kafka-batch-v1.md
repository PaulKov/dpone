# Feature design: MySQL → Kafka batch/event-log route v1

- Status: IMPLEMENTED
- Owner: dpone maintainers
- Issue: follow-up to #395 / #396 / #401; promote mysql→kafka
- Target release: 0.73.8 (or next patch after merge)
Last verified: 2026-07-22
Approval: maintainer directed implementation + release 2026-07-22

## Executive summary

MySQL source is Batch for mssql/postgres/clickhouse. Landing into **Kafka**
remains Experimental: default MySQL export is `mssql-delimited`, KafkaSink
cannot read gzip, and there is no Docker IT evidence. This feature promotes
`mysql → kafka` to **Batch/event-log supported** (parity with postgres→kafka)
via sink-aware CSV defaults, fail-closed compress/BCP wire, hermetic contracts,
and Docker `integration_mysql` × `integration_kafka` live evidence.

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
|---|---|---|---|
| Data engineer | Stream MySQL rows to Kafka topics | Experimental; wrong wire risk | Example plan/run with `dpone[mysql,kafka]` |
| Platform engineer | Reuse MySQL + Kafka connections | Docs warn not certified | Matrix Batch/event-log |
| Operator | Trust append + watermark extract | No live evidence | Docker IT PASS |

Journey: matrix → extras → `examples/batch/landing_mysql_to_kafka.batch.yaml`
→ plan → run → consume topic.

## Scope

### In scope

- Sink-aware MySQL export default: `csv` when `sink_type=kafka`
- Fail-closed: `mssql-delimited` and `compress_export=true` for Kafka sinks
  (plan + runtime)
- Strategies already supported by KafkaSink (`incremental_append` primary;
  `full_refresh` / event_upsert merge as sink allows)
- Hermetic contracts + Docker IT (produce + consume ids)
- Docs/matrix/example/CHANGELOG claim flip
- No dedicated PairTypeMatrix profile (parity with postgres→kafka)

### Non-goals

- MySQL→BigQuery in this PR
- MySQL sink/state, binlog CDC
- Typed Kafka schema registry / Avro MVP
- PairTypeMatrix mysql_kafka suite
- Claiming exactly-once or delete-correctness from watermark alone

### Assumptions and constraints

- Reuse KafkaSink JSON + `envelope: dpone` path; no new ADR
- Optional deps: `dpone[mysql]` + `dpone[kafka]`
- Watermark via MySQL source max after extract (Kafka has no get_max)

## Approaches considered

| Approach | Pros | Cons | Decision |
|---|---|---|---|
| **A. CSV FileExportArtifact → KafkaSink JSON** | Matches postgres→kafka; local Docker IT | Values are stringly from CSV | **Adopt** |
| B. Stream InMemoryRows only | Avoids file wire | Breaks large-batch pattern; different from postgres→kafka | Reject |
| C. Keep mssql-delimited default | No export change | BCP text into csv.reader is unsafe | Reject (fail-closed) |

## Public contract

### Manifest/schema

Public `export_format: csv` (enum). Runtime rejects `mssql-delimited` and
`compress_export: true` when `sink.type=kafka`.

### Compatibility

Additive. mysql→mssql/postgres/clickhouse unchanged.

## Detailed algorithm

1. Validate `source.type=mysql`, `sink.type=kafka`, topic present.
2. Resolve export: default/force `csv`; fail if BCP or gzip.
3. Extract via MySQL full/incremental strategies → `FileExportArtifact(format=csv)`.
4. KafkaSink reads comma-delimited rows → JSON messages (optional dpone envelope).
5. Incremental watermark: cold start without sink max; advance from MySQL max.
6. Evidence: hermetic + Docker IT PASS or explicit SKIP.

## Market research (relevant only)

| System | Relevance | Note |
|---|---|---|
| Airbyte / Fivetran | CDC to Kafka | Binlog out of scope; watermark batch only |
| dlt | Load to Kafka | N/A for dpone staging-first claim |
| SSIS / Pentaho / Informatica / Beam / Cosmos / gusty | N/A | Not the Kafka event-log path under comparison |

Sources consulted: Apache Kafka docs (produce semantics), dpone postgres→kafka
guide (current repo, 2026-07-22). No unqualified “better” claim; differentiation
axis = fail-closed CSV wire + Docker IT for MySQL→Kafka parity with postgres→kafka.

## Measurable differentiation

```yaml
axis: fail-closed MySQL→Kafka CSV wire with Docker produce/consume evidence
scenario: MySQL OLTP → Kafka topic, incremental_append, json envelope
baseline: Experimental guide with mssql-delimited default risk
metric: IT consume_ids == seeded source ids; compress/BCP rejected
target: Docker IT PASS when mysql+kafka available
procedure: integration_mysql × integration_kafka markers
artifact: pytest integration evidence
```

## Test and certification plan

| Layer | Expected |
|---|---|
| Unit | fail-closed compress + mssql-delimited; default csv for kafka |
| Integration | Docker produce/consume PASS or SKIP |
| Release | include in 0.73.8 after CI + live IT |

## Agent execution plan

| Role | Owned paths |
|---|---|
| Runtime | `mysql_base.py`, `load_config_builder.py` kafka guards |
| Tests | mysql contracts + new mysql→kafka IT |
| Docs/integrator | matrix, guide, example, CHANGELOG, this spec |

## Approval checklist

- [x] User problem and CJM are clear.
- [x] Algorithm implementable without guessing.
- [x] Public contracts and compatibility explicit.
- [x] Maintainer directed APPROVED + release (2026-07-22).
