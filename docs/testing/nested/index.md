# Nested normalization testing

Nested normalization has two test layers:

| Layer | Command | Purpose |
|---|---|---|
| Unit/contract | `uv run pytest tests/test_nested_normalization_contracts.py tests/test_nested_normalization_industrial.py tests/test_nested_normalization_runtime_hardening.py tests/test_nested_normalization_industrial_layer.py` | Validates hierarchy IDs, path policies, file artifacts, spill-to-disk, child finalizers, snapshot state, SQL state, native fast-path handoff, child quality, lint, certification and benchmark contracts. |
| Manual evidence | `uv run dpone normalize certify --output-dir test_artifacts/nested/latest` | Writes JSON/Markdown evidence for local release review. |
| Benchmark evidence | `uv run dpone normalize benchmark --row-count 10000 --output-dir test_artifacts/nested/benchmark-latest` | Measures local nested normalization throughput and generated row counts. |
| Stress evidence | `uv run dpone normalize stress --row-count 100000 --output-dir test_artifacts/nested/stress-latest` | Generates skewed nested payload evidence with sparse objects and large child arrays. |
| Live harness | `NestedLiveCertificationRunner(...).run(output_dir=...)` | Runs per-sink live probes against local MSSQL/Postgres/ClickHouse/Kafka/BigQuery-compatible targets when available. |

## What data is generated

The certification and benchmark services generate deterministic nested order
payloads with:

- one root `orders` row per generated order;
- nested `customer` object;
- nested `items` array with two line rows per order;
- nested scalar `tags` array;
- optional `internal_payload` path routed to quarantine.

Default benchmark size is `10000` root rows. The CLI caps benchmark input at
`100000` rows by default to avoid accidental laptop-scale memory or disk abuse.

## Failure runbooks

### Lint fails with `nested.table_collision`

Two or more `split_paths` write to the same table. Give every independent
nested entity a unique `table` value, then rerun:

```bash
dpone normalize lint --root-table orders --config examples/nested/normalization.yml --fail-on-error
```

### Lint warns with `nested.guardrails_missing`

Add at least one production guardrail:

```yaml
guardrails:
  max_child_tables: 16
  max_rows_per_root: 10000
  max_array_length: 1000
```

### Certification fails reverse readback

Check whether a custom path policy preserved, ignored, or quarantined data that
your test expects to round-trip. Reverse readback is intended for modeled child
tables, not intentionally ignored/quarantined paths.

### Benchmark is slow

Use spill-to-disk mode for large payloads and inspect generated row counts. If
one root row creates too many child rows, add stricter `max_array_length` and
`max_rows_per_root` guardrails.

## Child lifecycle tests

The industrial contract suite also validates child lifecycle behavior:

```bash
uv run pytest tests/test_nested_normalization_industrial.py::test_child_unique_key_keeps_row_identity_stable_when_array_reorders
uv run pytest tests/test_nested_normalization_industrial.py::test_child_reconciliation_detects_physical_deletes_by_child_unique_key
uv run pytest tests/test_nested_normalization_industrial.py::test_nested_load_service_can_materialize_child_tables_from_spill_files
uv run pytest tests/test_nested_normalization_industrial_layer.py
```

These tests prove that:

- child `__dpone__row_id` is stable when arrays reorder;
- deleted child keys are detected from previous/current snapshots;
- sink-native child delete finalizers render staging-first SQL/events for MSSQL,
  Postgres, BigQuery, ClickHouse and Kafka;
- committed/staged child snapshots do not advance state until commit;
- SQL-backed child snapshot stores render durable stage/commit/rollback
  contracts;
- manifest options expose `child_snapshot_store`, `atomicity` and
  `child_quality` policies;
- native spill formats support JSON Lines, ClickHouse-style `json_each_row` and
  strict TSV with unsafe delimiter detection;
- runtime `materialization: spill_to_disk` loads generated tables through
  sink-native `FileExportArtifact` paths when supported and streaming fallback
  otherwise;
- package lifecycle marks failed loads without committing source/state;
- child quality gates detect duplicate child keys and orphan child rows.

## Manual live readiness and certification expectations

GitHub Actions workflow:

```text
.github/workflows/nested-live-certification.yml
```

The workflow is manual (`workflow_dispatch`) and runs the
`integration_nested_live` readiness marker after starting local Postgres,
MSSQL, ClickHouse and Kafka-compatible services. The marker currently proves
service connectivity and fail-closed evidence handling only. It reports
`unverified`, never `passed`, until a case records all required real data-path
checks.

Live nested certification is intentionally manual because it depends on local
service availability. A complete live run should include one case per available
sink:

| Sink | Required local proof |
|---|---|
| MSSQL | TSV spill hands off to file artifact path and the MSSQL sink can route it to `bcp`; child snapshots commit only after success. |
| Postgres | TSV/CSV spill can route to `COPY`; failed child load leaves committed snapshot unchanged. |
| ClickHouse | `json_each_row` or TSV spill routes to ClickHouse native loading; child delete finalizer is lightweight-delete based and documented as eventually cleaned up. |
| Kafka | JSONL/NDJSON spill streams as keyed events; child deletes are keyed delete events, not table mutations. |
| BigQuery | JSONL/NDJSON spill routes to load-job compatible files; vendor credentials are optional and skipped when absent. |

If a live case is skipped, the artifact must say `skipped`, not `passed`.
If a service is available but a required proof fails, the artifact must say
`failed`. Any skipped or incomplete case makes the top-level result
`unverified`; a release-scoped run should include exactly the sinks required by
that release decision.
