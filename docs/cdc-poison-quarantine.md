# CDC poison quarantine and replay

CDC poison quarantine keeps replication-grade routes moving safely when one CDC
event cannot be applied with the current stream contract. The runtime classifies
known-bad events, writes a durable quarantine artifact, and either fails closed
or continues with clean events according to the selected policy.

The first live target for this workflow is `mssql -> clickhouse`, but the
contract is source and sink neutral. It is built around `CDCBatch`,
`CdcRuntimeStream`, `CdcSinkApplier`, and stable JSON artifacts instead of
route-specific command branches.

## When to use it

Use poison quarantine when a CDC batch may contain events that should not reach
the sink before investigation:

- missing unique-key values;
- unsupported CDC operations;
- duplicate event identities inside the same runtime window;
- route-specific failures that a future injected policy marks replayable.

The default runtime mode is fail-closed. It writes the quarantine artifact and
does not apply clean events or advance offsets. Use
`--poison-mode quarantine_and_continue` only after the route runbook says that
clean events can be applied while the quarantined records are investigated.

## Runtime command

Run one bounded local runtime tick and fail closed on poison events:

```bash
dpone ops cdc-runtime-run \
  --source mssql \
  --sink clickhouse \
  --backend mssql_change_tracking \
  --pipeline-name orders-cdc \
  --source-schema dbo \
  --source-table orders \
  --target-dataset analytics.orders_cdc \
  --unique-key order_id \
  --events-json test_artifacts/cdc_runtime/orders/events.json \
  --checkpoint-json test_artifacts/cdc_runtime/orders/checkpoint.json \
  --output-dir test_artifacts/cdc_runtime/orders \
  --format json
```

Allow clean events to apply while poison events are quarantined:

```bash
dpone ops cdc-runtime-run \
  --source mssql \
  --sink clickhouse \
  --backend mssql_change_tracking \
  --pipeline-name orders-cdc \
  --source-schema dbo \
  --source-table orders \
  --target-dataset analytics.orders_cdc \
  --unique-key order_id \
  --events-json test_artifacts/cdc_runtime/orders/events.json \
  --checkpoint-json test_artifacts/cdc_runtime/orders/checkpoint.json \
  --poison-mode quarantine_and_continue \
  --output-dir test_artifacts/cdc_runtime/orders \
  --format json
```

The runtime writes `cdc_poison_quarantine.json` next to
`cdc_runtime_run.json` when poison records exist.

## Inspect a quarantine

Use `dpone ops cdc-quarantine-inspect` before replaying or closing an incident:

```bash
dpone ops cdc-quarantine-inspect \
  --quarantine-json test_artifacts/cdc_runtime/orders/cdc_poison_quarantine.json \
  --output-dir test_artifacts/cdc_runtime/orders/inspection \
  --format json
```

The command writes `cdc_quarantine_inspection.json` and
`cdc_quarantine_inspection.md` with reason counts and action counts.

## Replay quarantined events

Use `dpone ops cdc-replay-execute` after fixing the source data, route policy,
or sink-side capability that caused the quarantine. Replay execution applies
only replayable quarantined events and does not mutate CDC offsets.

Local dry replay into a credential-free sink artifact:

```bash
dpone ops cdc-replay-execute \
  --mode local \
  --source mssql \
  --sink clickhouse \
  --backend mssql_change_tracking \
  --pipeline-name orders-cdc \
  --source-schema dbo \
  --source-table orders \
  --target-dataset analytics.orders_cdc \
  --unique-key order_id \
  --quarantine-json test_artifacts/cdc_runtime/orders/cdc_poison_quarantine.json \
  --output-dir test_artifacts/cdc_runtime/orders/replay \
  --format json
```

Live replay to ClickHouse:

```bash
dpone ops cdc-replay-execute \
  --mode live \
  --source mssql \
  --sink clickhouse \
  --backend mssql_change_tracking \
  --pipeline-name orders-cdc \
  --source-schema dbo \
  --source-table orders \
  --target-dataset analytics.orders_cdc \
  --unique-key order_id \
  --sink-connection-id clickhouse-prod \
  --credentials-source env \
  --quarantine-json test_artifacts/cdc_runtime/orders/cdc_poison_quarantine.json \
  --output-dir test_artifacts/cdc_runtime/orders/replay \
  --format json
```

Replay writes `cdc_replay_execution.json` and `cdc_replay_execution.md`. For
ClickHouse CDC logs, the sink applier skips already-present event hashes and
reports `duplicate_events_skipped` in the sink receipt metrics. This makes the
replay executor safe to rerun after a partial operator retry.

## Artifacts

| Artifact | Schema | Purpose |
| --- | --- | --- |
| `cdc_poison_quarantine.json` | `dpone.cdc_poison_quarantine.v1` | Durable poison records, event identity, original change payload, replayability, and blocker summary. |
| `cdc_poison_quarantine.md` | Markdown | Human incident review for the same quarantine. |
| `cdc_quarantine_inspection.json` | `dpone.cdc_quarantine_inspection.v1` | Reason/action counts for runbooks and dashboards. |
| `cdc_quarantine_inspection.md` | Markdown | Operator-readable inspection summary. |
| `cdc_replay_execution.json` | `dpone.cdc_replay_execution.v1` | Replay result, sink receipt, blocker list, and `committed=false` offset safety flag. |
| `cdc_replay_execution.md` | Markdown | Operator-readable replay summary. |

## Runbook

1. Open `cdc_runtime_run.json` and confirm whether the runtime used
   `fail_closed` or `quarantine_and_continue`.
2. Open `cdc_poison_quarantine.json` and group records by `reason`.
3. For `cdc_poison.unique_key_missing`, fix the source row or stream key mapping
   before replay.
4. For `cdc_poison.unsupported_operation`, verify that the reader emitted an
   operation supported by the sink apply contract.
5. For `cdc_poison.duplicate_event`, compare the CDC reader window and replay
   boundary. ClickHouse replay can skip already-written hashes, but the reader
   should not emit duplicates as normal behavior.
6. Run `dpone ops cdc-quarantine-inspect` and attach
   `cdc_quarantine_inspection.json` to the incident.
7. Run `dpone ops cdc-replay-execute --mode local` against the quarantine.
8. Run the opt-in live replay check only after local replay is green.
9. Confirm `cdc_replay_execution.json` has `passed=true`,
   `committed=false`, and a durable sink receipt.
10. Resume normal runtime ticks with `dpone ops cdc-runtime-run`.

## Local and Docker-live verification

Default OSS CI remains credential-free and runs local tests for classifier,
quarantine, replay, CLI contracts, and docs contracts.

Run the live MSSQL + ClickHouse gate only when Docker services are explicitly
started and `DPONE_RUN_INTEGRATION=1` is set:

```bash
docker compose -f docker/docker-compose.integration.yml up -d mssql clickhouse

DPONE_RUN_INTEGRATION=1 \
DPONE_IT_MSSQL_HOST=127.0.0.1 \
DPONE_IT_MSSQL_PORT=51433 \
DPONE_IT_MSSQL_PASSWORD='Dp0ne.Strong.Pw.2026!' \
DPONE_IT_MSSQL_TRUST_SERVER_CERTIFICATE=yes \
DPONE_IT_CH_HOST=127.0.0.1 \
DPONE_IT_CH_PORT=59000 \
DPONE_IT_CH_HTTP_PORT=58123 \
DPONE_IT_CH_DATABASE=dpone_it \
DPONE_IT_CH_USER=default \
DPONE_IT_CH_PASSWORD=dpone \
uv run pytest tests/integration/mssql/test_mssql_clickhouse_live_cdc_runtime_integration.py -q
```

The live gate verifies SQL Server Change Tracking reads, ClickHouse durable CDC
apply, offset commit safety, and duplicate replay protection in the
ClickHouse CDC log.
