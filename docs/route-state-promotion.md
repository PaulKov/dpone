# Route state promotion

Route state promotion is the self-service gate that advances source state only
after a verified route execution ledger and durable sink commit receipt agree.
It is the control-plane step that turns a successful load, CDC window, repair,
or resync into a safe source checkpoint.

The command does not read sources or write sinks. It reads evidence, evaluates a
fail-closed policy, and writes promoted source state through a local or SQLite
state store.

## When to use it

- After `loaded_to_staging`, `finalized`, or `quality_checked` is recorded in
  [Route execution ledger](route-execution-ledger.md).
- Before recording a `state_committed` route execution step.
- Before advancing xmin, LSN, offset, cursor, or any source checkpoint.
- During replay, repair, or resync when operators need proof that state movement
  is idempotent.

## User workflow

1. Generate or locate `route_execution_ledger.json`.
2. Capture the durable sink commit receipt: source boundary, sink boundary,
   commit token, target, row/event counts, idempotency key, and fencing token.
3. Run `dpone ops route-state-promote`.
4. Review `state_promotion.json` in automation and `state_promotion.md` in
   release or incident review.
5. Attach `state_promotion` evidence to [Route readiness](route-readiness.md).
6. Record the matching `state_committed` route execution step only after
   promotion succeeds.

## CLI example

```bash
uv run dpone ops route-state-promote \
  --source mssql \
  --sink clickhouse \
  --strategy cdc \
  --dataset dbo.orders \
  --run-id orders-cdc-2026-06-13T09-00Z \
  --ledger-json test_artifacts/route_execution/mssql_to_clickhouse/orders/route_execution_ledger.json \
  --proposed-state "lsn:0x00000000000000000020" \
  --source-boundary "lsn:0x00000000000000000010..0x00000000000000000020" \
  --sink-boundary "clickhouse:analytics.orders_cdc:events:1500" \
  --idempotency-key "dbo.orders:promote:0x20" \
  --commit-token "clickhouse-partition-20260613-0001" \
  --target analytics.orders_cdc \
  --fencing-token "mssql_to_clickhouse__cdc:dbo.orders:cdc-worker-1:1781350000000000" \
  --rows-applied 1500 \
  --events-applied 1500 \
  --state-backend sqlite \
  --state-uri test_artifacts/route_state/shared/route_state_store.sqlite3 \
  --output-dir test_artifacts/route_state/mssql_to_clickhouse/orders \
  --format json
```

## Commit receipt

The commit receipt is the factual sink-side proof used for source state
promotion.

| Field | Meaning |
| --- | --- |
| `proposed_state` | Source state to persist, such as an LSN, xmin, offset, or cursor. |
| `source_boundary` | Source window that was applied. Must match durable ledger evidence. |
| `sink_boundary` | Sink receipt boundary. Must match durable ledger evidence. |
| `commit_token` | Durable sink receipt token, partition id, transaction id, or apply id. |
| `target` | Target table, log, stream, or checkpoint namespace. |
| `fencing_token` | Lease fencing token from the route execution ledger when a lease was used. |
| `idempotency_key` | Stable identity of this source-state side effect. |

## State stores

| Backend | Use case | Coordination behavior |
| --- | --- | --- |
| `local_json` | Default local evidence and single-runner CI. | Best-effort compare-and-swap over `route_state_store.json`. |
| `sqlite` | Shared local runners, Docker-live certification, and release rehearsal state. | Atomic compare-and-swap writes with SQLite `BEGIN IMMEDIATE`. |

## Report contract

The command writes:

| Artifact | Purpose |
| --- | --- |
| `state_promotion.json` | Stable schema `dpone.route_state_promotion.v1` with receipt, blockers, warnings, previous state, and promoted state. |
| `state_promotion.md` | Human-readable promotion summary and operator runbook. |
| `route_state_store.json` | Local JSON promoted source-state store when `--state-backend local_json` is used. |
| `route_state_store.sqlite3` | SQLite promoted source-state store when `--state-backend sqlite` is used without `--state-uri`. |

## Operator runbook

| Symptom | Action |
| --- | --- |
| `state_promotion.ledger_not_passed` | Fix the route execution ledger before promoting source state. |
| `state_promotion.missing_durable_sink_success` | Record a successful `loaded_to_staging`, `finalized`, or `quality_checked` ledger step. |
| `state_promotion.boundary_mismatch` | Use a commit receipt whose source and sink boundaries match durable ledger evidence. |
| `state_promotion.fencing_token_mismatch` | Use the active ledger lease fencing token or rerun route execution with a valid lease. |
| `state_promotion.idempotency_conflict` | Use a new idempotency key for a new state transition or replay the exact same receipt. |
| `state_promotion.concurrent_write_conflict` | Re-read the state store and retry against the latest version. |

## Route readiness

`state_promotion` is a generic route readiness evidence domain. Add
`state_promotion.json` to `dpone ops route-readiness` after route execution
ledger evidence and before release promotion.
