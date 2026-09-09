# Route execution ledger

The route execution ledger is the self-service audit trail for one
`source -> sink -> strategy` execution. It records what boundary was processed,
which durable sink step succeeded, which artifacts prove the step, and whether a
rerun is an idempotent replay or a conflicting side effect.

Use it whenever a route needs production-grade retry, resume, release, or
incident evidence. The command does not extract or load data. It records
control-plane evidence around work that has already happened or is about to be
claimed by a runner.

## When to use it

- Before promoting a route through [Route readiness](route-readiness.md).
- Before committing source state after a target load, CDC apply, repair, or
  resync.
- During incident review when a run must be replayed safely.
- In CI when a route-specific certification pack needs `route_execution_ledger`
  evidence.

## User workflow

1. Choose the route from the [source -> sink matrix](source-sink-matrix.md).
2. Record `planned` or `extracting` when a runner claims the route and dataset.
3. Record `loaded_to_staging` after the sink has durably accepted the batch,
   CDC window, or resync chunk.
4. Record `finalized` after the target commit or swap succeeds.
5. Record `quality_checked` after reconciliation, count, hash, or SLO evidence
   is attached.
6. Record `state_committed` only after durable sink success exists in the same
   ledger.
7. Attach `route_execution_ledger.json` to route readiness, release evidence, or
   incident review.

## CLI examples

Bulk route step for `postgres -> mssql`:

```bash
uv run dpone ops route-execution-ledger \
  --source postgres \
  --sink mssql \
  --strategy incremental_merge \
  --dataset public.orders \
  --run-id orders-2026-06-13T09-00Z \
  --stage loaded_to_staging \
  --status succeeded \
  --runner-id worker-a \
  --source-boundary "xmin:120000..120500" \
  --sink-boundary "mssql_staging:orders__run_20260613" \
  --idempotency-key "orders:xmin:120000:120500:staging" \
  --artifact native_transfer=test_artifacts/native/postgres_to_mssql/orders.json \
  --artifact quality=test_artifacts/reconciliation/postgres_to_mssql/orders.json \
  --lease-ttl-seconds 900 \
  --output-dir test_artifacts/route_execution/postgres_to_mssql/orders \
  --format json
```

CDC window for `mssql -> clickhouse`:

```bash
uv run dpone ops route-execution-ledger \
  --source mssql \
  --sink clickhouse \
  --strategy cdc \
  --dataset dbo.orders \
  --run-id orders-cdc-2026-06-13T09-00Z \
  --stage loaded_to_staging \
  --status succeeded \
  --runner-id cdc-worker-1 \
  --source-boundary "lsn:0x00000000000000000010..0x00000000000000000020" \
  --sink-boundary "clickhouse:analytics.orders_cdc:events:1500" \
  --idempotency-key "dbo.orders:cdc-window:0x10:0x20" \
  --artifact cdc_apply=test_artifacts/cdc_apply/mssql_to_clickhouse/orders/cdc_apply_certification.json \
  --artifact cdc_runtime=test_artifacts/cdc_runtime/mssql_to_clickhouse/orders/cdc_runtime_run.json \
  --lease-ttl-seconds 300 \
  --output-dir test_artifacts/route_execution/mssql_to_clickhouse/orders \
  --format json
```

Resync chunk after retention recovery:

```bash
uv run dpone ops route-execution-ledger \
  --source mssql \
  --sink clickhouse \
  --strategy cdc \
  --dataset dbo.orders \
  --run-id orders-resync-2026-06-13T10-00Z \
  --stage finalized \
  --status succeeded \
  --runner-id repair-worker-1 \
  --source-boundary "resync:chunk:orders:0004" \
  --sink-boundary "clickhouse:analytics.orders_cdc:repair_actions:42" \
  --idempotency-key "dbo.orders:resync:chunk:0004" \
  --artifact resync_execution=test_artifacts/cdc_resync/mssql_to_clickhouse/orders/cdc_resync_execution.json \
  --artifact repair=test_artifacts/cdc_compare_repair/mssql_to_clickhouse/orders/cdc_repair_execution.json \
  --output-dir test_artifacts/route_execution/mssql_to_clickhouse/orders \
  --format md
```

State commit after durable sink success:

```bash
uv run dpone ops route-execution-ledger \
  --source mssql \
  --sink clickhouse \
  --strategy cdc \
  --dataset dbo.orders \
  --run-id orders-cdc-2026-06-13T09-00Z \
  --stage state_committed \
  --status committed \
  --runner-id cdc-worker-1 \
  --source-boundary "lsn:0x00000000000000000020" \
  --sink-boundary "checkpoint:orders-cdc:0x00000000000000000020" \
  --idempotency-key "dbo.orders:checkpoint:0x20" \
  --output-dir test_artifacts/route_execution/mssql_to_clickhouse/orders \
  --format json
```

Shared SQLite backend for multiple local runners:

```bash
uv run dpone ops route-execution-ledger \
  --source mssql \
  --sink clickhouse \
  --strategy cdc \
  --dataset dbo.orders \
  --run-id orders-cdc-2026-06-13T09-00Z \
  --stage loaded_to_staging \
  --status succeeded \
  --runner-id cdc-worker-1 \
  --source-boundary "lsn:0x10..0x20" \
  --sink-boundary "clickhouse:analytics.orders_cdc:events:1500" \
  --idempotency-key "dbo.orders:cdc-window:0x10:0x20" \
  --lease-ttl-seconds 300 \
  --store-backend sqlite \
  --store-uri test_artifacts/route_execution/shared/route_execution_ledger.sqlite3 \
  --output-dir test_artifacts/route_execution/mssql_to_clickhouse/orders \
  --format json
```

## Idempotency key

The idempotency key is the stable identity of one intended side effect. Reusing
the same key with the exact same payload produces a warning
`route_execution.idempotent_replay` and does not append a duplicate step. Reusing
the same key with a different boundary, stage, status, metadata, or artifact
checksum blocks the ledger with `route_execution.idempotency_conflict`.

If `--idempotency-key` is omitted, the service derives one from route, dataset,
run id, stage, boundaries, and artifact hashes.

## Lease TTL

`--lease-ttl-seconds` enables local commit fencing for the route and dataset. A
second runner with a different `--runner-id` is blocked until the lease expires.
The report includes owner, fencing token, and expiration time so operators can
decide whether a takeover is safe.

Treat the lease TTL as an operational safety window, not as proof that the
previous runner failed.

## Store backends

| Backend | Use case | Coordination behavior |
| --- | --- | --- |
| `local_json` | Default local evidence, single-runner CI, and review artifacts. | Best-effort compare-and-swap over local JSON files. |
| `sqlite` | Shared local runners, Docker-live certification, and release rehearsals that need one durable ledger file. | Atomic compare-and-swap appends and lease fencing with SQLite `BEGIN IMMEDIATE` transactions. |

The SQLite backend stores run steps and leases in
`route_execution_ledger.sqlite3` when `--store-uri` is omitted. Pass
`--store-uri` to place the database in a shared workspace path. The generated
`route_execution_ledger.json` and `route_commit_protocol.md` files are still
written to `--output-dir` for CI and release review.

The atomic compare-and-swap check prevents a runner from appending a step based
on a stale step count. When another runner wins the write first, the command
returns `route_execution.concurrent_write_conflict`; re-read the shared ledger
and retry with the latest state.

## Artifacts

The command accepts repeatable artifact references:

```bash
--artifact name=/path/to/artifact.json
```

Each artifact is checksumed and written under the step's `artifact_hashes`
section. Missing artifact paths are represented with the zero SHA-256 value and
can be handled by downstream readiness policy.

## Report contract

The command writes:

| Artifact | Purpose |
| --- | --- |
| `route_execution_ledger.json` | Latest public report for CI, route readiness, release gates, and incident review. |
| `route_commit_protocol.md` | Human-readable commit protocol summary and operator runbook. |
| `runs/*__route_execution_ledger.json` | Append-only run-specific step ledger. |
| `route_execution_index.json` | Local index of route, dataset, run id, and ledger path. |
| `route_execution_leases.json` | Local lease state when `--lease-ttl-seconds` is used. |
| `route_execution_ledger.sqlite3` | SQLite-backed shared ledger when `--store-backend sqlite` is used without `--store-uri`. |

The JSON schema version is `dpone.route_execution_ledger.v1`.

## Operator runbook

| Symptom | Action |
| --- | --- |
| `route_execution.lease_held_by_another_runner` | Wait for the active lease to expire, rerun with the same runner id, or perform an approved takeover after checking the owner. |
| `route_execution.idempotency_conflict` | Use a new idempotency key for a new side effect or replay the exact same step payload. |
| `route_execution.stage_regression` | Resume from the latest recorded stage or start a new run id for an approved replay. |
| `route_execution.state_commit_before_sink_success` | Record a successful `loaded_to_staging`, `finalized`, or `quality_checked` step before committing state. |
| `route_execution.concurrent_write_conflict` | Re-read the shared SQLite ledger and retry from the latest step version. |
| `route_execution.step_failed` | Fix the failed stage and record a later step only after the failure is resolved. |

## Route readiness

`route_execution_ledger` is a generic required evidence domain for every route
profile. Add it to `dpone ops route-readiness` or let
`dpone ops route-certification-pack` normalize it from the provided artifact.
