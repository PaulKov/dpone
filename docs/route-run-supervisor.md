# Route run supervisor

`dpone ops route-run-supervisor` builds one route-level run receipt for a
`source -> sink -> strategy` execution. Use it after a manifest run, dry-run,
CDC apply, repair, or release-candidate execution when operators need one
artifact that explains whether the route run is ready, blocked, retryable,
unsafe to retry, or waiting for manual approval.

The command does not connect to databases, run manifests, apply DDL, promote
state, replay CDC, or mutate sinks. It reads existing evidence artifacts and
writes a stable JSON/Markdown receipt. In `--run-mode route_refresh`, the
receipt also includes an `execution_contract` with the expected production
stages, required artifacts, and copy/paste command hints for the next missing
or blocked stage.

## CLI example

```bash
uv run dpone ops route-run-supervisor \
  --source mssql \
  --sink clickhouse \
  --strategy incremental_merge \
  --run-id orders-2026-06-14T10-00Z \
  --dataset analytics.orders \
  --manifest manifests/orders.yml \
  --run-mode route_refresh \
  --artifact route_readiness=test_artifacts/route_readiness/mssql_to_clickhouse/route_readiness.json \
  --artifact route_refresh_execution=test_artifacts/route_refresh_execution/mssql_to_clickhouse/orders/route_refresh_execution.json \
  --artifact route_refresh_snapshot_capture=test_artifacts/route_refresh_execution/mssql_to_clickhouse/orders/route_refresh_snapshot_capture.json \
  --artifact route_refresh_verification=test_artifacts/route_refresh_execution/mssql_to_clickhouse/orders/route_refresh_verification.json \
  --artifact route_execution_ledger=test_artifacts/route_execution/mssql_to_clickhouse/orders/route_execution_ledger.json \
  --artifact state_promotion=test_artifacts/route_state/mssql_to_clickhouse/orders/state_promotion.json \
  --artifact cdc_observability=test_artifacts/cdc_observability/mssql_to_clickhouse/orders/cdc_observability.json \
  --output-dir test_artifacts/route_runs/mssql_to_clickhouse/orders \
  --format json
```

## Outputs

| File | Description |
| --- | --- |
| `route_run_receipt.json` | Stable schema `dpone.route_run_supervisor.v1` with route identity, run identity, decision, `execution_contract`, phases, evidence checksums, blockers, warnings, and next actions. |
| `route_run_receipt.md` | Human-readable route run receipt and Runbook. |

## Run modes

| Mode | Required evidence | Use when |
| --- | --- | --- |
| `evidence_only` | `route_readiness`, `route_execution_ledger`, `state_promotion` | You already have route lifecycle evidence and need a compact go/no-go receipt. |
| `route_refresh` | `route_readiness`, `route_refresh_execution`, `route_refresh_snapshot_capture`, `route_refresh_verification`, `route_execution_ledger`, `state_promotion` | You are supervising a replication-grade refresh or replay path and want the receipt to enforce execute -> capture -> verify -> ledger -> promote evidence. |

`route_refresh` is the production execution-plane contract for current
`postgres -> mssql` and `mssql -> clickhouse` routes. It stays source/sink
agnostic: future routes only need matrix/profile metadata, executor
configuration, and evidence artifacts, not new supervisor command logic.
The CLI also accepts hyphenated aliases such as `route-refresh`, but receipts
always write the canonical mode id.

## Required evidence

The default required evidence domains are:

| Evidence | Phase | Purpose |
| --- | --- | --- |
| `route_readiness` | `preflight` | Matrix-driven route readiness and required route evidence status. |
| `route_execution_ledger` | `execution` | Idempotent route execution, lease fencing, source/sink boundaries, and commit protocol evidence. |
| `state_promotion` | `state` | Proof that source state is advanced only after durable sink success. |

Use `--require <name>` to make additional domains mandatory. Common optional
domains are `route_schema_evolution`, `cdc_handoff`, `cdc_apply`,
`cdc_observability`, `route_reconciliation_repair`, and `route_release_gate`.

## Execution contract

Every receipt includes `execution_contract`:

- `mode`: the run mode used by the supervisor.
- `ready`: true only when every required contract stage is complete.
- `stages`: ordered stage records with phase, evidence domain, required flag,
  status, artifact path, blockers, and a command hint.
- `next_commands`: command hints for required stages that are missing, failed,
  retryable, unsafe, or waiting for manual approval.

For `--run-mode route_refresh`, the contract points operators through:

1. `dpone ops route-refresh-execute --execute`
2. `dpone ops route-refresh-capture-snapshots`
3. `dpone ops route-refresh-verify`
4. `dpone ops route-execution-ledger`
5. `dpone ops route-state-promote`
6. `dpone ops route-run-supervisor`

Command hints include placeholders such as `<executor_backend>`,
`<executor-config.json>`, `<key_column>`, and `<source_boundary>`. Fill those
from the manifest/run context and keep the emitted artifacts immutable.

## Decision statuses

| Status | Meaning |
| --- | --- |
| `ready` | All required evidence exists, belongs to the route, and passed. |
| `blocked` | Required evidence is missing, malformed, failed hard, unsupported, or route-mismatched. |
| `retryable` | A runtime-like failure is present and evidence does not mark retry as unsafe. |
| `unsafe_to_retry` | State, schema, repair, release, duplicate, commit, or fencing evidence says automatic retry could be unsafe. |
| `manual_approval_required` | Evidence is otherwise usable but requires approval before continuation. |

The policy is conservative. Missing evidence and route mismatches are always
blockers. Schema/backfill approval signals block the run until governance
evidence is attached.

## Python API

```python
from dpone.ops.route_run_supervisor import RouteRunSupervisorService

report = RouteRunSupervisorService().evaluate(
    output_dir="test_artifacts/route_runs/mssql_to_clickhouse/orders",
    source="mssql",
    sink="clickhouse",
    strategy="incremental_merge",
    run_id="orders-2026-06-14T10-00Z",
    dataset="analytics.orders",
    manifest="manifests/orders.yml",
    run_mode="route_refresh",
    artifacts={
        "route_readiness": "test_artifacts/route_readiness/route_readiness.json",
        "route_refresh_execution": "test_artifacts/route_refresh_execution/route_refresh_execution.json",
        "route_refresh_snapshot_capture": "test_artifacts/route_refresh_execution/route_refresh_snapshot_capture.json",
        "route_refresh_verification": "test_artifacts/route_refresh_execution/route_refresh_verification.json",
        "route_execution_ledger": "test_artifacts/route_execution/route_execution_ledger.json",
        "state_promotion": "test_artifacts/route_state/state_promotion.json",
    },
)
```

## Runbook

1. Run the manifest, route RC executor, or CDC/repair command that produced the
   upstream artifacts.
2. Attach evidence with `--artifact name=/path/to/artifact.json`.
3. If the receipt is `ready`, attach `route_run_receipt.json` to route release
   evidence.
4. If the receipt is `retryable`, fix the runtime blocker and rerun the manifest
   or replay command.
5. If the receipt is `unsafe_to_retry`, stop automatic retry and inspect state,
   schema, repair, and release artifacts before continuing.
6. If the receipt is `manual_approval_required`, capture approval evidence and
   rerun `dpone ops route-run-supervisor`.
7. For release candidates, pass the receipt to `dpone ops route-release-gate`
   as `--artifact route_run_supervisor=<path>` and require it with
   `--require route_run_supervisor`.

## Related docs

- [Route readiness](route-readiness.md)
- [Route execution ledger](route-execution-ledger.md)
- [Route state promotion](route-state-promotion.md)
- [Route release gate](route-release-gate.md)
- [Developer route run supervisor](developer-route-run-supervisor.md)
