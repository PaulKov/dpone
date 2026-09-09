# Route refresh plan

`dpone ops route-refresh-plan` creates a control-plane plan for a bounded
backfill, refresh, replay, or resync on one `source -> sink -> strategy`
route. It is the operator-facing bridge between route readiness evidence and
the actual chunked work that a runtime, CDC loop, or release-candidate executor
will perform later.

The command does not connect to databases, truncate tables, replay rows, mutate
sinks, or promote source state. It reads route evidence, checks that evidence
belongs to the same route, validates the requested window, computes
idempotent chunks, classifies approval needs, and writes
`route_refresh_plan.json` plus `route_refresh_plan.md`.

## When to use it

Use a route refresh plan when a release or incident workflow needs a reviewed
operator plan before moving data:

| Reason | Use case |
| --- | --- |
| `initial_backfill` | First route load before CDC or incremental mode starts. |
| `manual_resync` | Human-triggered replay after an incident or reconciliation gap. |
| `dq_repair` | Targeted replay after data quality, quarantine, or repair evidence. |
| `schema_backfill` | Backfill an additive column or derived typed representation. |
| `retention_gap` | Recover from source CDC retention or log-window loss. |
| `range_replay` | Replay a known bounded primary-key, timestamp, partition, or state range. |

## CLI example

```bash
uv run dpone ops route-refresh-plan \
  --source mssql \
  --sink clickhouse \
  --strategy incremental_merge \
  --dataset analytics.orders \
  --reason dq_repair \
  --window-kind integer \
  --start 1 \
  --end 250000 \
  --chunk-size 10000 \
  --artifact route_data_quality=test_artifacts/route_quality/mssql_to_clickhouse/orders/route_data_quality.json \
  --artifact route_run_supervisor=test_artifacts/route_runs/mssql_to_clickhouse/orders/route_run_receipt.json \
  --require route_data_quality \
  --output-dir test_artifacts/route_refresh/mssql_to_clickhouse/orders \
  --format json
```

Exit code `0` means the plan is `ready`. Exit code `1` means the plan is
`approval_required` or `blocked`.

## Outputs

| File | Description |
| --- | --- |
| `route_refresh_plan.json` | Stable schema `dpone.route_refresh_plan.v1` with route identity, profile metadata, dataset, reason, window, chunks, approval, state rewind assessment, evidence checksums, blockers, warnings, and next actions. |
| `route_refresh_plan.md` | Human-readable plan and Runbook for release review or incident handoff. |

## Window modes

| `--window-kind` | Behavior |
| --- | --- |
| `integer` | Splits inclusive numeric boundaries into chunks using `--chunk-size`. Example: `1..250` with chunk size `100` becomes `1..100`, `101..200`, `201..250`. |
| `timestamp` | Produces one bounded chunk using the supplied start and end boundaries. Use upstream orchestration to split timestamp windows when needed. |
| `partition` | Produces one partition-aware chunk. Set `--partition` to the source partition expression, for example `dt=2026-06-01`. |
| `state` | Produces one state replay chunk and records source/sink boundaries for downstream execution evidence. |

The default maximum number of integer chunks is `1000`. Raise
`--max-chunks` only when the downstream executor and observability budget can
handle the larger plan.

## Evidence

The service accepts any route-scoped evidence through repeated
`--artifact name=/path/to/file.json`. Use repeated `--require <name>` to make a
domain mandatory.

Common evidence domains:

| Evidence | Why it matters |
| --- | --- |
| `route_data_quality` | Proves the route has no unhandled quality, quarantine, or exception backlog before replay. |
| `route_run_supervisor` | Proves the prior route lifecycle was ready, retryable, unsafe, or blocked. |
| `route_execution_ledger` | Proves idempotency keys, lease fencing, and sink commit ordering. |
| `state_promotion` | Proves source state was advanced only after durable sink commit. |
| `cdc_retention_check` | Proves retention margin and identifies retention-gap recovery windows. |
| `route_schema_evolution` | Proves schema backfill or target DDL governance before replay. |

Evidence JSON should include either `route.case_id` or `route.source`,
`route.sink`, and `route.strategy`. Mismatched route evidence is blocked with
`<name>.route_mismatch`.

## Approval and state rewind

Approval is required when any of these are true:

| Trigger | How to express it |
| --- | --- |
| Destructive refresh | Pass `--destructive`. |
| Source state rewind | Pass both `--current-state` and `--target-state` and make them different. |
| Policy-required approval | Pass `--require-approval`. |

If approval has already been captured in the external release process, pass
`--approval-granted` and attach the approval artifact with `--artifact`.

## Route release gate usage

Attach refresh plans to route release gates when the release includes a
backfill, replay, repair, or retention-gap recovery:

```bash
uv run dpone ops route-release-gate \
  --release v0.10.0-rc1 \
  --source mssql \
  --sink clickhouse \
  --strategy incremental_merge \
  --artifact route_refresh_plan=test_artifacts/route_refresh/mssql_to_clickhouse/orders/route_refresh_plan.json \
  --require route_refresh_plan
```

## Python API

```python
from dpone.ops.route_refresh_plan import RouteRefreshPlanService

report = RouteRefreshPlanService().plan(
    output_dir="test_artifacts/route_refresh/mssql_to_clickhouse/orders",
    source="mssql",
    sink="clickhouse",
    strategy="incremental_merge",
    dataset="analytics.orders",
    reason="dq_repair",
    window_kind="integer",
    start="1",
    end="250000",
    chunk_size=10000,
    artifacts={
        "route_data_quality": (
            "test_artifacts/route_quality/mssql_to_clickhouse/orders/"
            "route_data_quality.json"
        ),
    },
    required_evidence=("route_data_quality",),
)
```

## Runbook

1. Generate the upstream route evidence first, such as route data quality,
   route run supervisor, execution ledger, state promotion, schema evolution,
   or CDC retention evidence.
2. Run `dpone ops route-refresh-plan` with a bounded window and explicit
   evidence attachments.
3. If status is `ready`, attach `route_refresh_plan.json` and
   `route_refresh_plan.md` to release evidence before executing the chunks.
4. If status is `approval_required`, capture the manual approval, attach the
   approval evidence, and rerun with `--approval-granted`.
5. If status is `blocked`, fix the first blocker in the upstream evidence or
   window parameters. Do not edit `route_refresh_plan.json` by hand.
6. Execute chunks idempotently with the runtime or route RC executor, then
   write route execution ledger and state promotion evidence.
7. Require `route_refresh_plan` in `dpone ops route-release-gate` for releases
   that include replay, backfill, resync, or repair.

Use [`dpone ops route-refresh-execute`](route-refresh-execute.md) to turn a
reviewed plan into `route_refresh_execution.json` evidence before recording
ledger and state-promotion receipts.

## Related docs

- [Route readiness](route-readiness.md)
- [Route data quality](route-data-quality.md)
- [Route run supervisor](route-run-supervisor.md)
- [Route refresh execute](route-refresh-execute.md)
- [Route execution ledger](route-execution-ledger.md)
- [Route state promotion](route-state-promotion.md)
- [Route release gate](route-release-gate.md)
- [Developer route refresh plan](developer-route-refresh-plan.md)
