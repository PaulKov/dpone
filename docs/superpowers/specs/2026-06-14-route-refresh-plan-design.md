# Route Refresh Plan Design

## Context

Route readiness, route run supervision, schema evolution, reconciliation repair,
and route data quality now provide strong route release evidence. The next
industrial gap is an operator-safe way to plan historical backfills, refreshes,
retention-gap resyncs, and DQ-driven replays for any `source -> sink -> strategy`
route.

This design adds a generic route refresh planning capability. It is a
control-plane abstraction only. It produces a route-aware plan, chunk list,
state rewind guidance, approval decision, idempotency keys, evidence checksums,
and runbook. It does not execute queries, mutate sinks, replay CDC, repair rows,
or promote source state.

## Goals

- Provide one reusable refresh/backfill/resync planning taxonomy for any route.
- Support common reasons: initial backfill, manual resync, DQ repair, schema
  backfill, retention gap, and range replay.
- Generate deterministic chunks with route/dataset-aware idempotency keys.
- Make state rewind and destructive refresh behavior explicit and approval
  gated.
- Normalize optional upstream evidence such as route DQ, route run supervisor,
  CDC retention, schema evolution, and reconciliation repair.
- Produce stable JSON/Markdown artifacts for CI, release gates, and operators.

## Non-Goals

- No live source or sink connections.
- No execution command in this PR.
- No target DDL, CDC replay, row repair, or state mutation.
- No route-specific behavior for `postgres -> mssql` or `mssql -> clickhouse`.

## Public Contract

The command writes:

- `route_refresh_plan.json`
- `route_refresh_plan.md`

The JSON schema version is `dpone.route_refresh_plan.v1`.

Top-level fields include:

- `route`
- `profile`
- `dataset`
- `reason`
- `status`
- `passed`
- `window`
- `chunks`
- `state_rewind`
- `approval`
- `blockers`
- `warnings`
- `next_actions`
- `evidence`
- `evidence_index`
- `json_path`
- `markdown_path`

## Status Taxonomy

- `ready`: the route exists, the window is valid, evidence is usable, no
  destructive/state rewind approval is required, and chunking fits policy.
- `approval_required`: the plan is well-formed but must be approved before any
  execution because it is destructive, state-rewinding, or policy-required.
- `blocked`: route is unsupported, window is invalid, required evidence is
  missing/failed/malformed, route mismatches, or chunk policy is exceeded.

## Module Boundaries

- `dpone.ops.routes.refresh_plan_models`
  - immutable public contracts for window, chunks, approval, state rewind,
    evidence, decision, and report.
- `dpone.ops.routes.refresh_plan_policy`
  - pure blocker/status/next-action policy.
- `dpone.ops.route_refresh_plan`
  - catalog lookup, generic evidence reading, chunk planning, policy evaluation,
    and report writing.
- CLI parser/handler
  - argument parsing and service invocation only.

## CLI Shape

```bash
uv run dpone ops route-refresh-plan \
  --source mssql \
  --sink clickhouse \
  --strategy incremental_merge \
  --dataset analytics.orders \
  --reason dq_repair \
  --window-kind integer \
  --start 1 \
  --end 1000 \
  --chunk-size 250 \
  --current-state lsn:0x200 \
  --target-state lsn:0x100 \
  --artifact route_data_quality=test_artifacts/route_quality/orders/route_data_quality.json \
  --require route_data_quality \
  --require-approval \
  --output-dir test_artifacts/route_refresh/mssql_to_clickhouse/orders \
  --format json
```

## Data Flow

1. CLI receives route identity, dataset, refresh reason, window, chunk policy,
   optional state rewind, approval flags, and evidence artifacts.
2. `RouteRefreshPlanService` resolves `RouteKey` and `RouteProfile`.
3. The service normalizes optional evidence using the existing route artifact
   pass/fail and route identity semantics.
4. The service builds deterministic chunks from the requested window.
5. `RouteRefreshPlanPolicy` decides whether the plan is ready, approval-gated,
   or blocked.
6. `RouteRefreshPlanReport` writes JSON and Markdown artifacts.

## Testing Strategy

- Service tests for ready chunk planning, approval-required state rewind,
  destructive refresh approval, invalid window, chunk cap, missing required
  evidence, malformed evidence, route mismatch, and first supported routes.
- CLI tests for JSON output and blocked exit code.
- Release gate regression proving `route_refresh_plan` works as generic
  required evidence.
- Docs contract tests requiring user docs, developer docs, ops CLI, CI/CD,
  control-plane, release-gate, and MkDocs navigation.

## Documentation Strategy

All OSS docs are English:

- `docs/route-refresh-plan.md`
- `docs/developer-route-refresh-plan.md`
- Updates to ops CLI, CI/CD, operational control plane, route release gate, and
  MkDocs navigation.
- Generated CLI reference and quality metrics refreshed after staging new
  Python source files.
