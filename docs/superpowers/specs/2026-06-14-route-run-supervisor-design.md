# Route run supervisor design

## Summary

`RouteRunSupervisorService` is a route-level control-plane service that turns
one planned or completed `source -> sink -> strategy` run into a stable
`RouteRunEvidenceBundle`. It is the operator-facing lifecycle receipt between a
manifest run and route release evidence.

The first target routes are `postgres -> mssql` and `mssql -> clickhouse`, but
the service must be generic. Route support comes from `RouteProfileCatalog` and
the integration matrix. Route-specific behavior belongs in route profiles,
upstream evidence, or injected policy collaborators, not in CLI handlers or the
supervisor service.

## Goals

- Provide one self-service receipt for route run lifecycle status.
- Normalize existing readiness, schema, state, execution, CDC, reconciliation,
  repair, observability, and release-gate artifacts into one bundle.
- Classify the run as `ready`, `blocked`, `retryable`, `unsafe_to_retry`, or
  `manual_approval_required`.
- Preserve CLI and Python API parity.
- Avoid live database execution and sink mutation inside the supervisor.
- Keep interfaces thin, testable, and reusable for future routes.

## Non-goals

- Do not replace runtime connectors, `AbstractSource`, `AbstractSink`, CDC
  readers, or manifest execution.
- Do not execute Docker-live or vendor-live tests.
- Do not apply DDL, promote state, replay CDC, or repair data directly.
- Do not encode route-specific branches for the first two routes.

## Public command

```bash
uv run dpone ops route-run-supervisor \
  --source mssql \
  --sink clickhouse \
  --strategy incremental_merge \
  --run-id orders-2026-06-14T10-00Z \
  --dataset analytics.orders \
  --manifest manifests/orders.yml \
  --artifact route_readiness=test_artifacts/route_readiness/route_readiness.json \
  --artifact route_execution_ledger=test_artifacts/route_execution/route_execution_ledger.json \
  --artifact state_promotion=test_artifacts/route_state/state_promotion.json \
  --output-dir test_artifacts/route_runs/orders \
  --format json
```

## Public Python API

```python
from dpone.ops.route_run_supervisor import RouteRunSupervisorService

report = RouteRunSupervisorService().evaluate(
    output_dir="test_artifacts/route_runs/orders",
    source="mssql",
    sink="clickhouse",
    strategy="incremental_merge",
    run_id="orders-2026-06-14T10-00Z",
    dataset="analytics.orders",
    manifest="manifests/orders.yml",
    artifacts={
        "route_readiness": "test_artifacts/route_readiness/route_readiness.json",
        "route_execution_ledger": "test_artifacts/route_execution/route_execution_ledger.json",
        "state_promotion": "test_artifacts/route_state/state_promotion.json",
    },
)
```

## Module boundaries

| Module | Responsibility |
| --- | --- |
| `dpone.ops.routes.run_supervisor_models` | Stable dataclasses for run phase, evidence, decision, and report contracts. |
| `dpone.ops.routes.run_supervisor_policy` | Pure status, blocker, warning, and next-action decisions from profile plus normalized evidence. |
| `dpone.ops.routes.run_supervisor` | Orchestrates catalog lookup, evidence normalization, policy evaluation, and report writing. |
| `dpone.ops.route_run_supervisor` | Small public facade for imports and service catalog access. |
| `dpone.services.ops.command_handlers_routes` | Thin CLI handler only. |
| `dpone.commands.ops_parsers_routes` | Argument parsing only. |

## Evidence domains

The default bundle understands these domains:

| Domain | Phase | Required by default | Purpose |
| --- | --- | --- | --- |
| `route_readiness` | `preflight` | yes | Route profile, matrix, docs, and required evidence readiness. |
| `route_schema_evolution` | `schema` | no | Route-level DDL/apply decision. |
| `route_execution_ledger` | `execution` | yes | Idempotent execution step and commit-protocol evidence. |
| `state_promotion` | `state` | yes | State commit-after-load receipt. |
| `cdc_handoff` | `cdc` | no | Snapshot-to-CDC handoff evidence. |
| `cdc_apply` | `cdc` | no | CDC apply correctness evidence. |
| `cdc_observability` | `observability` | no | CDC lag, freshness, retention, and throughput SLO evidence. |
| `route_reconciliation_repair` | `repair` | no | Source-target repair plan evidence. |
| `route_release_gate` | `release` | no | Route release gate receipt. |

Additional artifact names are allowed and become optional evidence unless passed
through `--require`.

## Decision model

The report has a `decision.status`:

| Status | Meaning |
| --- | --- |
| `ready` | Route profile exists and all required evidence is present, route-matched, and passed. |
| `blocked` | Required evidence is missing, failed, malformed, or belongs to a different route. |
| `retryable` | Failure is runtime-like and policy says replay/resume is safe after the blocker is resolved. |
| `unsafe_to_retry` | State, schema, repair, or release evidence says retry could duplicate, corrupt, or advance wrong state. |
| `manual_approval_required` | Evidence is not failed, but a schema/backfill/approval signal requires governance before continuation. |

The service should prefer conservative decisions. If a required artifact cannot
be parsed, it is blocked. If an artifact has `safe_to_retry=false` or has
blockers that mention state promotion, route mismatch, schema approval, repair
actions, or release failure, it must not be classified as retryable.

## Output files

| File | Description |
| --- | --- |
| `route_run_receipt.json` | Stable schema `dpone.route_run_supervisor.v1` with route, run identity, manifest path, decision, phases, evidence, blockers, warnings, and next actions. |
| `route_run_receipt.md` | Operator-readable run receipt and Runbook. |

## Testing

- Service tests cover ready, missing required evidence, route mismatch,
  manual-approval, retryable, and unsafe-to-retry decisions.
- CLI tests cover JSON output, non-zero exits, output files, and parser help.
- Docs contract tests require user docs, developer docs, MkDocs nav, ops CLI
  coverage, and architecture references.
- Existing route readiness/release-gate tests must continue to pass.
- Generated CLI reference and quality metrics must be updated when parsers or
  Python source files change.

## Runbook

1. Generate upstream route evidence with existing route/CDC commands.
2. Run `dpone ops route-run-supervisor` with route identity, run identity,
   dataset, manifest, and artifact references.
3. If status is `ready`, attach `route_run_receipt.json` to route release
   evidence.
4. If status is `retryable`, resolve the listed runtime blocker and rerun the
   manifest or replay command indicated by `next_actions`.
5. If status is `unsafe_to_retry`, stop automatic retry and inspect state,
   schema, repair, and release evidence before continuing.
6. If status is `manual_approval_required`, capture approval evidence and rerun
   the supervisor.
