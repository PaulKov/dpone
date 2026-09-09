# Route release gate

Route release gate is the final route-scoped go/no-go receipt for one
`source -> sink -> strategy` release candidate. It aggregates route readiness,
route certification pack, execution ledger, route refresh verification, state
promotion, benchmark, schema, docs, and optional CDC evidence into one stable
JSON/Markdown report.

The command does not run live databases, benchmarks, or CDC loops. It only reads
artifacts that already exist, verifies their pass/fail status and route
identity, records checksums, and writes a release review receipt.

## User workflow

1. Generate route evidence with the specialized commands for the route.
2. Run `dpone ops route-certification-pack` to normalize route evidence and
   produce embedded `route_readiness.json`.
3. Run `dpone ops route-execution-ledger` around durable sink work.
4. Run `dpone ops route-refresh-verify` after any executed refresh, backfill,
   replay, repair, or retention-gap recovery.
5. Run `dpone ops route-state-promote` only after the sink commit receipt is
   safe.
6. Add CDC evidence such as `cdc_apply_certification`,
   `cdc_recovery_evidence`, `cdc_schema_evolution_evidence`, or
   `cdc_promotion_gate` when the release changes replication behavior.
7. Run `dpone ops route-release-gate`.
8. For minor/major releases, run `dpone ops route-certify` to wrap this gate in
   `route_certification_bundle.json`.
9. Attach `route_release_gate.json` and `route_release_gate.md` to release
   review.

## CLI example

```bash
uv run dpone ops route-release-gate \
  --release vX.Y.Z-rc1 \
  --source mssql \
  --sink clickhouse \
  --strategy incremental_merge \
  --output-dir test_artifacts/route_release/mssql_to_clickhouse \
  --artifact route_readiness=test_artifacts/route_certification/mssql_to_clickhouse/readiness/route_readiness.json \
  --artifact route_certification_pack=test_artifacts/route_certification/mssql_to_clickhouse/route_certification_pack.json \
  --artifact route_execution_ledger=test_artifacts/route_execution/mssql_to_clickhouse/orders/route_execution_ledger.json \
  --artifact route_refresh_verification=test_artifacts/route_refresh_execution/mssql_to_clickhouse/orders/verify/route_refresh_verification.json \
  --artifact state_promotion=test_artifacts/route_state/mssql_to_clickhouse/orders/state_promotion.json \
  --artifact route_schema_evolution=test_artifacts/route_schema/mssql_to_clickhouse/orders/route_schema_evolution.json \
  --artifact route_reconciliation_repair=test_artifacts/route_repair/mssql_to_clickhouse/orders/route_reconciliation_repair.json \
  --artifact type_fidelity=test_artifacts/type_fidelity/latest/mssql_to_clickhouse.json \
  --artifact typed_hash=test_artifacts/typed_hash/latest/mssql_to_clickhouse.json \
  --artifact wide_type_certification=test_artifacts/type_matrix/latest/mssql_to_clickhouse_wide.json \
  --artifact benchmark_slo=test_artifacts/benchmarks/latest/mssql_to_clickhouse_slo.json \
  --artifact schema_evolution=test_artifacts/schema_evolution/latest/mssql_to_clickhouse.json \
  --require cdc_apply_certification \
  --require cdc_recovery_evidence \
  --require cdc_schema_evolution_evidence \
  --artifact cdc_apply_certification=test_artifacts/cdc_apply/mssql_to_clickhouse/orders/cdc_apply_certification.json \
  --artifact cdc_recovery_evidence=test_artifacts/cdc_recovery/mssql_to_clickhouse/orders/cdc_recovery_evidence.json \
  --artifact cdc_schema_evolution_evidence=test_artifacts/cdc_schema/mssql_to_clickhouse/orders/cdc_schema_evolution_evidence.json \
  --format json
```

Exit code `0` means the route release gate passed. Exit code `1` means the
release candidate is blocked or only a release candidate with unresolved
blockers.

## Required evidence

Every route release gate requires:

| Evidence | Purpose |
| --- | --- |
| `route_readiness` | Final matrix-driven route evidence decision. |
| `route_certification_pack` | Normalized evidence bundle and embedded readiness output. |
| `route_execution_ledger` | Idempotency, lease fencing, durable sink stage, and commit ordering evidence. |
| `route_refresh_verification` | Post-load source/sink row-count, boundary, duplicate/null key, and typed hash verification evidence. |
| `state_promotion` | Commit receipt and safe source-state advancement evidence. |

For release candidates that must prove end-to-end manifest/run lifecycle
safety, also attach `route_run_supervisor` and require it explicitly:

```bash
uv run dpone ops route-release-gate \
  --release v0.10.0-rc1 \
  --source mssql \
  --sink clickhouse \
  --strategy incremental_merge \
  --artifact route_run_supervisor=test_artifacts/route_runs/mssql_to_clickhouse/orders/route_run_receipt.json \
  --require route_run_supervisor
```

For release candidates that must prove data quality and exception management,
also attach `route_data_quality` and require it explicitly:

```bash
uv run dpone ops route-release-gate \
  --release v0.10.0-rc1 \
  --source mssql \
  --sink clickhouse \
  --strategy incremental_merge \
  --artifact route_data_quality=test_artifacts/route_quality/mssql_to_clickhouse/orders/route_data_quality.json \
  --require route_data_quality
```

For release candidates that include bounded replay, backfill, repair,
schema backfill, retention-gap recovery, or source-state rewind, also attach
`route_refresh_plan` and require it explicitly:

```bash
uv run dpone ops route-release-gate \
  --release v0.10.0-rc1 \
  --source mssql \
  --sink clickhouse \
  --strategy incremental_merge \
  --artifact route_refresh_plan=test_artifacts/route_refresh/mssql_to_clickhouse/orders/route_refresh_plan.json \
  --require route_refresh_plan
```

For release candidates that actually execute refresh chunks, also attach
`route_refresh_execution` and require it explicitly. The default gate already
requires `route_refresh_verification`, so execution evidence without
verification remains insufficient for release promotion:

```bash
uv run dpone ops route-release-gate \
  --release v0.10.0-rc2 \
  --source mssql \
  --sink clickhouse \
  --strategy incremental_merge \
  --artifact route_refresh_execution=test_artifacts/route_refresh_execution/mssql_to_clickhouse/orders/route_refresh_execution.json \
  --artifact route_refresh_verification=test_artifacts/route_refresh_execution/mssql_to_clickhouse/orders/verify/route_refresh_verification.json \
  --require route_refresh_execution
```

The service also requires every evidence domain listed by the active
`RouteProfile`. For `mssql -> clickhouse`, that includes type fidelity, typed
hash, wide type certification, benchmark/SLO, schema evolution, route schema
evolution, route reconciliation repair, docs, run artifact, and matrix
evidence. For `postgres -> mssql`, that includes lossless transport,
benchmark/SLO, resume checkpoint, type matrix, route schema evolution, and
route reconciliation repair evidence.

Use repeated `--require` flags for release-specific domains that are not part
of the static route profile. CDC release candidates commonly add:

| Extra evidence | When to require |
| --- | --- |
| `cdc_apply_certification` | CDC apply correctness, delete semantics, typed hash, or replay behavior changed. |
| `cdc_recovery_evidence` | Restart, partial commit, offset ordering, poison event, or retention behavior changed. |
| `cdc_schema_evolution_evidence` | CDC source schema change or target DDL governance changed. |
| `cdc_promotion_gate` | Final replication readiness and offset promotion review is required. |

## Report contract

`route_release_gate.json` uses schema version
`dpone.route_release_gate.v1` and contains:

- `release`: release or release-candidate id.
- `route`: canonical `source`, `sink`, `strategy`, `pair_id`, `case_id`, and
  `colon_id`.
- `profile`: matrix-driven route metadata.
- `passed`, `level`, `score`: go/no-go decision for CI and release review.
- `required_evidence`: all required domains after profile and `--require`
  expansion.
- `evidence` and `evidence_index`: normalized artifacts with status, route
  match, checksum, blockers, summary, and path.
- `blockers`, `warnings`, `next_actions`: operator diagnostics.
- `json_path`, `markdown_path`: generated receipt locations.

`route_release_gate.md` is the human-readable release review attachment.

## Operator runbook

When the route release gate is red:

1. Open `route_release_gate.json` and copy the first blocker.
2. If the blocker ends with `.missing`, attach or regenerate that artifact.
3. If the blocker ends with `.not_passed`, open the upstream artifact and use
   its runbook first. Do not edit aggregate gate JSON by hand.
4. If the blocker ends with `.route_mismatch`, rerun the upstream command for
   the same `source`, `sink`, and `strategy`.
5. Re-run `dpone ops route-release-gate` after the upstream evidence changes.
6. Keep the evidence directory immutable for the release candidate.

## Related docs

- [Route readiness](route-readiness.md)
- [Route certification pack](route-certification-pack.md)
- [Route schema evolution](route-schema-evolution.md)
- [Route reconciliation repair](route-reconciliation-repair.md)
- [Route data quality](route-data-quality.md)
- [Route refresh plan](route-refresh-plan.md)
- [Route refresh execute](route-refresh-execute.md)
- [Route certify](route-certify.md)
- [Route execution ledger](route-execution-ledger.md)
- [Route state promotion](route-state-promotion.md)
- [CDC promotion gate](cdc-promotion-gate.md)
- [Source -> sink matrix](source-sink-matrix.md)
- [`dpone ops` operational controls](ops-cli.md)
