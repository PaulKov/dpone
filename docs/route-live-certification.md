# Route live certification

Route live certification is the route-scoped Docker-live and vendor-live
evidence bundle for one `source -> sink -> strategy` pair. It sits between
specialized route checks and [Route release gate](route-release-gate.md).

`dpone ops route-live-certification` does not run databases by itself. It
renders the opt-in harness commands, validates already-produced live evidence,
checks that every artifact belongs to the same route, and writes
`route_live_certification.json` plus `route_live_certification.md`. Feed the
JSON file to `route-release-gate` as `route_live_evidence_bundle`.

## When to use it

Use this command for release candidates that changed critical route behavior:

| Route | Required live signal |
| --- | --- |
| MSSQL -> ClickHouse | CDC apply certification, snapshot handoff, recovery, schema evolution, promotion, benchmark/SLO, type fidelity, and ClickHouse live apply evidence. |
| Postgres -> MSSQL | Native transfer evidence, lossless transport, resume checkpoint, type matrix, benchmark/SLO, and state promotion evidence. |

The command supports Docker-live profiles such as `real_local` and
`native_transfer`, and credentialed `vendor_live` profiles. Vendor-live remains
manual and opt-in; never store credentials inside artifacts.

## CLI

```bash
uv run dpone ops route-live-certification \
  --release vX.Y.Z-rc1 \
  --source mssql \
  --sink clickhouse \
  --strategy incremental_merge \
  --profile vendor_live \
  --row-count 25000 \
  --output-dir test_artifacts/route_live/mssql_to_clickhouse \
  --artifact service_markers=test_artifacts/live_certification/service_markers.json \
  --artifact route_readiness=test_artifacts/route_certification/mssql_to_clickhouse/readiness/route_readiness.json \
  --artifact route_certification_pack=test_artifacts/route_certification/mssql_to_clickhouse/route_certification_pack.json \
  --artifact route_execution_ledger=test_artifacts/route_execution/mssql_to_clickhouse/orders/route_execution_ledger.json \
  --artifact state_promotion=test_artifacts/route_state/mssql_to_clickhouse/orders/state_promotion.json \
  --artifact benchmark_slo=test_artifacts/live_certification/benchmark-slo/benchmark_slo_gate.json \
  --artifact performance_certification=test_artifacts/live_certification/performance-certification/performance_certification.json \
  --artifact live_state_reconciliation=test_artifacts/live_certification/live-state-reconciliation/live_state_reconciliation.json \
  --artifact evidence_chain=test_artifacts/live_certification/evidence-chain/evidence_chain_index.json \
  --artifact cdc_handoff=test_artifacts/cdc_handoff/mssql_to_clickhouse/orders/cdc_handoff.json \
  --artifact cdc_apply_certification=test_artifacts/cdc_apply/mssql_to_clickhouse/orders/cdc_apply_certification.json \
  --artifact cdc_observability_evidence=test_artifacts/cdc_observability/mssql_to_clickhouse/orders/cdc_observability.json \
  --artifact cdc_recovery_evidence=test_artifacts/cdc_recovery/mssql_to_clickhouse/orders/cdc_recovery_evidence.json \
  --artifact cdc_schema_evolution_evidence=test_artifacts/cdc_schema/mssql_to_clickhouse/orders/cdc_schema_evolution_evidence.json \
  --artifact cdc_promotion_gate=test_artifacts/cdc_promotion/mssql_to_clickhouse/orders/cdc_promotion_gate.json \
  --format json
```

For Postgres -> MSSQL native transfer certification, use the same command with
`--source postgres --sink mssql` and attach `native_transfer_evidence`,
`lossless_transport_contract`, `resume_checkpoint`, and `type_matrix` artifacts.
When `--profile native_transfer` is used, also attach
`native_transfer_transport_certification` and
`native_transfer_route_certification`. Produce connector transport evidence
with:

```bash
uv run dpone connectors certify \
  --profile static \
  --capability native_transfer.stream \
  --artifact-dir test_artifacts/connectors/postgres \
  --format json
```

Use the generated
`test_artifacts/connectors/postgres/connector-capability-certification.json`
as the `native_transfer_transport_certification` artifact. Produce route
decision evidence with:

```bash
uv run dpone ops route-transport-certification \
  --manifest manifests/postgres_to_mssql.yaml \
  --profile static \
  --artifact-dir test_artifacts/native_transfer/postgres_to_mssql/route \
  --format json
```

Use the generated
`test_artifacts/native_transfer/postgres_to_mssql/route/native_transfer_route_certification.json`
as the `native_transfer_route_certification` artifact.

## Outputs

| File | Purpose |
| --- | --- |
| `route_live_certification.json` | Stable schema `dpone.route_live_certification.v1` with route identity, profile, harness steps, required evidence, blockers, and checksumed evidence index. |
| `route_live_certification.md` | Human-readable route-live review and Operator runbook. |

The JSON contains an `evidence_bundle` object named
`route_live_evidence_bundle`. That name is the handoff contract for the final
route release gate:

```bash
uv run dpone ops route-release-gate \
  --release vX.Y.Z-rc1 \
  --source mssql \
  --sink clickhouse \
  --strategy incremental_merge \
  --artifact route_live_evidence_bundle=test_artifacts/route_live/mssql_to_clickhouse/route_live_certification.json \
  --require route_live_evidence_bundle \
  --output-dir test_artifacts/route_release/mssql_to_clickhouse \
  --format json
```

## Operator runbook

Failure: `*.missing`.

1. Run the upstream Docker-live or vendor-live check that produces the missing
   artifact.
2. Attach the generated artifact with `--artifact name=path`.
3. Re-run `dpone ops route-live-certification`.

For `native_transfer_transport_certification.missing`, run connector capability
certification for the source and sink transport roles, then attach the generated
capability evidence. For `native_transfer_route_certification.missing`, run
`dpone ops route-transport-certification` for the exact manifest and route.
Do not mark a native transfer route release-ready without both connector
transport capability evidence and route decision evidence.

Failure: `*.not_passed`.

1. Open the failed artifact and follow that artifact's runbook.
2. Re-run the live test instead of editing generated JSON by hand.
3. Rebuild the route live bundle after the upstream artifact is green.

Failure: `*.route_mismatch`.

1. Confirm `source`, `sink`, and `strategy` in the artifact.
2. Use evidence generated for the same route as the release candidate.
3. Rebuild `route-release-gate` only after the mismatch is gone.

Failure: vendor-live credentials.

1. Verify the workflow or local shell explicitly set the required secrets.
2. Keep `vendor_live` manual and opt-in.
3. Rotate credentials if they were printed to logs.

## Related docs

- [Live certification](live-certification.md)
- [Route release gate](route-release-gate.md)
- [CDC apply certification](cdc-apply-certification.md)
- [Source -> sink matrix](source-sink-matrix.md)
