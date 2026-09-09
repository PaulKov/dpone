# Route release candidate orchestrator

Route release candidate orchestrator is the self-service release train for one
critical `source -> sink -> strategy` route. It composes:

1. `route_certification_pack`
2. `route_live_evidence_bundle`
3. `route_release_gate`
4. `release_evidence_pack`

The command is fail-closed. Missing, red, or route-mismatched required evidence
keeps the release candidate blocked and writes machine-readable blockers.
When the upstream artifacts already exist and you only need the final release
promotion artifact, run [`dpone ops route-certify`](route-certify.md) instead.

`dpone ops route-rc-orchestrator` does not start Docker, run pytest, or open
database connections. It coordinates existing evidence services and emits the
commands an operator or CI job should run in the opt-in Docker-live or
vendor-live environment. Pass the generated `route_rc_orchestration.json` to
`dpone ops route-rc-execute` when the command train needs dry-run validation or
explicit execution.

## Routes

| Route | Release candidate focus |
| --- | --- |
| MSSQL -> ClickHouse | CDC apply, typed hash, recovery, schema evolution, promotion, ClickHouse apply evidence, and route release gate. |
| Postgres -> MSSQL | COPY/bcp native transfer, lossless transport, resume checkpoint, type matrix, and route release gate. |

## CLI

```bash
uv run dpone ops route-rc-orchestrator \
  --release vX.Y.Z-rc1 \
  --source mssql \
  --sink clickhouse \
  --strategy incremental_merge \
  --profile vendor_live \
  --row-count 25000 \
  --output-dir test_artifacts/route_rc/mssql_to_clickhouse \
  --artifact service_markers=test_artifacts/live_certification/service_markers.json \
  --artifact route_execution_ledger=test_artifacts/route_execution/mssql_to_clickhouse/orders/route_execution_ledger.json \
  --artifact state_promotion=test_artifacts/route_state/mssql_to_clickhouse/orders/state_promotion.json \
  --artifact benchmark_slo=test_artifacts/live_certification/benchmark-slo/benchmark_slo_gate.json \
  --artifact performance_certification=test_artifacts/live_certification/performance-certification/performance_certification.json \
  --artifact live_state_reconciliation=test_artifacts/live_certification/live-state-reconciliation/live_state_reconciliation.json \
  --artifact pre_release_checklist=test_artifacts/live_certification/pre-release/pre_release_checklist.json \
  --artifact evidence_chain=test_artifacts/live_certification/evidence-chain/evidence_chain_index.json \
  --artifact cdc_apply_certification=test_artifacts/cdc_apply/mssql_to_clickhouse/orders/cdc_apply_certification.json \
  --format json
```

## Output tree

```text
test_artifacts/route_rc/mssql_to_clickhouse/
  route_rc_orchestration.json
  route_rc_orchestration.md
  route-certification-pack/route_certification_pack.json
  route-certification-pack/readiness/route_readiness.json
  route-live-certification/route_live_certification.json
  route-release-gate/route_release_gate.json
  release-evidence-pack/release_evidence_pack.json
  ../route_rc_execution/mssql_to_clickhouse/route_rc_execution.json
```

`route_rc_orchestration.json` has schema
`dpone.route_rc_orchestrator.v1`. It contains the ordered release train, command
plan, checksums, blockers, and an artifact index.

## Operator runbook

Failure: `route_certification_pack.not_passed`.

1. Open `route-certification-pack/readiness/route_readiness.json`.
2. Fix the missing or red source evidence.
3. Re-run the orchestrator.

Failure: `route_live_certification.not_passed`.

1. Open `route-live-certification/route_live_certification.json`.
2. Run the Docker-live or vendor-live command that produces the missing live
   artifact.
3. Re-run the orchestrator with the new artifact path.

Failure: `route_release_gate.not_passed`.

1. Open `route-release-gate/route_release_gate.json`.
2. Fix `.missing`, `.not_passed`, or `.route_mismatch` blockers upstream.
3. Do not create a release tag until the gate is green.

Failure: `release_evidence_pack.not_passed`.

1. Open `release-evidence-pack/release_evidence_pack.json`.
2. Attach missing release evidence such as `pre_release_checklist`.
3. Keep generated evidence immutable; regenerate instead of editing JSON.

## Related docs

- [Route live certification](route-live-certification.md)
- [Route certify](route-certify.md)
- [Route release candidate executor](route-rc-executor.md)
- [Route release gate](route-release-gate.md)
- [Release evidence](release-evidence.md)
- [Live certification](live-certification.md)
