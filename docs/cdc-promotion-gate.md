# CDC promotion gate

`dpone ops cdc-promotion-gate` combines the CDC evidence packs for one stream
into the final replication-readiness decision.

Use it after snapshot handoff, apply certification, observability, recovery,
and schema evolution evidence are green. The command decides whether the stream
is `production_ready` and whether downstream automation may safely
`promote_offsets`.

The first production-grade route for this bundle is `mssql -> clickhouse`, but
the command is route-neutral. It reads stream and route identity from upstream
artifacts and applies the same taxonomy to any future `source -> sink -> cdc`
stream.

## User workflow

1. Generate CDC apply certification evidence for the stream.
2. Review or generate the CDC snapshot handoff report.
3. Generate CDC observability evidence from normalized telemetry.
4. Generate CDC recovery evidence from fault-injection scenarios.
5. Generate CDC schema evolution evidence from source schema-change and target
   DDL planning artifacts.
6. Run `dpone ops cdc-promotion-gate`.
7. Attach `cdc_promotion_gate.json`, `cdc_promotion_gate.md`, and
   `evidence/*.json` to release or environment-promotion review.
8. Let an external release tool promote offsets only when
   `production_ready=true` and `promote_offsets=true`.
9. Use [CDC runtime orchestrator](cdc-runtime-orchestrator.md) for bounded
   read -> apply -> durable offset commit ticks.

## Required upstream artifacts

| Argument | Expected artifact | Why it is required |
| --- | --- | --- |
| `--apply-certification-json` | `cdc_apply_certification.json` | Apply correctness, delete semantics, typed CDC hash, and fixture replay evidence. |
| `--handoff-json` | `cdc_handoff.json` | Snapshot boundary, CDC window, retention, apply, delete, typed hash, and schema drift evidence. |
| `--observability-json` | `cdc_observability.json` | Lag, freshness, retention, offset commit, replay, and throughput SLO evidence. |
| `--recovery-json` | `cdc_recovery_evidence.json` | Restart/resume, offset ordering, idempotent replay, partial commit repair, poison event, and retention recovery evidence. |
| `--schema-evolution-json` | `cdc_schema_evolution_evidence.json` | Schema-change capture, compatibility, DDL dry-run, backfill, approval, and offset/schema ordering evidence. |

The command validates that all upstream artifacts describe the same stream and
route before it allows offset promotion.

## Evidence taxonomy

The promotion bundle emits exactly these normalized evidence domains:

| Evidence | Purpose | Default blocker |
| --- | --- | --- |
| `cdc_apply_gate` | Upstream CDC apply certification is present and passed. | `cdc_apply_gate.not_passed` |
| `cdc_handoff_gate` | Upstream snapshot handoff is present and passed. | `cdc_handoff_gate.not_passed` |
| `cdc_observability_gate` | Upstream observability evidence is present and passed. | `cdc_observability_gate.not_passed` |
| `cdc_recovery_gate` | Upstream recovery evidence is present and passed. | `cdc_recovery_gate.not_passed` |
| `cdc_schema_evolution_gate` | Upstream schema evolution evidence is present and passed. | `cdc_schema_evolution_gate.not_passed` |
| `cdc_stream_identity_consistency` | Stream and route identities match across every upstream artifact. | `cdc_stream_identity_consistency.mismatch` |
| `cdc_offset_promotion_decision` | Final offset-promotion decision is allowed only when all gates pass. | `cdc_offset_promotion_decision.blocked` |

`production_ready` and `promote_offsets` are both false when any evidence domain
has blockers.

## CLI examples

Generate the final promotion bundle for `mssql -> clickhouse`:

```bash
dpone ops cdc-promotion-gate \
  --apply-certification-json test_artifacts/cdc_apply/mssql_to_clickhouse/orders/cdc_apply_certification.json \
  --handoff-json test_artifacts/cdc_apply/mssql_to_clickhouse/orders/handoff/cdc_handoff.json \
  --observability-json test_artifacts/cdc_observability/mssql_to_clickhouse/orders/cdc_observability.json \
  --recovery-json test_artifacts/cdc_recovery/mssql_to_clickhouse/orders/cdc_recovery_evidence.json \
  --schema-evolution-json test_artifacts/cdc_schema/mssql_to_clickhouse/orders/cdc_schema_evolution_evidence.json \
  --output-dir test_artifacts/cdc_promotion/mssql_to_clickhouse/orders \
  --format json
```

For a human review:

```bash
dpone ops cdc-promotion-gate \
  --apply-certification-json test_artifacts/cdc_apply/mssql_to_clickhouse/orders/cdc_apply_certification.json \
  --handoff-json test_artifacts/cdc_apply/mssql_to_clickhouse/orders/handoff/cdc_handoff.json \
  --observability-json test_artifacts/cdc_observability/mssql_to_clickhouse/orders/cdc_observability.json \
  --recovery-json test_artifacts/cdc_recovery/mssql_to_clickhouse/orders/cdc_recovery_evidence.json \
  --schema-evolution-json test_artifacts/cdc_schema/mssql_to_clickhouse/orders/cdc_schema_evolution_evidence.json \
  --output-dir test_artifacts/cdc_promotion/mssql_to_clickhouse/orders \
  --format md
```

## Generated artifacts

| Artifact | Purpose |
| --- | --- |
| `cdc_promotion_gate.json` | Stable schema `dpone.cdc_promotion_gate.v1`, stream id, route id, `production_ready`, `promote_offsets`, blockers, upstream artifact paths, and evidence paths. |
| `cdc_promotion_gate.md` | Human-readable final CDC promotion review. |
| `evidence/cdc_apply_gate.json` | Apply certification gate evidence. |
| `evidence/cdc_handoff_gate.json` | Snapshot handoff gate evidence. |
| `evidence/cdc_observability_gate.json` | Observability gate evidence. |
| `evidence/cdc_recovery_gate.json` | Recovery gate evidence. |
| `evidence/cdc_schema_evolution_gate.json` | Schema evolution gate evidence. |
| `evidence/cdc_stream_identity_consistency.json` | Stream and route identity consistency evidence. |
| `evidence/cdc_offset_promotion_decision.json` | Final offset-promotion decision evidence. |

## Runbook

If `cdc_promotion_gate.json` is red:

1. Fix `cdc_apply_gate.not_passed` by regenerating green CDC apply
   certification evidence.
2. Fix `cdc_handoff_gate.not_passed` before treating the snapshot-to-stream
   boundary as complete.
3. Fix `cdc_observability_gate.not_passed` by restoring lag, freshness,
   retention, offset, replay, or throughput SLO evidence.
4. Fix `cdc_recovery_gate.not_passed` before trusting restart, replay,
   partial-commit repair, poison-event, or retention recovery behavior.
5. Fix `cdc_schema_evolution_gate.not_passed` before source DDL changes are
   allowed into the promoted stream.
6. Fix `cdc_stream_identity_consistency.mismatch` by regenerating artifacts for
   the same route, source dataset, and target dataset.
7. Treat `cdc_offset_promotion_decision.blocked` as the final stop signal. Do
   not promote offsets until all upstream blockers are resolved.
8. Re-run `dpone ops cdc-promotion-gate` and attach the new artifacts.
