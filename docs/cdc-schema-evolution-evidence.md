# CDC schema evolution evidence

`dpone ops cdc-schema-evolution-evidence` turns CDC apply, handoff,
observability, recovery, and schema-change planning artifacts into a DDL
governance evidence pack.

Use it when a CDC route must prove that source schema changes are captured,
classified, dry-run against the target, approved when breaking, and ordered
before durable CDC offset promotion.

The first route that should use this pack is `mssql -> clickhouse`, but the
command is generic. It reads stream and route identity from upstream artifacts
and applies the same evidence taxonomy to any future `source -> sink -> cdc`
route.

## User workflow

1. Generate CDC apply certification evidence.
2. Generate or review the CDC handoff report.
3. Generate CDC observability evidence.
4. Generate CDC recovery evidence for fault-injection scenarios.
5. Capture the source schema-change event.
6. Build a target DDL dry-run plan and compatibility decision.
7. Run `dpone ops cdc-schema-evolution-evidence`.
8. Attach `cdc_schema_evolution_evidence.json`,
   `cdc_schema_evolution_evidence.md`, and `evidence/*.json` to release
   review.
9. Run [CDC promotion gate](cdc-promotion-gate.md) when apply, handoff,
   observability, recovery, and schema evolution evidence are all green.

## Schema change contract

The schema-change JSON has two objects: `change` and `plan`.

```json
{
  "change": {
    "change_id": "orders-add-status-reason",
    "kind": "add_column",
    "captured_at": "2026-06-12T12:20:00Z",
    "source_table": "dbo.orders",
    "source_column": "status_reason",
    "target_table": "analytics.orders",
    "target_column": "status_reason",
    "old_type": "",
    "new_type": "nvarchar(100)",
    "old_nullable": true,
    "new_nullable": true,
    "source_offset": "0x15",
    "breaking": false
  },
  "plan": {
    "sink_impact": "add nullable String column",
    "compatibility_level": "compatible",
    "target_ddl_preview": "ALTER TABLE analytics.orders ADD COLUMN status_reason Nullable(String)",
    "ddl_dry_run_passed": true,
    "backfill_required": false,
    "backfill_plan": "",
    "type_widening_safe": true,
    "offset_schema_ordering_safe": true,
    "approved_by": ["data-architect"]
  }
}
```

Supported `kind` values:

- `add_column`
- `drop_column`
- `rename_column`
- `alter_type`
- `nullable_changed`
- `default_changed`

Optional policy JSON:

```json
{
  "allowed_compatibility_levels": ["compatible", "backward_compatible"],
  "require_ddl_dry_run": true,
  "require_breaking_approval": true,
  "require_offset_schema_ordering": true
}
```

The command evaluates these evidence domains:

| Evidence | Purpose | Default blocker |
| --- | --- | --- |
| `cdc_schema_change_capture` | Source schema change has a stable id, supported kind, source table, and source offset. | `cdc_schema_change_capture.invalid` |
| `cdc_schema_compatibility` | Planned target impact is compatible with route policy. | `cdc_schema_compatibility.incompatible` |
| `cdc_type_widening_safety` | Type changes are widening-safe before target apply. | `cdc_type_widening_safety.unsafe` |
| `cdc_target_ddl_dry_run` | Target DDL preview exists and dry-run validation passed. | `cdc_target_ddl_dry_run.failed` |
| `cdc_backfill_requirement` | Backfill need is explicit and planned when required. | `cdc_backfill_requirement.missing_plan` |
| `cdc_breaking_change_gate` | Breaking changes have explicit approval. | `cdc_breaking_change_gate.unapproved` |
| `cdc_offset_schema_ordering` | CDC offsets are not advanced before schema evidence is governed. | `cdc_offset_schema_ordering.unsafe` |

## CLI examples

Generate schema evolution evidence for `mssql -> clickhouse`:

```bash
dpone ops cdc-schema-evolution-evidence \
  --handoff-json test_artifacts/cdc_apply/mssql_to_clickhouse/orders/handoff/cdc_handoff.json \
  --apply-certification-json test_artifacts/cdc_apply/mssql_to_clickhouse/orders/cdc_apply_certification.json \
  --observability-json test_artifacts/cdc_observability/mssql_to_clickhouse/orders/cdc_observability.json \
  --recovery-json test_artifacts/cdc_recovery/mssql_to_clickhouse/orders/cdc_recovery_evidence.json \
  --schema-change-json test_artifacts/cdc_schema/mssql_to_clickhouse/orders/schema_change.json \
  --policy-json test_artifacts/cdc_schema/mssql_to_clickhouse/orders/policy.json \
  --output-dir test_artifacts/cdc_schema/mssql_to_clickhouse/orders \
  --format json
```

For default policy thresholds, omit `--policy-json`.

## Generated artifacts

| Artifact | Purpose |
| --- | --- |
| `cdc_schema_evolution_evidence.json` | Stable schema `dpone.cdc_schema_evolution_evidence.v1`, stream id, route id, change, plan, policy, blockers, upstream paths, and evidence paths. |
| `cdc_schema_evolution_evidence.md` | Human-readable schema evolution review. |
| `evidence/cdc_schema_change_capture.json` | Source schema-change capture evidence. |
| `evidence/cdc_schema_compatibility.json` | Compatibility evidence. |
| `evidence/cdc_type_widening_safety.json` | Type widening safety evidence. |
| `evidence/cdc_target_ddl_dry_run.json` | Target DDL dry-run evidence. |
| `evidence/cdc_backfill_requirement.json` | Backfill planning evidence. |
| `evidence/cdc_breaking_change_gate.json` | Breaking-change approval evidence. |
| `evidence/cdc_offset_schema_ordering.json` | Offset/schema ordering evidence. |

## Runbook

If `cdc_schema_evolution_evidence.json` is red:

1. Fix upstream `cdc_handoff.not_passed`,
   `cdc_apply_certification.not_passed`, `cdc_observability.not_passed`, or
   `cdc_recovery.not_passed` first.
2. Fix `cdc_schema_change_capture.invalid` by capturing a stable change id,
   supported kind, source table, and source CDC offset.
3. Fix `cdc_schema_compatibility.incompatible` by changing the DDL plan,
   routing the change through an expand/contract process, or marking it as a
   governed breaking change.
4. Fix `cdc_type_widening_safety.unsafe` before applying type changes to the
   target.
5. Fix `cdc_target_ddl_dry_run.failed` before executing any live target DDL.
6. Fix `cdc_backfill_requirement.missing_plan` by adding an explicit backfill
   or historical-repair plan.
7. Fix `cdc_breaking_change_gate.unapproved` before continuing a breaking
   source change.
8. Fix `cdc_offset_schema_ordering.unsafe`; do not advance CDC offsets until
   schema evidence is governed.
9. Re-run `dpone ops cdc-schema-evolution-evidence` and attach the new
   artifacts.
10. Re-run `dpone ops cdc-promotion-gate` before any external system promotes
    CDC offsets.
