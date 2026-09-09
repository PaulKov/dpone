# Developer CDC schema evolution evidence

CDC schema evolution evidence is the control-plane gate for source schema
changes observed during CDC. It proves that a schema change has been captured,
classified, planned against the sink, dry-run, approved when breaking, and
ordered before CDC offset promotion.

The service does not execute live DDL, live CDC reads, live sink writes, or
heavy tests. It parses local JSON artifacts, evaluates generic evidence
factories, and writes stable JSON/Markdown reports.

## Module boundaries

| Module | Responsibility |
| --- | --- |
| `dpone.ops.cdc.schema_evolution_models` | `CdcSchemaChangeEvent`, `CdcSchemaEvolutionPlan`, `CdcSchemaEvolutionPolicy`, `CdcSchemaEvolutionEvidenceItem`, `CdcSchemaEvolutionDecision`, `CdcSchemaEvolutionReport`, and evidence-domain constants. |
| `dpone.ops.cdc.schema_evolution` | `CdcSchemaEvolutionEvidenceService`, upstream artifact parsing, schema-change parsing, evidence factory execution, blocker aggregation, and artifact writing. |
| `dpone.ops.catalog_cdc` | CDC service factory facade for apply certification, handoff, observability, recovery, and schema evolution evidence. |
| `dpone.services.ops.command_handlers_release` | Thin CLI handler that invokes the CDC catalog and emits JSON or Markdown. |

## Interfaces

`CdcSchemaChangeEvent` is the normalized source event. It records the change id,
kind, capture timestamp, source and target table/column, old and new type,
nullability, source offset, and whether the change is breaking.

`CdcSchemaEvolutionPlan` is the target-side handling plan. It records sink
impact, compatibility level, target DDL preview, DDL dry-run status, backfill
requirement, backfill plan, type widening safety, offset/schema ordering safety,
and approvals.

`CdcSchemaEvolutionPolicy` is the small policy object used by evidence
factories: allowed compatibility levels, whether DDL dry-run is required,
whether breaking changes require approval, and whether schema evidence must be
governed before offset promotion.

`CdcSchemaEvolutionDecision` is the pure decision output: `passed`, `blockers`,
and `warnings`.

`CdcSchemaEvolutionEvidenceService` orchestrates artifact loading, stream
identity extraction, schema-change parsing, policy parsing, evidence factory
execution, upstream blocker propagation, and report writing.

## Evidence taxonomy

The service emits exactly these normalized domains:

- `cdc_schema_change_capture`
- `cdc_schema_compatibility`
- `cdc_type_widening_safety`
- `cdc_target_ddl_dry_run`
- `cdc_backfill_requirement`
- `cdc_breaking_change_gate`
- `cdc_offset_schema_ordering`

Each evidence factory has the same shape:

```python
Callable[
    [CdcSchemaChangeEvent, CdcSchemaEvolutionPlan, CdcSchemaEvolutionPolicy],
    CdcSchemaEvolutionEvidenceItem,
]
```

This keeps the orchestrator open for new governance checks while closed to
route-specific branching.

## Extension rules

Do not add route-specific branches to the service. Route-specific behavior
belongs in upstream route metadata, schema-change generation, target DDL
planning, policy JSON, or a small injected evidence factory selected by
composition.

When adding a new CDC route:

1. Add or reuse the matrix-backed CDC handoff profile.
2. Generate CDC apply, handoff, observability, and recovery artifacts first.
3. Emit schema-change JSON using the normalized `change` and `plan` contract.
4. Add a route-specific schema policy only when default compatibility and DDL
   governance thresholds are not appropriate.
5. Add tests for service behavior, CLI behavior, docs links, and failure
   blockers.
6. Add user docs, developer docs, source-sink examples, and runbooks.
7. Run full CI, docs, import, layer, module-size, and architecture-fitness
   checks.

Keep this package deterministic. Local JSON goes in; normalized JSON/Markdown
comes out. Runtime CDC readers, connector clients, live DDL executors, and sink
apply engines stay outside this package.
