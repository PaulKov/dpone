# Developer CDC promotion gate

CDC promotion gate is the control-plane bundle that converts upstream CDC
evidence into a final replication-readiness decision. It is intentionally small:
local JSON artifacts go in, normalized JSON/Markdown promotion artifacts come
out.

The service does not promote offsets, execute live CDC reads, run sink apply
work, mutate target schemas, or execute heavy tests. External release or
orchestration tooling may act on `promote_offsets=true`, but this package only
records the decision.

## Module boundaries

| Module | Responsibility |
| --- | --- |
| `dpone.ops.cdc.promotion_models` | `CdcPromotionEvidenceItem`, `CdcPromotionDecision`, `CdcPromotionReport`, schema version, and evidence-domain constants. |
| `dpone.ops.cdc.promotion` | `CdcPromotionGateService`, upstream artifact parsing, stream identity consistency, blocker aggregation, decision creation, and artifact writing. |
| `dpone.ops.catalog_cdc` | CDC service factory facade for apply certification, handoff, observability, recovery, schema evolution, and promotion gate services. |
| `dpone.services.ops.command_handlers_release` | Thin CLI handler that invokes the CDC catalog and emits JSON or Markdown. |
| `dpone.commands.ops_parsers_artifacts` | Argparse wiring only. |

## Interfaces

`CdcPromotionEvidenceItem` is the normalized per-domain evidence record. It
contains `name`, `passed`, `summary`, `blockers`, and structured `details`.

`CdcPromotionDecision` is the pure decision object. It contains `passed`,
`production_ready`, `promote_offsets`, `blockers`, `warnings`, and
`next_actions`.

`CdcPromotionReport` is the stable public report contract. It serializes schema
version `dpone.cdc_promotion_gate.v1`, stream and route identity, final
promotion flags, blockers, warnings, next actions, normalized evidence,
evidence artifact paths, upstream artifact paths, and output paths.

`CdcPromotionGateService` orchestrates five upstream report reads, creates gate
evidence, validates stream identity consistency, computes the offset promotion
decision, and writes the report. The service receives file paths and an output
directory so it can stay deterministic in CLI, CI, and tests.

## Evidence taxonomy

The service emits exactly these normalized domains:

- `cdc_apply_gate`
- `cdc_handoff_gate`
- `cdc_observability_gate`
- `cdc_recovery_gate`
- `cdc_schema_evolution_gate`
- `cdc_stream_identity_consistency`
- `cdc_offset_promotion_decision`

Each upstream gate uses the same rule: the report must exist, parse as JSON, and
have `passed=true`. Upstream blockers are copied into the promotion bundle for
operator context.

Stream identity is inferred from upstream `stream.stream_id` and
`route.case_id` fields. Missing, blank, or mismatched identities block the
promotion decision because offsets must not be advanced for an ambiguous
stream.

## Extension rules

Do not add route-specific branches to the service. Route-specific behavior
belongs in upstream route readiness metadata, CDC handoff profiles, fixture
generation, schema-change planning, policy JSON, or a small injected policy
selected by composition.

When adding a new CDC route:

1. Add matrix-backed route and CDC handoff profile metadata.
2. Generate CDC apply, handoff, observability, recovery, and schema evolution
   artifacts first.
3. Ensure every upstream report emits the same stream id and route id.
4. Run `dpone ops cdc-promotion-gate` and publish the promotion bundle.
5. Add user docs, developer docs, source-sink examples, and runbooks.
6. Add service tests, CLI tests, docs-contract tests, and route-specific
   examples only where the route has distinct evidence inputs.
7. Run full CI, docs, import, layer, module-size, and architecture-fitness
   checks.

Keep this package closed to live IO and open to new evidence producers. New
stream types should require new upstream metadata and tests, not new command
logic.
