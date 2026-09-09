# Developer CDC recovery evidence

CDC recovery evidence is the control-plane gate for fault-injection results. It
proves that a CDC stream can recover safely after operational failures without
silently losing rows, duplicating committed data, or advancing offsets before a
sink commit is durable.

The service does not execute live CDC reads, live sink writes, container
orchestration, or heavy tests. It parses local JSON artifacts, evaluates
generic recovery evidence factories, and writes stable JSON/Markdown reports.

## Module boundaries

| Module | Responsibility |
| --- | --- |
| `dpone.ops.cdc.recovery_models` | `CdcFailureScenario`, `CdcRecoveryPolicy`, `CdcRecoveryEvidenceItem`, `CdcRecoveryDecision`, `CdcRecoveryReport`, and recovery evidence-domain constants. |
| `dpone.ops.cdc.recovery` | `CdcRecoveryEvidenceService`, upstream artifact parsing, evidence factory execution, policy evaluation, blocker aggregation, and artifact writing. |
| `dpone.ops.catalog_cdc` | CDC service factory facade for apply certification, handoff, observability evidence, and recovery evidence. |
| `dpone.services.ops.command_handlers_release` | Thin CLI handler that invokes the CDC catalog and emits JSON or Markdown. |

## Interfaces

`CdcFailureScenario` is the normalized fault-injection result. It records the
scenario id, scenario kind, injected timestamp, failed and recovered offsets,
resume status, sink commit state, offset commit state, replay counts, poison
event counts, repair actions, and retention margin.

`CdcRecoveryPolicy` is the small threshold object used by evidence factories:
`min_recovery_margin_seconds`, `max_duplicate_events`, optional
`max_replayed_events`, `require_poison_quarantine`, and
`require_offset_commit_ordering`.

`CdcRecoveryDecision` is the pure decision output: `passed`, `blockers`, and
`warnings`.

`CdcRecoveryEvidenceService` orchestrates artifact loading, stream identity
extraction, scenario parsing, policy parsing, evidence factory execution,
upstream blocker propagation, and report writing.

## Evidence taxonomy

The service emits exactly these normalized domains:

- `cdc_restart_resume`
- `cdc_offset_commit_ordering`
- `cdc_idempotent_replay_window`
- `cdc_partial_commit_repair`
- `cdc_poison_event_quarantine`
- `cdc_retention_recovery_margin`

Each evidence factory has the same shape:

```python
Callable[[CdcFailureScenario, CdcRecoveryPolicy], CdcRecoveryEvidenceItem]
```

This keeps the service open for future recovery checks while keeping the
orchestrator closed to route-specific branching.

## Extension rules

Do not add route-specific branches to the service. Route-specific behavior
belongs in upstream route metadata, fault scenario generation, recovery policy
files, or a small injected evidence factory selected by composition.

When adding a new CDC route:

1. Add or reuse the matrix-backed CDC handoff profile.
2. Generate CDC apply, handoff, and observability artifacts first.
3. Emit fault-injection scenario JSON using the normalized contract.
4. Add a route-specific recovery policy only when default thresholds are not
   appropriate.
5. Add tests for service behavior, CLI behavior, docs links, and failure
   blockers.
6. Add user docs, developer docs, source-sink examples, and runbooks.
7. Run full CI, docs, import, layer, module-size, and architecture-fitness
   checks.

Keep this package deterministic. Local JSON goes in; normalized JSON/Markdown
comes out. Runtime CDC readers, connector clients, and sink apply engines stay
outside this package.
