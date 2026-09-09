# Developer CDC observability evidence

CDC observability evidence is the control-plane gate that proves a CDC stream is
operationally healthy after apply correctness and snapshot handoff have already
been certified.

It does not execute heavy tests, source reads, or sink writes. It parses
artifact JSON, evaluates a small SLO profile, writes normalized evidence files,
and returns a stable report for CLI, CI, and release review.

## Module boundaries

| Module | Responsibility |
| --- | --- |
| `dpone.ops.cdc.observability_models` | `CdcTelemetrySnapshot`, `CdcSloProfile`, `CdcObservabilityEvidenceItem`, `CdcObservabilityDecision`, `CdcObservabilityReport`, and evidence-domain constants. |
| `dpone.ops.cdc.observability` | `CdcObservabilityEvidenceService`, evidence factories, upstream artifact parsing, SLO evaluation, and artifact writing. |
| `dpone.ops.catalog_cdc` | CDC service factory facade for apply certification, handoff, and observability evidence. |
| `dpone.services.ops.command_handlers_release` | Thin CLI handler that invokes the CDC catalog and emits JSON or Markdown. |

## Interfaces

`CdcTelemetrySnapshot` normalizes a single stream telemetry sample:
`captured_at`, `lag_seconds`, `freshness_seconds`,
`retention_remaining_seconds`, `events_per_second`, `duplicate_events`,
`replayed_events`, `offset_commit_status`, and `last_committed_offset`.

`CdcSloProfile` carries pluggable thresholds:
`max_lag_seconds`, `max_freshness_seconds`,
`min_retention_remaining_seconds`, `min_events_per_second`,
`max_duplicate_events`, optional `max_replayed_events`, and
`require_committed_offset`.

`CdcObservabilityDecision` is the pure policy result: `passed`, `blockers`, and
`warnings`.

`CdcObservabilityEvidenceService` orchestrates artifact loading, stream
identity extraction, evidence factory execution, upstream blocker propagation,
and report writing.

## Evidence taxonomy

The service emits exactly these normalized evidence domains:

- `cdc_lag_slo`
- `cdc_freshness_slo`
- `cdc_retention_risk`
- `cdc_offset_commit_health`
- `cdc_duplicate_replay_rate`
- `cdc_throughput_slo`

Each evidence factory has the same shape:

```python
Callable[[CdcTelemetrySnapshot, CdcSloProfile], CdcObservabilityEvidenceItem]
```

This keeps the service open for new evidence domains without turning it into a
route-specific decision tree.

## Extension rules

Do not add route-specific branches to the service. Route-specific behavior
belongs in upstream route metadata, SLO profile files, or a small injected
evidence factory selected by composition.

When adding a new CDC route:

1. Add or reuse the matrix-backed CDC handoff profile.
2. Generate CDC apply certification and handoff artifacts first.
3. Export telemetry using the normalized metrics contract.
4. Add a route-specific SLO profile only when the defaults are not appropriate.
5. Add user docs, developer docs, source-sink examples, and runbooks.
6. Add tests for service behavior, CLI behavior, docs links, and failure
   blockers.
7. Run full CI, docs, import, layer, module-size, and architecture-fitness
   checks.

## Dependency rules

`dpone.ops.cdc.observability` may import CDC value objects and standard-library
JSON/path helpers. It must not import live connector adapters, runtime CDC
readers, ClickHouse clients, MSSQL clients, orchestration runners, or CI
workflow code.

Keep the package as a deterministic evidence layer: local JSON in, normalized
JSON/Markdown out.
