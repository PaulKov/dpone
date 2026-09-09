# CDC recovery evidence

`dpone ops cdc-recovery-evidence` turns CDC apply, handoff, observability, and
fault-injection scenario artifacts into a recovery go/no-go pack.

Use it after `dpone ops cdc-apply-certification`,
`dpone ops cdc-handoff`, and `dpone ops cdc-observability-evidence` when a CDC
route needs proof that it survives restart, partial sink commit, duplicate
replay, poison events, unsafe offset ordering, and retention pressure.

The first route that should use this pack is `mssql -> clickhouse`, but the
command is generic. It reads stream and route identity from upstream artifacts
and applies the same evidence taxonomy to any future `source -> sink -> cdc`
route.

## User workflow

1. Generate CDC apply certification evidence.
2. Generate or review the CDC snapshot handoff report.
3. Generate CDC observability evidence for lag, freshness, retention, offsets,
   replay, and throughput.
4. Run a bounded fault-injection scenario in a local, CI, or staging-like
   environment.
5. Save the scenario result as JSON.
6. Optionally provide a recovery policy JSON.
7. Run `dpone ops cdc-recovery-evidence`.
8. Run [`dpone ops cdc-schema-evolution-evidence`](cdc-schema-evolution-evidence.md)
   when source schema changes or target DDL governance are part of the release gate.
9. Attach `cdc_recovery_evidence.json`, `cdc_recovery_evidence.md`, and
   `evidence/*.json` to release review.

## Failure scenario contract

The scenario JSON is a single fault-injection result for one stream:

```json
{
  "scenario_id": "orders-consumer-restart",
  "kind": "consumer_restart",
  "injected_at": "2026-06-12T12:05:00Z",
  "failed_after_offset": "0x12",
  "recovered_offset": "0x14",
  "resume_completed": true,
  "sink_commit_state": "committed",
  "offset_commit_state": "committed",
  "offset_committed_after_sink_commit": true,
  "partial_sink_commit_detected": true,
  "partial_sink_commit_repaired": true,
  "repair_actions": ["truncate staging", "replay bounded CDC window"],
  "replayed_events": 10,
  "duplicate_events": 2,
  "idempotent_replay_passed": true,
  "poison_events": 1,
  "quarantined_events": 1,
  "retention_remaining_seconds": 3600,
  "recovery_margin_seconds": 2400
}
```

Supported `kind` values:

- `consumer_restart`
- `partial_sink_commit`
- `offset_commit_failed`
- `duplicate_replay`
- `poison_event`
- `retention_window_pressure`

Optional policy JSON:

```json
{
  "min_recovery_margin_seconds": 900,
  "max_duplicate_events": 5,
  "max_replayed_events": 20,
  "require_poison_quarantine": true,
  "require_offset_commit_ordering": true
}
```

The command evaluates these evidence domains:

| Evidence | Purpose | Default blocker |
| --- | --- | --- |
| `cdc_restart_resume` | Consumer resumes from a durable offset after restart. | `cdc_restart_resume.not_resumed` |
| `cdc_offset_commit_ordering` | Offset commit happens only after sink commit. | `cdc_offset_commit_ordering.unsafe` |
| `cdc_idempotent_replay_window` | Bounded replay produces the same final sink state. | `cdc_idempotent_replay_window.failed` |
| `cdc_partial_commit_repair` | Partial sink commits are detected and repaired. | `cdc_partial_commit_repair.unrepaired` |
| `cdc_poison_event_quarantine` | Poison CDC events are quarantined. | `cdc_poison_event_quarantine.missing` |
| `cdc_retention_recovery_margin` | Source CDC retention leaves enough time to recover. | `cdc_retention_recovery_margin.too_low` |

## CLI examples

Generate recovery evidence for `mssql -> clickhouse`:

```bash
dpone ops cdc-recovery-evidence \
  --handoff-json test_artifacts/cdc_apply/mssql_to_clickhouse/orders/handoff/cdc_handoff.json \
  --apply-certification-json test_artifacts/cdc_apply/mssql_to_clickhouse/orders/cdc_apply_certification.json \
  --observability-json test_artifacts/cdc_observability/mssql_to_clickhouse/orders/cdc_observability.json \
  --scenario-json test_artifacts/cdc_recovery/mssql_to_clickhouse/orders/scenario.json \
  --policy-json test_artifacts/cdc_recovery/mssql_to_clickhouse/orders/policy.json \
  --output-dir test_artifacts/cdc_recovery/mssql_to_clickhouse/orders \
  --format json
```

For default policy thresholds, omit `--policy-json`.

## Generated artifacts

| Artifact | Purpose |
| --- | --- |
| `cdc_recovery_evidence.json` | Stable schema `dpone.cdc_recovery_evidence.v1`, stream id, route id, scenario, policy, blockers, upstream paths, and evidence paths. |
| `cdc_recovery_evidence.md` | Human-readable recovery review. |
| `evidence/cdc_restart_resume.json` | Restart/resume evidence. |
| `evidence/cdc_offset_commit_ordering.json` | Sink-commit-before-offset evidence. |
| `evidence/cdc_idempotent_replay_window.json` | Replay idempotency evidence. |
| `evidence/cdc_partial_commit_repair.json` | Partial commit detection and repair evidence. |
| `evidence/cdc_poison_event_quarantine.json` | Poison event quarantine evidence. |
| `evidence/cdc_retention_recovery_margin.json` | Recovery margin against source retention evidence. |

## Runbook

If `cdc_recovery_evidence.json` is red:

1. Fix upstream `cdc_handoff.not_passed`,
   `cdc_apply_certification.not_passed`, or `cdc_observability.not_passed`
   before reviewing recovery behavior.
2. Fix `cdc_restart_resume.not_resumed` by checking durable state, consumer
   restart logic, and the recovered offset.
3. Fix `cdc_offset_commit_ordering.unsafe` before advancing CDC offsets; sink
   commit must win before offset commit.
4. Fix `cdc_idempotent_replay_window.failed` by checking unique keys, event ids,
   replay window bounds, and target merge semantics.
5. Fix `cdc_partial_commit_repair.unrepaired` by making partial sink commits
   detectable and replayable.
6. Fix `cdc_poison_event_quarantine.missing` by routing invalid CDC events to
   quarantine with enough context for replay.
7. Fix `cdc_retention_recovery_margin.too_low` before retrying; the source CDC
   retention horizon may no longer cover the recovery window.
8. Re-run `dpone ops cdc-recovery-evidence` and attach the new artifacts.
