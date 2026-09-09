# CDC observability evidence

`dpone ops cdc-observability-evidence` turns CDC handoff, CDC apply
certification, and normalized telemetry artifacts into a release-grade
observability evidence pack.

Use it after `dpone ops cdc-apply-certification` and `dpone ops cdc-handoff`
when a route needs replication-grade proof that lag, freshness, retention,
offset commits, replay behavior, and throughput are inside the configured SLO.

The first profiled route is `mssql -> clickhouse`, but the command is generic:
it reads the stream and route identity from upstream JSON artifacts and applies
the same evidence taxonomy to any future `source -> sink -> cdc` route.

## User workflow

1. Generate CDC apply evidence from a credential-free fixture.
2. Review or run the embedded `dpone ops cdc-handoff` report.
3. Export a normalized telemetry snapshot from the CDC consumer, orchestrator,
   or observability workflow.
4. Optionally provide a route-specific SLO profile JSON.
5. Run `dpone ops cdc-observability-evidence`.
6. Attach `cdc_observability.json`, `cdc_observability.md`, and
   `evidence/*.json` to release review.
7. Run [`dpone ops cdc-recovery-evidence`](cdc-recovery-evidence.md) when
   fault-injection recovery is part of the release gate.
8. Advance durable CDC offsets only when apply, handoff, observability, and
   recovery evidence are all green.

## Metrics contract

The metrics JSON is a single telemetry snapshot for one CDC stream:

```json
{
  "captured_at": "2026-06-12T12:00:00Z",
  "lag_seconds": 30,
  "freshness_seconds": 45,
  "retention_remaining_seconds": 7200,
  "events_per_second": 1200,
  "duplicate_events": 1,
  "replayed_events": 2,
  "offset_commit_status": "committed",
  "last_committed_offset": "0x13"
}
```

The command evaluates these evidence domains:

| Evidence | Purpose | Default blocker |
| --- | --- | --- |
| `cdc_lag_slo` | Source-to-sink CDC lag is below `max_lag_seconds`. | `cdc_lag_slo.exceeded` |
| `cdc_freshness_slo` | Latest applied event is fresh enough for the route. | `cdc_freshness_slo.exceeded` |
| `cdc_retention_risk` | Source retention still covers the recovery window. | `cdc_retention_risk.window_not_available` |
| `cdc_offset_commit_health` | Consumer offset is durably committed. | `cdc_offset_commit_health.not_committed` |
| `cdc_duplicate_replay_rate` | Duplicate and replayed events stay inside the idempotency budget. | `cdc_duplicate_replay_rate.exceeded` |
| `cdc_throughput_slo` | Apply throughput is above the configured floor. | `cdc_throughput_slo.too_low` |

Optional SLO profile:

```json
{
  "max_lag_seconds": 300,
  "max_freshness_seconds": 600,
  "min_retention_remaining_seconds": 1800,
  "min_events_per_second": 1,
  "max_duplicate_events": 2,
  "max_replayed_events": 10,
  "require_committed_offset": true
}
```

## CLI examples

Generate observability evidence for `mssql -> clickhouse`:

```bash
dpone ops cdc-observability-evidence \
  --handoff-json test_artifacts/cdc_apply/mssql_to_clickhouse/orders/handoff/cdc_handoff.json \
  --apply-certification-json test_artifacts/cdc_apply/mssql_to_clickhouse/orders/cdc_apply_certification.json \
  --metrics-json test_artifacts/cdc_metrics/mssql_to_clickhouse/orders/metrics.json \
  --slo-json test_artifacts/cdc_metrics/mssql_to_clickhouse/orders/slo.json \
  --output-dir test_artifacts/cdc_observability/mssql_to_clickhouse/orders \
  --format json
```

For default SLOs, omit `--slo-json`:

```bash
dpone ops cdc-observability-evidence \
  --handoff-json .dpone/cdc-handoff/orders/cdc_handoff.json \
  --apply-certification-json .dpone/cdc-apply/orders/cdc_apply_certification.json \
  --metrics-json .dpone/cdc-metrics/orders/metrics.json \
  --output-dir .dpone/cdc-observability/orders \
  --format md
```

## Generated artifacts

| Artifact | Purpose |
| --- | --- |
| `cdc_observability.json` | Stable schema `dpone.cdc_observability.v1`, stream id, route id, metrics, SLO profile, blockers, upstream paths, and evidence paths. |
| `cdc_observability.md` | Human-readable release review. |
| `evidence/cdc_lag_slo.json` | Lag SLO evidence. |
| `evidence/cdc_freshness_slo.json` | Freshness SLO evidence. |
| `evidence/cdc_retention_risk.json` | Retention-window risk evidence. |
| `evidence/cdc_offset_commit_health.json` | Durable offset commit evidence. |
| `evidence/cdc_duplicate_replay_rate.json` | Duplicate and replay budget evidence. |
| `evidence/cdc_throughput_slo.json` | Throughput floor evidence. |

## Runbook

If `cdc_observability.json` is red:

1. Fix upstream `cdc_handoff.not_passed` or
   `cdc_apply_certification.not_passed` before tuning observability thresholds.
2. Fix `cdc_lag_slo.exceeded` by checking source capture delay, target apply
   backlog, batch size, network IO, and ClickHouse merge pressure.
3. Fix `cdc_freshness_slo.exceeded` by confirming the consumer is still polling
   and the sink apply loop is committing new events.
4. Fix `cdc_retention_risk.window_not_available` before retrying the window;
   do not continue if the source CDC retention horizon has passed.
5. Fix `cdc_offset_commit_health.not_committed` before advancing offsets or
   promoting the stream.
6. Fix `cdc_duplicate_replay_rate.exceeded` by checking idempotency keys,
   replay windows, and exactly-once handoff assumptions.
7. Fix `cdc_throughput_slo.too_low` by tuning CDC batch size, ClickHouse insert
   format, staging table shape, and merge finalization cadence.
8. Re-run `dpone ops cdc-observability-evidence` and attach the new artifacts.
