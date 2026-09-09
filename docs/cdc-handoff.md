# CDC snapshot handoff

`dpone ops cdc-handoff` evaluates whether one `source -> sink -> cdc`
stream has enough evidence to move from an initial snapshot into CDC apply.
It is a control-plane gate. It does not connect to MSSQL, ClickHouse, or any
other live system, and it does not execute heavy replay tests.

## User workflow

Use this command after the snapshot load, CDC window capture, sink apply test,
and reconciliation jobs have already produced artifacts.
For fixture-based certification, run
[`dpone ops cdc-apply-certification`](cdc-apply-certification.md) first and use
its generated `evidence/*.json` files or embedded handoff report.

1. Capture the snapshot boundary from the source CDC backend.
2. Capture the CDC window that starts after the snapshot boundary.
3. Run retention preflight for that window.
4. Apply the CDC window into the sink staging/apply path.
5. Produce delete semantics, typed hash, and schema drift evidence.
6. Run `dpone ops cdc-handoff` with every evidence artifact.
7. Run [`dpone ops cdc-observability-evidence`](cdc-observability-evidence.md)
   when telemetry and CDC SLOs are part of the release gate.
8. Use `cdc_handoff.json` and `cdc_observability.json` as go/no-go contracts
   for release review.

The first profiled route is `mssql -> clickhouse` with MSSQL CDC as the source
backend and ClickHouse typed staging apply as the sink-side apply contract.

## CLI examples

Minimal JSON output for `mssql -> clickhouse`:

```bash
dpone ops cdc-handoff \
  --source mssql \
  --sink clickhouse \
  --strategy cdc \
  --source-dataset dbo.orders \
  --target-dataset analytics.orders \
  --artifact cdc_snapshot_boundary=.dpone/cdc/orders/snapshot_boundary.json \
  --artifact cdc_window=.dpone/cdc/orders/window.json \
  --artifact retention_preflight=.dpone/cdc/orders/retention.json \
  --artifact cdc_apply_correctness=.dpone/cdc/orders/apply_correctness.json \
  --artifact delete_semantics=.dpone/cdc/orders/delete_semantics.json \
  --artifact typed_cdc_hash=.dpone/cdc/orders/typed_hash.json \
  --artifact schema_drift_governance=.dpone/cdc/orders/schema_drift.json \
  --output-dir .dpone/cdc-handoff/orders \
  --format json
```

Markdown output for a release review:

```bash
dpone ops cdc-handoff \
  --source mssql \
  --sink clickhouse \
  --source-dataset dbo.orders \
  --target-dataset analytics.orders \
  --artifact cdc_snapshot_boundary=.dpone/cdc/orders/snapshot_boundary.json \
  --artifact cdc_window=.dpone/cdc/orders/window.json \
  --artifact retention_preflight=.dpone/cdc/orders/retention.json \
  --artifact cdc_apply_correctness=.dpone/cdc/orders/apply_correctness.json \
  --artifact delete_semantics=.dpone/cdc/orders/delete_semantics.json \
  --artifact typed_cdc_hash=.dpone/cdc/orders/typed_hash.json \
  --artifact schema_drift_governance=.dpone/cdc/orders/schema_drift.json \
  --format md
```

## Evidence contract

The command uses the same normalized evidence semantics as route readiness:
JSON artifacts may set `passed`, `summary`, and `blockers`. Missing required
artifacts become blockers automatically.

| Evidence | Purpose |
| --- | --- |
| `cdc_snapshot_boundary` | Proves the snapshot finish boundary is durable and source-specific. For MSSQL CDC this is an LSN. |
| `cdc_window` | Proves the CDC window starts after the snapshot boundary and ends at a known high watermark. |
| `retention_preflight` | Proves the source or replay artifact can still serve the requested CDC window. |
| `cdc_apply_correctness` | Proves insert/update/delete events were applied into the sink with deterministic idempotency. |
| `delete_semantics` | Proves physical delete, soft delete, or tombstone behavior is explicit and tested. |
| `typed_cdc_hash` | Proves source and sink values match under the route's type fidelity policy. |
| `schema_drift_governance` | Proves schema changes observed during CDC are governed before apply. |

## Generated artifacts

The command writes:

- `cdc_handoff.json`: stable machine contract for CI, release gates, and docs.
- `cdc_handoff.md`: human-readable release review summary.

The report includes the canonical route id, stream id, source backend, sink
apply mode, evidence table, blockers, warnings, and next actions.

## Runbook

If the report fails:

1. Open `cdc_handoff.json`.
2. Start with `blockers`; each blocker maps to one required evidence artifact.
3. Regenerate the source artifact in the specialized CDC or reconciliation job.
4. Keep the snapshot boundary and CDC window immutable for the reviewed run.
5. Re-run `dpone ops cdc-handoff`.
6. Only advance the durable CDC offset after the sink load has committed and the
   handoff report is green.

Use [CDC replay planning](cdc.md) when the requested window is outside the
current source retention range or requires artifact-backed replay.
