# CDC apply certification

`dpone ops cdc-apply-certification` generates deterministic CDC apply evidence
from a credential-free fixture. It is the recommended producer for
`cdc_apply_correctness`, `delete_semantics`, and `typed_cdc_hash` artifacts
before running `dpone ops cdc-handoff`.

The command does not open live database connections. It applies a bounded event
window in memory, compares the final rows with the expected sink state, writes
handoff-compatible evidence files, and embeds a CDC handoff report.

## User workflow

Use this command when promoting a route from fast snapshot load to
replication-grade CDC apply:

1. Export or generate a bounded CDC window fixture.
2. Include the snapshot boundary, CDC window, retention boundary, initial rows,
   CDC events, expected final rows, and unique key.
3. Run `dpone ops cdc-apply-certification`.
4. Review `cdc_apply_certification.json`.
5. Review the embedded `handoff/cdc_handoff.json`.
6. Use the generated evidence artifacts in release review.

The first profiled route is `mssql -> clickhouse`.

## Fixture contract

The fixture is a JSON object:

```json
{
  "unique_key": ["order_id"],
  "snapshot_boundary": "0x00000010",
  "window_start": "0x00000011",
  "window_end": "0x00000014",
  "retention_min": "0x00000001",
  "initial_rows": [
    {"order_id": 1, "status": "new", "amount": "10.00"},
    {"order_id": 2, "status": "new", "amount": "20.00"}
  ],
  "events": [
    {
      "operation": "insert",
      "position": "0x00000011",
      "sequence": 1,
      "key": {"order_id": 3},
      "after": {"order_id": 3, "status": "new", "amount": "30.00"}
    },
    {
      "operation": "update",
      "position": "0x00000012",
      "sequence": 2,
      "key": {"order_id": 1},
      "after": {"order_id": 1, "status": "paid", "amount": "11.00"}
    },
    {
      "operation": "delete",
      "position": "0x00000013",
      "sequence": 3,
      "key": {"order_id": 2},
      "before": {"order_id": 2, "status": "new", "amount": "20.00"}
    }
  ],
  "expected_rows": [
    {"order_id": 1, "status": "paid", "amount": "11.00"},
    {"order_id": 3, "status": "new", "amount": "30.00"}
  ]
}
```

`operation` supports `insert`, `update`, and `delete`. Replayed duplicate events
with the same deterministic event id are counted as duplicates and skipped by
the idempotent apply strategy.

## CLI examples

Generate certification and embedded handoff evidence for `mssql -> clickhouse`:

```bash
dpone ops cdc-apply-certification \
  --source mssql \
  --sink clickhouse \
  --strategy cdc \
  --source-dataset dbo.orders \
  --target-dataset analytics.orders \
  --fixture-json test_artifacts/cdc/orders/fixture.json \
  --output-dir test_artifacts/cdc_apply/mssql_to_clickhouse/orders \
  --format json
```

Then inspect the embedded handoff report:

```bash
dpone ops cdc-handoff \
  --source mssql \
  --sink clickhouse \
  --strategy cdc \
  --source-dataset dbo.orders \
  --target-dataset analytics.orders \
  --artifact cdc_snapshot_boundary=test_artifacts/cdc_apply/mssql_to_clickhouse/orders/evidence/cdc_snapshot_boundary.json \
  --artifact cdc_window=test_artifacts/cdc_apply/mssql_to_clickhouse/orders/evidence/cdc_window.json \
  --artifact retention_preflight=test_artifacts/cdc_apply/mssql_to_clickhouse/orders/evidence/retention_preflight.json \
  --artifact cdc_apply_correctness=test_artifacts/cdc_apply/mssql_to_clickhouse/orders/evidence/cdc_apply_correctness.json \
  --artifact delete_semantics=test_artifacts/cdc_apply/mssql_to_clickhouse/orders/evidence/delete_semantics.json \
  --artifact typed_cdc_hash=test_artifacts/cdc_apply/mssql_to_clickhouse/orders/evidence/typed_cdc_hash.json \
  --artifact schema_drift_governance=test_artifacts/cdc_apply/mssql_to_clickhouse/orders/evidence/schema_drift_governance.json \
  --format md
```

## Generated artifacts

The command writes:

| Artifact | Purpose |
| --- | --- |
| `cdc_apply_certification.json` | Stable schema `dpone.cdc_apply_certification.v1`, metrics, blockers, generated evidence paths, and embedded handoff paths. |
| `cdc_apply_certification.md` | Human-readable certification summary. |
| `evidence/cdc_apply_correctness.json` | Final-row correctness evidence. |
| `evidence/delete_semantics.json` | Delete handling evidence. |
| `evidence/typed_cdc_hash.json` | Typed source/sink hash evidence. |
| `evidence/cdc_snapshot_boundary.json` | Snapshot boundary evidence derived from the fixture. |
| `evidence/cdc_window.json` | Window boundary evidence derived from the fixture. |
| `evidence/retention_preflight.json` | Retention coverage evidence derived from the fixture. |
| `evidence/schema_drift_governance.json` | Schema drift governance evidence. |
| `handoff/cdc_handoff.json` | Embedded CDC handoff go/no-go report. |

## Runbook

If certification fails:

1. Open `cdc_apply_certification.json`.
2. Fix `cdc_apply_correctness.mismatch` by checking event order, unique keys,
   expected rows, and duplicate replay behavior.
3. Fix `typed_cdc_hash.mismatch` by checking type normalization and expected
   final rows.
4. Fix `delete_semantics.mismatch` by checking delete events and target
   tombstone or physical-delete policy.
5. Fix `retention_preflight.window_not_available` before retrying the window.
6. Re-run `dpone ops cdc-apply-certification`.
7. Promote the durable CDC offset only after the embedded
   `dpone ops cdc-handoff` report is green.
