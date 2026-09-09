# Feature design: independent native quality row authorities

- Status: IMPLEMENTED
- Owner: dpone maintainers
- Issue: SS-76, global Airflow self-service review follow-up
- Target release: next patch after 0.73.1
- Last verified: 2026-07-19
- Evidence: `tests/test_native_transfer_row_authority.py`,
  `tests/test_native_transfer_quality_scope_actual_rows.py`,
  `tests/test_etl_processor_native_transfer_resume.py` (quality resume cases)

## Executive summary

Native-transfer quality reconciliation currently can compare a sink loader row
count with the same value copied into the source side. A malformed loader
result is also coerced with `int(...)`. Both defects can create a false passed
quality receipt.

This bug fix gives source and target counts independent authorities:

- source rows come only from a completed source export that reports an actual
  non-negative integer;
- target rows come only from a completed sink load or staged-target observation
  that reports an actual non-negative integer;
- missing, estimated, legacy, malformed, or ambiguous values are unavailable
  and make every dependent gate fail closed.

The change uses additive checkpoint diagnostics and corrects the existing
`rows_exported` meaning. It does not add a new checkpoint status, automatic
reconciliation, a target fence, or a durable transaction.

## User and operator contract

A pipeline author does not configure row authorities. Existing quality gates
continue to use:

```yaml
quality:
  gates:
    - id: rows_match
      type: row_count_reconciliation
      severity: error
```

When either independent observation is unavailable, the gate returns
`failed` with an unavailable metric. It never substitutes an estimate, the
other side's count, `0`, or a coerced value.

Operators may see older committed checkpoints become insufficient for a
resume-only row reconciliation. That is intentional fail-closed behavior.
Rerunning the source export or using a separately certified live target probe
creates current evidence; dpone does not manufacture provenance for old state.

## Public state and evidence

`PartitionCheckpoint.rows_exported` means actual rows reported by the source
export. New checkpoints also contain bounded additive diagnostics:

```json
{
  "row_count_authority": "source_export_and_target_loader",
  "rows_loaded": 100
}
```

Rules:

1. `rows_exported` and `diagnostics.rows_loaded` accept only non-negative
   integers; `bool`, strings, floats, mappings, lists, negative values, and
   missing values are never coerced.
2. `row_count_authority` is written only when both values came from their
   declared completed operations.
3. Quality scope trusts checkpoint row counts only when the marker and both
   values are valid.
4. Older checkpoints without the marker remain readable for resume planning,
   but their quality source/target counts are `None`.
5. Checkpoint identity, statuses, guard phases, query/schema hashes, and retry
   ordering do not change.
6. Safe evidence contains only counts and authority labels, never row data,
   SQL, connector responses, or backend error text.

For lazy transfer plans, per-slice evidence contains separate
`rows_exported` and `rows_loaded`. For physical file partitions, the completed
export records `rows_exported` on its artifact and the completed loader records
`rows_loaded` on the same attempt-owned artifact.

## Algorithm

### Source export

1. Execute the source-native export.
2. Read its explicit completed row count.
3. Validate `type(value) is int` and `value >= 0`.
4. On invalid output, fail with `native_transfer_source_row_count_invalid`.
5. Store the validated value as `rows_exported`; do not infer it from planned
   partition estimates.

For BCP pipe streaming, the completed BCP process callback assigns
`rows_exported` only after the producer exits successfully. A consumer result
cannot populate this field.

### Target load

1. Invoke the sink loader.
2. Validate its result with the same exact non-negative integer rule.
3. On invalid output, fail with `native_transfer_target_row_count_invalid`.
4. Store the value as `rows_loaded`.
5. Never use `int(value)`, `value or 0`, or source rows as a fallback.

### Checkpoint commit

1. Build each terminal checkpoint only after target success.
2. Read source and target values from the same attempt-owned partition or
   per-slice evidence.
3. Set `rows_exported` to the source value.
4. Add `diagnostics.rows_loaded` and the authority marker only when both values
   validate.
5. If either value is unavailable, persist the checkpoint without the marker;
   dependent quality gates later fail closed.

### Quality scope

1. For active staged work, sum actual source-export values and compare them to
   the independent staged target count.
2. For committed or resume-only work, sum source and target values separately
   across exact matching checkpoints.
3. If any planned partition lacks a trusted value, that side is `None`.
4. Add skipped committed partitions once and active partitions once.
5. Build `QualityProbeSnapshot` only from the two independent aggregates.

## Failure and compatibility semantics

- Invalid source or target row values fail before a passed receipt can exist.
- A target mutation that already succeeded still follows the existing
  post-commit failure outcome and target-commit guard behavior.
- Old checkpoints do not fail deserialization or resume planning.
- Old checkpoints cannot certify row reconciliation.
- No automatic retry-safety or durable-commit claim is added.
- Live route certification remains `UNVERIFIED` until an approved MSSQL and
  ClickHouse environment proves source, target, checkpoint, and evidence parity.

## Components and dependency direction

| Component | Responsibility |
|---|---|
| MSSQL export adapters | publish actual source rows |
| `PartitionedTransferPlanArtifact` | preserve separate per-slice observations |
| `PartitionedFileExportArtifact` | bind each loader result to its attempt artifact |
| checkpoint lifecycle/plan adapter | persist additive provenance |
| `NativeTransferQualityScope` | aggregate each side independently |
| quality snapshot validator | reject malformed typed probes |

Connector adapters produce observations. Runtime policy validates and combines
them. Checkpoint storage persists typed state. No connector imports governance
policy, and governance does not call a credential backend.

## Test plan

- loader returns `True`, string, float, negative, mapping, list, or `None`:
  fail closed without checkpoint advancement;
- source export returns the same malformed matrix: fail closed;
- source `10`, target `0`: reconciliation and target `min_rows` fail;
- source `10`, target `9`: tolerance is applied to distinct values;
- skipped and active partitions are summed once on each side;
- old checkpoint without authority marker yields both counts unavailable;
- one malformed checkpoint among valid checkpoints makes only the affected
  logical quality scope unavailable;
- file and pipe paths preserve independent counts;
- receipt/evidence never reports `passed` after dropping a required metric;
- focused tests are mocked/local; live MSSQL to ClickHouse remains explicitly
  `UNVERIFIED` unless executed in an approved environment.

## Rollout and rollback

Ship behind no feature flag because false-green quality evidence is a release
blocker. Rollback reintroduces unsafe evidence and is not an operational
recovery mechanism. Deployments should drain older workers before enabling
automatic retries, as already required by the target-commit guard migration.

Update the compatibility guide and changelog with the old-checkpoint
fail-closed behavior. The generated schemas do not change because diagnostics
are already an additive bounded map.
