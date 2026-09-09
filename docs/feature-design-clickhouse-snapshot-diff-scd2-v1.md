# Feature design: ClickHouse snapshot_diff / scd2 staged finalizers v1

- Status: APPROVED
- Owner: dpone maintainers
- Issue: Phase B2 of postgres route live wide certification
- Target release: next patch after stack merges
Last verified: 2026-07-23
Approval: maintainer APPROVED 2026-07-23 — approach A (port PG/MSSQL/BQ
  staging-first algorithm; default `lightweight_delete_insert`, optional
  `shadow_swap` / `mutation_delete_insert` via existing `merge_policy`)

## Executive summary

ClickHouse staged load rejected `snapshot_diff` and `scd2` at finalize, so
postgres→CH / mysql→CH wide certification documented those strategies as N/A.
This change adds set-based ClickHouse finalizers that match the public
manifest contract already used by Postgres, MSSQL, and BigQuery, reusing
`StrategyMetadataEnricher` technical columns and existing merge-policy delete
helpers.

## Personas and customer journey

| Persona | Goal | Pain | Success |
|---|---|---|---|
| Data engineer | History/diff loads into ClickHouse | N/A or ad-hoc SQL | Wide live + guide claim both strategies |
| Maintainer | One algorithm across sinks | Forked semantics | CH finalizers mirror MSSQL state machine |

## Scope

### In scope

- `ClickHouseStagedLoadService.finalize` dispatch for `snapshot_diff` / `scd2`
- Missing-key policies: diff `hard_delete|soft_delete|ignore`; scd2 `expire|ignore`
- Changed-key reload via delete+insert (diff) and expire+insert (scd2)
- Default `merge_policy=lightweight_delete_insert`; optional shadow/mutation
- Hermetic contracts + postgres→CH (and mysql→CH if suite exists) live IT
- Docs/matrix/CHANGELOG after PASS

### Non-goals

- `backfill.inner_mode` = snapshot_diff/scd2
- ReplacingMergeTree as the history engine
- CDC apply path changes

## Public contract

No new CLI. Existing manifest keys:

```yaml
sink.strategy.mode: snapshot_diff|scd2
sink.strategy.unique_key: [...]
sink.strategy.diff.delete_policy: hard_delete|soft_delete|ignore
sink.strategy.scd2: {valid_*_column, current_flag_column, row_hash_column, delete_policy}
sink.strategy.merge_policy: lightweight_delete_insert|shadow_swap|mutation_delete_insert
```

Technical columns from `StrategyMetadataEnricher` (file + row artifacts).

## Algorithm

```text
snapshot_diff:
  1. validate unique_key + staging duplicates
  2. if target missing: swap staging → target
  3. apply missing-key policy (DELETE / ALTER UPDATE deleted_at / ignore)
  4. delete matching staging keys (merge_policy)
  5. INSERT SELECT staging → target

scd2:
  1. validate unique_key + staging duplicates
  2. if target missing: swap staging → target
  3. ALTER UPDATE expire currents where key matches and hash differs
  4. if delete_policy=expire: ALTER UPDATE expire currents missing from staging
  5. INSERT staging rows that have no matching current+hash
```

Soft-delete / expire use `ALTER TABLE … UPDATE … SETTINGS mutations_sync=<n>`.
Hard deletes reuse `ClickHouseStagingFinalizer` lightweight/mutation deletes.

## Module plan

- `clickhouse_production_finalize.py` — diff/scd2 finalizers (keeps staged_load <400 SLOC)
- Thin dispatch in `clickhouse_staged_load.finalize`
- Extend finalizer helpers only when reuse requires it

## Test plan

| Layer | Evidence |
|---|---|
| Hermetic | dispatch + SQL shape for hard_delete / expire / soft_delete |
| Live | postgres→CH snapshot_diff + scd2 wide; mysql→CH if suite present |

## Rollout

Stacked PR on `feat/mssql-hex-binary-character-bcp` (#436). Update guides only after live PASS.
