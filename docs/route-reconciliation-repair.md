# Route reconciliation repair

`dpone ops route-reconciliation-repair` turns bounded source/target row
comparison into a route-level `source -> sink -> strategy` repair receipt. Use
it after snapshot, CDC apply, backfill, or incident recovery when the route must
prove that data differences are either absent or have an actionable repair
plan.

The command is local and artifact-first. It reads source and target row samples,
delegates comparison semantics to `ReconciliationService`, converts row-level
actions into route repair actions, and writes JSON and Markdown artifacts.

## CLI example

```bash
uv run dpone ops route-reconciliation-repair \
  --source postgres \
  --sink mssql \
  --strategy incremental_merge \
  --source-rows-json test_artifacts/reconciliation/orders/source_rows.json \
  --target-rows-json test_artifacts/reconciliation/orders/target_rows.json \
  --key id \
  --compare-column status \
  --compare-column amount \
  --delete-column deleted_at \
  --output-dir test_artifacts/route_repair/postgres_to_mssql/orders \
  --format json
```

## Outputs

| File | Description |
| --- | --- |
| `route_reconciliation_repair.json` | Stable schema `dpone.route_reconciliation_repair.v1` with route identity, reconciliation summary, blockers, next actions, and repair plan. |
| `route_reconciliation_repair.md` | Human-readable route repair review and Runbook. |
| `reconciliation/reconciliation_report.json` | Underlying row-level reconciliation artifact. |

## Repair actions

| Route action | Source reconciliation action | Meaning |
| --- | --- | --- |
| `replay_source_row` | `insert_target_row` | Re-copy or replay a source row missing from the sink. |
| `replay_source_row` | `update_target_row` | Re-copy or replay a source row whose sink values differ. |
| `delete_target_row` | `delete_target_row` | Remove an extra target row or a row whose source record is physically deleted. |

## Runbook

1. Run the command for the smallest useful bounded window.
2. If `route_reconciliation_repair.unrepaired_differences` appears, execute the
   generated repair plan with the route's normal replay, resync, or merge
   mechanism.
3. Rerun the command after repair.
4. Attach green `route_reconciliation_repair.json` to `dpone ops route-readiness`
   as `route_reconciliation_repair` evidence.
5. Do not promote source state while the report is red unless a separate
   governance artifact records an explicit exception.

## Related docs

- [Route readiness](route-readiness.md)
- [Reconciliation and CDC](cdc.md)
- [CDC compare and repair](cdc-compare-repair.md)
- [Developer route schema evolution and repair](developer-route-schema-evolution-repair.md)
