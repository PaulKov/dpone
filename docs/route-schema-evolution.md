# Route schema evolution

`dpone ops route-schema-evolution` turns CDC schema evolution evidence into a
route-level `source -> sink -> strategy` gate. Use it after
`dpone ops cdc-schema-evolution-evidence` and before route readiness or release
promotion.

The command does not connect to databases and does not apply DDL. It reads an
existing schema evolution artifact, validates that the artifact belongs to the
requested route, classifies the target DDL decision, and writes stable JSON and
Markdown evidence.

## CLI example

```bash
uv run dpone ops route-schema-evolution \
  --source mssql \
  --sink clickhouse \
  --strategy incremental_merge \
  --schema-evolution-json test_artifacts/cdc_schema/mssql_to_clickhouse/orders/cdc_schema_evolution_evidence.json \
  --output-dir test_artifacts/route_schema/mssql_to_clickhouse/orders \
  --format json
```

## Outputs

| File | Description |
| --- | --- |
| `route_schema_evolution.json` | Stable schema `dpone.route_schema_evolution.v1` with route identity, profile metadata, upstream artifact references, blockers, next actions, and DDL apply decision. |
| `route_schema_evolution.md` | Human-readable route schema evolution review and Runbook. |

## Decision model

The report has an `apply_decision` object:

| Field | Meaning |
| --- | --- |
| `mode` | `auto_apply`, `manual_approval`, or `blocked`. |
| `safe_to_apply` | `true` only when the upstream schema evidence is green, the route matches, DDL dry-run passed, type widening is safe, offset ordering is safe, and no approval/backfill is required. |
| `requires_approval` | `true` when the route needs manual governance before state advancement. |
| `ddl_preview` | Target DDL preview copied from upstream schema evidence. |
| `reason` | Short operator-facing reason for the decision. |

## Runbook

1. Generate CDC schema evidence with `dpone ops cdc-schema-evolution-evidence`.
2. Run `dpone ops route-schema-evolution` with the same route identity.
3. If `route_schema_evolution.route_mismatch` appears, regenerate the upstream
   artifact for the requested route.
4. If upstream CDC blockers appear, fix the CDC schema evidence first.
5. Attach `route_schema_evolution.json` to `dpone ops route-readiness` as
   `route_schema_evolution` evidence.

## Related docs

- [Route readiness](route-readiness.md)
- [CDC schema evolution evidence](cdc-schema-evolution-evidence.md)
- [CDC schema apply](cdc-schema-apply.md)
- [Developer route schema evolution and repair](developer-route-schema-evolution-repair.md)
