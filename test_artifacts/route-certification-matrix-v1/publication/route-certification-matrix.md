# dpone route certification matrix

- Expected commit: `17e61ed053c9853b8aec9ac5fe936c3bba46bb50`
- Evaluated at: `2026-07-17T12:06:31.714515Z`
- Supplied evidence failures: `False`

| source | sink | strategy | transport | schema evolution | runtime | status | evidence |
|---|---|---|---|---|---|---|---|
| `mssql` | `clickhouse` | `incremental_merge` | `native_bcp_to_clickhouse` | `widening` | `kpo` | **experimental** | contract=UNVERIFIED; live=UNVERIFIED; prod=UNVERIFIED |

## Counts

- `experimental`: `1`
- `route-certified`: `0`
- `production-certified`: `0`
- `enterprise-certified`: `0`

## Blockers

- `route_matrix.live_evidence_unverified`
- `route_matrix.production_evidence_unverified`

## Interpretation

- Catalog membership is not production authorization.
- Missing or unavailable live evidence is `UNVERIFIED`, never `PASS`.
- Repair upstream evidence and republish; do not edit this generated matrix.
