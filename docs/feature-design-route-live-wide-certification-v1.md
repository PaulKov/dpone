# Feature design: Route live wide-type + full-strategy certification standard v1

- Status: IMPLEMENTED (mysql→BigQuery reference; other routes follow-up)
- Owner: dpone maintainers
- Issue: follow-up to mysql→bigquery Batch ETL; raise bar for all integrations
- Target release: next patch after merge
Last verified: 2026-07-22
Approval: maintainer directed wide types + all load strategies for all
  integrations 2026-07-22

## Executive summary

Batch ETL claims must not rest on a 3–4 column smoke table and two strategies.
Every source→sink integration that claims Batch ETL (or stronger) must prove:

1. a **wide** fixture covering the pair type profile (round-tripable types);
2. **every** load strategy declared supported for that sink (or documented N/A);
3. typed / semantic assertions beyond row counts.

This increment upgrades mysql→BigQuery to that bar and publishes the standard
for all future route integrations.

## Scope

### In scope

- Shared IT helpers under `tests/integration/mysql/`
- Wide MySQL fixture from `mysql_to_bigquery_analytics_v1` round-trip core
- Live IT for all strategies in `BigQuerySinkCompositionFactory`
- Docs: testing standard + Cursor always-on rule
- Contract-required types (ENUM/SET/spatial/BIGINT UNSIGNED/YEAR/overflow
  DECIMAL) stay hermetic-only unless `schema_contract` is under test

### Non-goals

- Expanding every existing mysql→postgres/mssql/clickhouse/kafka IT in this PR
  (standard applies; migrations follow per route)
- CDC / binlog
- Committing credentials

## Strategies (mysql→BigQuery)

| Strategy | Live proof |
|---|---|
| full_refresh | PASS — wide CSV load + typed spot checks |
| incremental_append | PASS — watermark append growth |
| incremental_merge | PASS — idle watermark + new key upsert |
| replace | PASS — predicate window replace; outside untouched |
| partition_replace | PASS — one partition replaced; sibling intact |
| snapshot_diff | PASS — insert/update/hard_delete |
| scd2 | PASS — file CSV enriched with row_hash / is_current; version expire |
| backfill | PASS — replace inner_mode window |

## Public contract

Matrix/guide may claim Batch ETL only when wide+strategy live evidence exists
for that route (or an explicit exception with reason). Soften claims when only
narrow smoke exists.

## Rollout

1. Land mysql→BQ wide+strategy IT + standard docs/rule.
2. Migrate other mysql→* and remaining routes in follow-up PRs to the same bar.
