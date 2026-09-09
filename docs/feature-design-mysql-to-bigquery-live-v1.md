# Feature design: MySQL → BigQuery vendor-live certification v1

- Status: SUPERSEDED by `docs/feature-design-route-live-wide-certification-v1.md`
- Owner: dpone maintainers
- Issue: phase B follow-up to `docs/feature-design-mysql-to-bigquery-contract-v1.md`
- Target release: next patch after live evidence on this branch
Last verified: 2026-07-22

## Executive summary

Phase A made `mysql → bigquery` contract-ready (CSV wire + type profile) without
GCP. Phase B initially certified narrow `full_refresh` + `incremental_merge`
live evidence, then was expanded to the wide typed fixture and all BigQuery
load strategies (including `scd2`) under
`docs/feature-design-route-live-wide-certification-v1.md`. Treat that document
as the current evidence authority.

## Scope

### In scope

- Vendor-live IT: Docker MySQL + BigQuery sink via
  `BIGQUERY_DWH_PROJECT_ID` + `BIGQUERY_DWH_SERVICE_ACCOUNT_KEY_FILE`
- Strategies: `full_refresh` and `incremental_merge` (unique_key=`id`,
  watermark=`updated_at`)
- Disposable dataset/table under project (default dataset `dpone_it_mysql`)
- Docs/matrix/CHANGELOG update only after live PASS
- Credentials never printed, never committed, never written into artifacts

### Non-goals

- CDC / binlog
- CI secret wiring for GitHub Actions (local/manual evidence first)
- Conformance Lab / named native fast path
- Claiming delete-correctness from watermark alone beyond existing BQ merge policy

## Public contract

Current claim authority: wide vendor-live suite for all BigQuery strategies
(see route-live-wide certification). This v1 scope note is historical.

## Test plan

| Layer | Expected |
|---|---|
| Hermetic | unchanged from phase A |
| Live | new `tests/integration/mysql/test_mysql_to_bigquery_vendor_live_integration.py` |
| Skip | when MySQL host or BQ key env unset → SKIP (not PASS) |

## Rollout

1. Implement live IT; run with maintainer SA + Docker MySQL.
2. On PASS: update matrix/guide/CHANGELOG; mark this design IMPLEMENTED.
3. Fresh review subagents before merge.

## Evidence status

The historical environment-specific execution log is not included in this
source snapshot. Fresh certification requires an explicitly approved project
and newly generated evidence. This document does not certify a new environment;
its live status is **UNVERIFIED**.
