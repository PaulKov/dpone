# Feature design: certified route recipe → golden path v1

- Status: APPROVED
- Owner: dpone maintainers
- Issue: Outer plan item D / mssql Phase C self-service parity
- Target release: next patch on `feat/mssql-route-wide-cert` stack
Last verified: 2026-07-24
Approval: maintainer APPROVED 2026-07-24 (outer plan A–D proceed)

## Executive summary

Postgres/mysql Phase C already shipped copy-paste
`doctor → plan → schema type-matrix → run` blocks, landing-batch parity, and
gallery evidence badges. After mssql→sink wide vendor-live certification (Phase
A), operators still hit placeholder runbooks (`plan <manifest>`), missing
`landing_mssql_to_{postgres,bigquery}` batches, gallery rows stuck on
`Batch ETL`, and no contract that recipe discovery/`init pipeline --route`
keeps surfacing supported mssql routes when a beginner recipe exists.

This change closes that CJM parity without new CLI commands or Studio OpenAPI
changes.

## Personas and customer journey

| Persona | Goal | Pain | Success |
|---|---|---|---|
| Data engineer | Land a certified mssql→sink route from docs | Placeholder runbooks, missing landing examples | Golden path runs against checked-in example |
| Airflow beginner | Scaffold via recipe/route | Unclear whether mssql routes appear | `recipe list --source mssql` lists supported sinks; `--route` scaffolds when a beginner recipe exists |
| Maintainer | Keep docs/examples honest | Gallery/landing/CJM drift | Contract tests fail on missing golden path / landing / recipe surface |

Journey: discover gallery/matrix → open mssql→sink guide → golden path
(`doctor → plan → type-matrix → run`) → optional landing batch for GitOps →
optional `dpone recipe list` / `init pipeline --route` when a beginner recipe
exists → promote. Vendor-live IT remains maintainer evidence (SKIP ≠ PASS).

## Scope

### In scope

- Golden-path blocks in all five mssql→sink guides matching postgres/mysql CJM
- Missing `examples/batch/landing_mssql_to_{postgres,bigquery}.batch.yaml`
  (complete five-sink landing set with existing clickhouse/mssql/kafka batches)
- Examples gallery: **MSSQL wide-certified flows** section with `wide vendor-live`
  evidence column; promote mssql rows in other tables to the same badge
- Contract tests extending `test_docs_language_contracts.py` patterns for
  mssql golden path + landing + gallery
- Hermetic CLI contract: `dpone recipe list --source mssql` surfaces supported
  routes for all five sinks; `init pipeline --route mssql:clickhouse:incremental_merge`
  scaffolds when capability exposes a beginner recipe; supported routes without
  a beginner recipe keep fail-closed `DPONE_ROUTE_NOT_SCAFFOLDABLE`
- CHANGELOG Unreleased note + pointer from wide-cert testing doc

### Non-goals

- New CLI commands or public recipe/schema fields
- New built-in beginner recipes for mssql→{postgres,mssql,kafka,bigquery}
  (scaffold only when an existing recipe already exists)
- Studio OpenAPI changes unless a discovery bug blocks recipe list
- Changing certification `evidence_status` vocabulary in recipe JSON
- Phase D gated live CI workflow expansion

### Assumptions and constraints

- Wide vendor-live evidence for mssql→{clickhouse,mssql,postgres,kafka,bigquery}
  already lands on this branch (Phase A).
- Only `mssql-to-clickhouse-incremental` is a built-in beginner recipe today;
  other mssql sinks remain discoverable as supported routes.

## Public contract

No new commands. Additive documentation/examples/contract tests only.

Existing public surfaces used as-is:

- `dpone recipe list [--source mssql] [--sink …] [--strategy …]`
- `dpone init pipeline <name> --route <source:sink:strategy>`
- Checked-in `examples/source-sink/mssql-to-*.yaml` and landing batches

## Detailed algorithm

1. For each mssql→{bigquery,postgres,mssql,clickhouse,kafka} guide, insert a
   **Self-service golden path** section before **Runbook** with concrete
   `doctor`, install extras, `plan examples/source-sink/{stem}.yaml`,
   `schema type-matrix --source mssql --sink {sink}`, and
   `run examples/source-sink/{stem}.yaml`.
2. Replace placeholder `plan <manifest>` runbook lines with the concrete example
   path (contract: no `plan <manifest>` remains in the guide).
3. Add missing vault/GitOps landing batches for postgres and bigquery sinks;
   link each guide to `examples/batch/landing_mssql_to_{sink}.batch.yaml`
   (bigquery uses the `bigquery` stem, matching mysql).
4. Add gallery section and badge updates.
5. Extend `_WIDE_CERT_ROUTES` and gallery contract tests; add recipe/route
   discovery contracts for mssql five-sink surface + scaffoldable clickhouse route.

### Pseudocode

```text
for sink in {bigquery, postgres, mssql, clickhouse, kafka}:
  guide = docs/source-sink/mssql-to-{sink}.md
  ensure "## Self-service golden path" with doctor/plan/type-matrix/run
  ensure landing examples/batch/landing_mssql_to_{sink|bigquery}.batch.yaml
  ensure not "plan <manifest>" in guide

assert gallery has "## MSSQL wide-certified flows" + five Flow rows
assert recipe list --source mssql includes all five sinks as supported
assert init --route mssql:clickhouse:incremental_merge scaffolds
assert init --route mssql:postgres:incremental_merge → DPONE_ROUTE_NOT_SCAFFOLDABLE
```

## Success criteria

1. Every mssql→sink guide contains the same golden-path CJM as postgres/mysql.
2. Landing batches exist for all five mssql sinks.
3. Examples gallery lists MSSQL wide-cert routes with evidence column.
4. Recipe list surfaces supported mssql routes for all five sinks; init `--route`
   scaffolds when a beginner recipe exists and fails closed otherwise.
5. Contract tests enforce (1)–(4).

## Test and certification plan

| Layer | Evidence |
|---|---|
| Docs contracts | `tests/test_docs_language_contracts.py` wide-cert + gallery |
| CLI hermetic | `tests/test_self_service_capability_cli.py` mssql recipe/route surface |
| Manifest load | Optional extend `tests/test_mssql_manifest_examples.py` for new landings |
| Live | N/A (already covered by Phase A vendor-live; SKIP ≠ PASS) |

## Documentation / CJM impact

- Route guides, examples gallery, wide-cert testing doc cross-link
- CHANGELOG Unreleased
- No First DAG rewrite; built-in recipe table unchanged

## Market comparison

| System | Relevant? | Note |
|---|---|---|
| dlt | Yes | Docs point at verified source/dest extras; no recipe→route gallery bar |
| Airbyte | Yes | Connector docs + certified catalog; strategy/recipe surface differs |
| Fivetran / Informatica / SSIS / Pentaho | N/A | Closed guided setup |
| Beam / gusty / Cosmos | N/A | Orchestration SDK, not ETL route golden-path docs |

Superiority claim (scenario): *operator can copy a four-command golden path from
an mssql→sink guide that matches a checked-in example and a landing batch, while
`recipe list` still exposes the route for Airflow scaffolding when a beginner
recipe exists.* Metric: docs/contract tests PASS; target 100% of five sinks;
procedure: pytest contracts; artifact: pytest output.

## Risks

- mssql→clickhouse guide is large; golden path must sit immediately before the
  primary **Runbook** so operators find it.
- Adding beginner recipes for other mssql sinks is out of scope; do not weaken
  `DPONE_ROUTE_NOT_SCAFFOLDABLE`.

## Approval checklist

- [x] Maintainer marks this spec **APPROVED**
- [x] Confirm no new CLI / Studio OpenAPI unless discovery bug
- [x] Confirm landing stems: `landing_mssql_to_bigquery` (not `_bq`)

## Implementation ownership

One integrator on `feat/mssql-route-wide-cert`:

- `docs/feature-design-certified-route-recipe-golden-path-v1.md`
- `docs/source-sink/mssql-to-*.md`
- `docs/getting-started/examples-gallery.md`
- `docs/testing/route-live-wide-certification.md` (cross-link only)
- `examples/batch/landing_mssql_to_{postgres,bigquery}.batch.yaml`
- `tests/test_docs_language_contracts.py`
- `tests/test_self_service_capability_cli.py` (and optionally
  `tests/test_mssql_manifest_examples.py`)
- `CHANGELOG.md`
