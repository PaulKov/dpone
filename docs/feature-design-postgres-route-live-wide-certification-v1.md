# Feature design: Postgres → sink live wide-type + full-strategy certification v1

- Status: APPROVED
- Owner: dpone maintainers
- Issue: follow-up to mysql wide-cert stack (#420–#426); apply the same bar to
  postgres→sink Batch ETL claims
- Target release: next patch after sequential merges
Last verified: 2026-07-23
Approval: maintainer APPROVED 2026-07-23 — Phase A sink order BQ → PG → MSSQL →
  CH → Kafka; then B → C → D

## Executive summary

Matrix and guides claim **Batch ETL supported** (Kafka: Batch/event-log) for
`postgres → {bigquery, postgres, mssql, clickhouse, kafka}`, but live evidence
is narrow smoke, backfill-only, or Int64-grid FR — not the wide-type ×
full-strategy bar already proven for mysql→sinks.

This program applies
[`docs/testing/route-live-wide-certification.md`](testing/route-live-wide-certification.md)
to every postgres→sink route that claims Batch ETL (or stronger), in the same
stacked PR pattern as mysql, then continues to Phase B (close N/A), C
(self-service UX), and D (gated live CI).

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
|---|---|---|---|
| Data engineer | Trust matrix “Batch ETL” for postgres sources | Claims outrun evidence | Guide lists strategies + vendor-live command that PASSed |
| Platform engineer | Same compose/IT gates as mysql suites | Ad-hoc refresh/backfill ITs only | Shared `postgres_live_support` + per-sink helper split |
| Maintainer | One cert bar across sources | mysql done; postgres lagging | Matrix note “wide vendor-live for …” only after PASS |

Journey: discover matrix → open route guide → run documented vendor-live IT →
promote manifest. No credential commit; SKIP ≠ PASS.

## Scope

### In scope (Phase A)

- Wide fixtures from existing pair profiles where present; **new** profile for
  postgres→BigQuery (mirror `mysql_to_bigquery_analytics_v1` shape)
- Vendor-live IT for every **supported** sink factory strategy (or documented
  N/A matching mysql/CH/Kafka precedent)
- Typed/semantic asserts beyond `COUNT(*)`
- Guide + type-mapping matrix + CHANGELOG updates **after** live PASS
- Stacked PRs: BQ → Postgres → MSSQL → ClickHouse → Kafka

### Non-goals (Phase A)

- CDC / xmin as the primary extract mode for these suites (watermark/
  full extract only unless a strategy requires otherwise)
- Implementing ClickHouse `snapshot_diff`/`scd2` or Kafka `partition_replace`/
  `scd2` (Phase B may productize; Phase A documents N/A)
- MSSQL native-binary BCP for `bytea`/`varbinary` (Phase B; character BCP N/A)
- CI live jobs (Phase D)
- Landing-example UX polish beyond claim honesty (Phase C)

### Assumptions and constraints

- Reuse `StrategyMetadataEnricher` before `sink.load` (production path)
- Docker compose services for postgres/mssql/clickhouse/kafka; BQ via existing
  `BIGQUERY_DWH_*` maintainer SA (never commit)
- Soften matrix/guide claims if a route cannot meet the bar in this program

## Approaches considered

| Approach | Pros | Cons | Verdict |
|---|---|---|---|
| **A1. Mirror mysql sink order (BQ first)** | Unblocks vendor creds early; same stack discipline | BQ needs profile creation first | **Adopt** |
| A2. Docker-first (PG→PG then MSSQL/CH/Kafka, BQ last) | Faster green without GCP | Leaves largest claim gap open longest | Reject for Phase A order |
| A3. One mega-PR all five sinks | One review | Unmergeable; hides per-route failures | Reject |

## Public contract

### CLI / Python API / Manifest

No new public commands or schema fields. Pair profile registration for
postgres→BigQuery is additive (matrix/docs/type-matrix hermetic).

### Artifacts and evidence

Manual vendor-live PASS under
`tests/integration/postgres/test_postgres_to_{sink}_vendor_live_integration.py`
(+ helpers). SKIP/UNVERIFIED when env missing — never PASS.

### Compatibility

Narrow smoke ITs remain. Guides may keep Batch ETL wording only when wide
evidence exists; otherwise soften or footnote until PASS.

## Detailed algorithm (per sink)

```text
for sink in [bigquery, postgres, mssql, clickhouse, kafka]:
  1. Create branch/worktree stacked on previous tip (or master for BQ)
  2. Map pair profile → wide DDL + seed (ENUM/spatial/contract types hermetic N/A)
  3. Build LoadConfig factory for each supported strategy
  4. Live IT: extract → StrategyMetadataEnricher → sink.load → typed asserts
  5. Document sink N/A with code-backed reason
  6. Update guide/matrix/CHANGELOG only after PASS
  7. Open stacked PR; merge after green + review path
```

### Strategy matrix (expected)

| Sink | Live strategies | Documented N/A |
|---|---|---|
| BigQuery | FR, append, merge, replace, partition_replace, snapshot_diff, scd2, backfill | — |
| Postgres | same 8 | — |
| MSSQL | same 8 | `bytea`/varbinary on character BCP (Phase B native binary) |
| ClickHouse | FR, append, merge, replace, partition_replace, backfill | snapshot_diff, scd2 |
| Kafka | FR, append, merge, replace, snapshot_diff, backfill(inner merge) | partition_replace, scd2 |

## Components

| Component | Change |
|---|---|
| `tests/integration/postgres/*` | New live_support + per-sink fixtures/configs/assertions/tests |
| `dpone.type_system.source_sink.postgres_bigquery` (new) | Pair profile for BQ |
| Guides / matrix / CHANGELOG | Claim honesty after PASS |
| Shared runtime | Only if a defect blocks live (fix-forward; no speculative refactors) |

## Test and certification plan

| Layer | Evidence |
|---|---|
| Hermetic | New/updated type-matrix cases for postgres→BQ profile |
| Live | Vendor-live suites gated `DPONE_RUN_INTEGRATION(_LIVE)=1` |
| Docs | `route-live-wide-certification.md` peer note lists postgres routes as they PASS |

## Rollout (program)

| Phase | Work | Gate |
|---|---|---|
| **A** | postgres→sinks wide cert (this spec) | Each route live PASS + PR merge |
| **B** | Close N/A: MSSQL native binary BCP; productize CH scd2/diff if approved | Separate APPROVED specs |
| **C** | Self-service: landing examples, doctor/plan copy-paste CJM | Docs + example contract tests |
| **D** | Gated live CI (compose mysql/postgres + sinks; BQ optional secret) | Workflow + SKIP≠PASS policy — see `docs/feature-design-route-live-wide-ci-v1.md` / `route-live-wide-certification.yml` |

## Market comparison (certification bar)

| System | Relevant? | Note |
|---|---|---|
| dlt | Yes | Typed load tests exist but not a public source×sink×strategy cert matrix |
| Airbyte | Yes | Connector certification suites; strategy surface differs |
| Fivetran / Informatica / SSIS / Pentaho | N/A | Closed suites; not comparable as open evidence artifacts |
| Beam | N/A | Pipeline SDK, not ETL route matrix |

Superiority claim (scenario): *operator can reproduce Batch ETL evidence for a
postgres→sink route via one documented pytest command covering wide types and
all supported strategies.* Metric: strategies PASS / supported; target 100% or
explicit N/A; procedure: vendor-live IT; artifact: pytest output + guide
command block.

## Risks

- Postgres→BQ profile greenfield may surface DataTypeMapper/staging gaps (same
  class as mysql→BQ) — fix in-route, not silent nvarchar/STRING fallback.
- Stacked squash merges need `--onto` discipline after each master landing.
- Local `master` worktree lock may force API merges again.

## Approval checklist

- [ ] Maintainer marks this spec **APPROVED**
- [ ] Confirm BQ SA path still valid for postgres→BQ live
- [ ] Confirm Phase A sink order: BQ → PG → MSSQL → CH → Kafka
- [ ] Confirm Phase B/C/D remain sequenced after A (no scope creep into A)

## Implementation ownership (after APPROVED)

One integrator per stacked PR; owned paths:
`tests/integration/postgres/**`, route guide under edit, pair profile module for
BQ, `CHANGELOG.md` / matrix / wide-cert testing doc (integrator only).
