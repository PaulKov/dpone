# Feature design: MSSQL → sink live wide-type + full-strategy certification v1

- Status: APPROVED
- Owner: dpone maintainers
- Issue: follow-up to postgres/mysql wide-cert stacks; apply the same bar to
  mssql→sink Batch ETL claims
- Target release: next patch after sequential merges
Last verified: 2026-07-24
Approval: maintainer APPROVED 2026-07-24 — Phase A sink order CH → MSSQL → PG →
  Kafka → BQ (asset-first); then B → C → D

## Executive summary

Matrix and guides claim **Batch ETL supported** (Kafka: Batch/event-log) for
`mssql → {clickhouse, mssql, postgres, kafka, bigquery}`, but live evidence is
uneven: mssql→clickhouse has deep type/wide harness without a full-strategy
vendor-live suite; the other four routes are boilerplate/smoke-level.

This program applies
[`docs/testing/route-live-wide-certification.md`](testing/route-live-wide-certification.md)
to every mssql→sink route that claims Batch ETL (or stronger), in the same
stacked PR pattern as postgres/mysql, then continues to Phase B (close N/A),
C (self-service UX), and D (extend gated live CI with mssql source legs).

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
|---|---|---|---|
| Data engineer | Trust matrix “Batch ETL” for MSSQL sources | Claims outrun evidence | Guide lists strategies + vendor-live command that PASSed |
| Platform engineer | Same compose/IT gates as postgres/mysql suites | CH-only deep IT; other sinks thin | Shared `mssql_live_support` + per-sink helper split |
| Maintainer | One cert bar across sources | mysql/postgres done; mssql lagging | Matrix note “wide vendor-live for …” only after PASS |

Journey: discover matrix → open route guide → run documented vendor-live IT →
promote manifest. No credential commit; SKIP ≠ PASS.

## Scope

### In scope (Phase A)

- Wide fixtures from existing pair profiles where present; **new** profiles for
  mssql→{postgres,kafka,bigquery} as needed; extend mssql→mssql identity usage
- Vendor-live IT for every **supported** sink factory strategy (or documented
  N/A matching postgres/mysql precedent)
- Typed/semantic asserts beyond `COUNT(*)`
- Guide + type-mapping matrix + CHANGELOG updates **after** live PASS
- Stacked PRs: **ClickHouse → MSSQL → Postgres → Kafka → BigQuery**

### Non-goals (Phase A)

- CDC / change-tracking as the primary extract mode for these suites
  (watermark/full extract only unless a strategy requires otherwise)
- Softening SKIP≠PASS for intentional CI/live runs
- Landing-example UX polish beyond claim honesty (Phase C / global plan D)
- Extending `route-live-wide-certification.yml` with mssql source legs (Phase D)

### Assumptions and constraints

- Reuse `StrategyMetadataEnricher` before `sink.load` (production path)
- Docker compose services for mssql/postgres/clickhouse/kafka; BQ via existing
  `BIGQUERY_DWH_*` maintainer SA (never commit)
- Soften matrix/guide claims if a route cannot meet the bar in this program
- mssql→clickhouse reuses existing wide-type / type-matrix assets; Phase A there
  is primarily **strategy breadth + vendor-live packaging**
- ClickHouse `snapshot_diff` / `scd2` are production-capable after postgres
  Phase B2 — include them in mssql→CH live (not N/A)

## Approaches considered

| Approach | Pros | Cons | Verdict |
|---|---|---|---|
| A1. Mirror postgres (BQ first) | Creds early | Delays CH asset leverage; BQ greenfield first | Reject for order |
| **A2. Asset-first CH → MSSQL → PG → Kafka → BQ** | Fastest first PASS; CH harness exists | BQ claim gap stays longest | **Adopt** |
| A3. One mega-PR all five sinks | One review | Unmergeable; hides per-route failures | Reject |

## Public contract

### CLI / Python API / Manifest

No new public commands. Additive pair profile / matrix registrations only.

### Artifacts and evidence

Manual vendor-live PASS under
`tests/integration/mssql/test_mssql_to_{sink}_vendor_live_integration.py`
(+ helpers). SKIP/UNVERIFIED when env missing — never PASS.

Release-review tooling may additionally emit closed, self-hashed local-Docker
receipts and an aggregate type/transport campaign. These are internal operator
tools rather than installed `dpone` CLI commands. Their trust contract is
strictly weaker than `vendor_live`: they must say production `UNVERIFIED`, and
generic route/release evidence readers must reject them as production
certification. Additive receipt schemas do not change the existing vendor-live
result schemas or source/sink capability claims.

The wide release campaign first freezes a create-once local authority plan. It
derives the expected ClickHouse type/nullability digests from the exact
202-column dbt relation and independently binds non-secret MSSQL, ClickHouse,
and S3 coordinates. Route producers self-verify their receipt immediately;
the campaign then deep-verifies exactly one required-native, one
Python-reference, and one Parquet/S3 slot. Authority and campaign replay read
and authenticate existing bytes; they never overwrite them.

### Compatibility

Narrow smoke/native-transfer ITs remain. Guides may keep Batch ETL wording only
when wide evidence exists; otherwise soften or footnote until PASS.

## Detailed algorithm (per sink)

```text
for sink in [clickhouse, mssql, postgres, kafka, bigquery]:
  1. Create branch/worktree stacked on previous tip (or master for CH)
  2. Map pair profile → wide DDL + seed (contract types hermetic N/A)
  3. Build LoadConfig factory for each supported strategy
  4. Live IT: extract → StrategyMetadataEnricher → sink.load → typed asserts
  5. Document sink N/A with code-backed reason
  6. Update guide/matrix/CHANGELOG only after PASS
  7. Open stacked PR; merge after green + review path
```

### Strategy matrix (expected)

| Sink | Live strategies | Documented N/A |
|---|---|---|
| ClickHouse | FR, append, merge, replace, partition_replace, snapshot_diff, scd2, backfill | — |
| MSSQL | same 8 | types that remain fail-closed hermetically |
| Postgres | same 8 | — |
| Kafka | FR, append, merge, replace, snapshot_diff, backfill(inner merge) | partition_replace, scd2 |
| BigQuery | FR, append, merge, replace, partition_replace, snapshot_diff, scd2, backfill | — |

## Components

| Component | Change |
|---|---|
| `tests/integration/mssql/*` | New live_support + per-sink fixtures/configs/assertions/tests |
| `dpone.type_system.source_sink.mssql_*` (as needed) | Pair profiles for PG/Kafka/BQ |
| Guides / matrix / CHANGELOG | Claim honesty after PASS |
| Shared runtime | Only if a defect blocks live (fix-forward) |

## Test and certification plan

| Layer | Evidence |
|---|---|
| Hermetic | New/updated type-matrix cases for new profiles |
| Live | Vendor-live suites gated `DPONE_RUN_INTEGRATION(_LIVE)=1` |
| Docs | `route-live-wide-certification.md` peer note lists mssql routes as they PASS |

## Rollout (program)

| Phase | Work | Gate |
|---|---|---|
| **A** | mssql→sinks wide cert (this spec) | Each route live PASS + PR merge |
| **B** | Close remaining N/A with separate APPROVED specs | Spec + live PASS |
| **C** | Self-service golden path on mssql guides | Docs + contract tests (`docs/feature-design-certified-route-recipe-golden-path-v1.md`) |
| **D** | Extend `route-live-wide-certification.yml` with mssql source legs | Workflow + SKIP≠PASS |

## Market comparison (certification bar)

| System | Relevant? | Note |
|---|---|---|
| dlt | Yes | Typed load tests; not a public source×sink×strategy cert matrix |
| Airbyte | Yes | Connector certification suites; strategy surface differs |
| Fivetran / Informatica / SSIS / Pentaho | N/A | Closed suites |
| Beam | N/A | Pipeline SDK, not ETL route matrix |

Superiority claim (scenario): *operator can reproduce Batch ETL evidence for an
mssql→sink route via one documented pytest command covering wide types and all
supported strategies.* Metric: strategies PASS / supported; target 100% or
explicit N/A; procedure: vendor-live IT; artifact: pytest output + guide
command block.

## Risks

- mssql→postgres/kafka/bigquery pair profiles are greenfield — expect
  DataTypeMapper/staging gaps; fix in-route, no silent nvarchar fallback.
- Softening overstated matrix claims may look like a “regression” in docs until
  each stacked PR lands PASS.
- Stacked merges need `--onto` discipline after each master landing.

## Approval checklist

- [x] Maintainer marks this spec **APPROVED**
- [x] Confirm Phase A sink order: CH → MSSQL → PG → Kafka → BQ
- [x] Confirm Phase B/C/D remain sequenced after A
- [x] Confirm BQ SA secrets already configured for Phase A tail

## Implementation ownership (after APPROVED)

One integrator per stacked PR; owned paths:
`tests/integration/mssql/**`, route guide under edit, new pair profile modules,
`CHANGELOG.md` / matrix / wide-cert testing doc (integrator only).
