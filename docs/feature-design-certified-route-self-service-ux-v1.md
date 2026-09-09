# Feature design: certified route self-service UX v1

- Status: APPROVED
- Owner: dpone maintainers
- Issue: Phase C of postgres route live wide certification
- Target release: next patch after stack merges
Last verified: 2026-07-23
Approval: maintainer APPROVED 2026-07-23 — approach A (golden path + landing
  parity + gallery badges + plan type-matrix hint + contract tests)

## Executive summary

After Phase A/B made matrix claims honest for postgres/mysql→sink wide
certification, operators still hit placeholder runbooks (`plan <manifest>`),
missing MySQL gallery rows, incomplete postgres landing batches, and a
`dpone plan` type-matrix hint that points at a generic `schema explain` JSON
path. Phase C closes the copy-paste self-service CJM without new CLI commands.

## Personas and customer journey

| Persona | Goal | Pain | Success |
|---|---|---|---|
| Data engineer | Land a certified route from docs | Placeholder commands, missing examples | Golden path runs against checked-in example |
| Maintainer | Keep docs/examples honest | Gallery/matrix drift | Contract tests fail on missing landing/CJM |

Journey: discover gallery/matrix → open route guide → golden path
(`doctor → plan → type-matrix → run`) → optional landing batch for GitOps →
promote. Vendor-live IT remains maintainer evidence (SKIP ≠ PASS).

## Scope

### In scope

- Golden-path blocks in all 10 postgres/mysql→sink guides
- Missing `landing_postgres_to_{clickhouse,postgres}.batch.yaml` + guide links
- Examples gallery: MySQL section + wide vendor-live evidence column
- `managed_planning` `explain_command` → `dpone schema type-matrix --source/--sink`
- Contract tests for golden path + landing parity
- CHANGELOG / matrix cross-links

### Non-goals

- New CLI commands or schema fields
- Airflow recipes / first-local-pipeline rewrite
- Phase D gated live CI

## Public contract

No new commands. Additive plan payload string change:
`type_matrix.explain_command` becomes the type-matrix CLI (still documented as
the operator follow-up from `dpone plan`).

## Success criteria

1. Every postgres/mysql→sink guide contains concrete
   `doctor` / `plan examples/source-sink/{stem}.yaml` /
   `schema type-matrix --source … --sink …` / `run examples/source-sink/{stem}.yaml`.
2. Landing batch files exist for all five postgres and five mysql sinks.
3. Examples gallery lists MySQL wide-cert routes with evidence badge.
4. Contract tests enforce (1)–(2); hermetic plan tests accept type-matrix hint.
