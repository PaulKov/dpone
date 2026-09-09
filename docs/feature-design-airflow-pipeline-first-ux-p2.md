# Feature design: Airflow self-service Source→Sink framing (P2)

- Status: IMPLEMENTED
- Evidence: docs language contracts in `tests/test_docs_language_contracts.py`
  (`test_airflow_ux_p2_route_framing_contracts`); merged
  [#353](https://github.com/PaulKov/dpone/pull/353) → `f6cae1e0`
- Owner: data architecture / integrator
- Issue: follow-up to UX P0 ([#344](https://github.com/PaulKov/dpone/pull/344)) and P1 ([#345](https://github.com/PaulKov/dpone/pull/345))
- Target release: docs-only patch after `0.73.2`
- Last verified: 2026-07-19

## Maintainer decisions (locked 2026-07-18)

1. **Scope A only** — documentation and language-contract tests.
2. No CLI label changes (`recipe list` humanization deferred to optional P2.1).
3. No renames of recipe refs, `workload_id`, schemas, URIs, or golden five commands.
4. Acceptance = docs language contracts + check-docs + mkdocs strict (no user study).

## Executive summary

Batch docs and ops already speak **source → sink** and **route**. The Airflow
beginner path (after P0/P1) still centers **pipeline / recipe / workload** without
an explicit bridge. P2 wires that bridge in docs so a beginner chooses an
MSSQL→ClickHouse **route**, instantiates it with a built-in **recipe** slug, and
edits a **pipeline** — without changing machine contracts.

**P2 success (contract):** glossary `## route` / `## recipe` / `## connection_ref`;
First DAG route callout before golden commands; three-row recipe↔route table;
stable bridge marker on all BRIDGE_PAGES; five golden bash commands unchanged;
freeze YAML/tests untouched.

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
|---|---|---|---|
| New data engineer | First Airflow pipeline | Thinks “recipe slug” ≠ “my source→sink” | Sees route → recipe → pipeline before commands |
| Analytics engineer | Find supported pair | Matrix vs First DAG feel separate products | Index/hub point route → Airflow path |
| DataOps | Keep freeze stable | Fear of renames | Empty freeze diff |
| Maintainer | Close UX train | P2 deferred | Scope A shipped |

### Beginner CJM (unchanged steps; clearer nouns)

1. Discover: index / quickstart / matrix → “pick your route”.
2. Airflow path: First DAG — “this recipe scaffolds that route”.
3. Same five commands (byte-identical golden fence).
4. Edit `pipeline.yaml`; commit per P0 What-to-commit.
5. check → preview → sample.
6. Advanced still gated (P0 CJM).

## Scope

### In scope

- Extend `docs/getting-started/airflow-pipeline-glossary.md`:
  - **route** = source + sink + load strategy (ops `RouteKey` mental model)
  - **recipe** = platform/built-in template that scaffolds a route into a pipeline
  - **connection_ref** = logical endpoint alias (not the route itself)
  - Keep existing pipeline / workload / DAG / pack entries
- First Airflow DAG: additive prose + built-in recipe↔route table; **do not** edit
  the golden bash fence body.
- Hub (`airflow-self-service.md`): one “Start from your route” bridge under
  beginner section.
- `docs/index.md` fast paths: Airflow row mentions route framing.
- `docs/getting-started/quickstart.md`: one sentence tying manifest source/sink
  to the same route vocabulary.
- `docs/getting-started/examples-gallery.md`: one sentence linking Flow column to
  Airflow First DAG / recipes.
- `docs/airflow-recipes.md`: short “Built-in recipes map to routes” table + link
  to matrix / First DAG.
- `docs/source-sink-matrix.md`: intro sentence pointing Airflow beginners to
  First DAG after picking a matrix row.
- Language contracts in `tests/test_docs_language_contracts.py`.
- `CHANGELOG.md` Unreleased docs note; backlog checkbox.

### Non-goals

- CLI text / `dpone recipe list` human labels (P2.1).
- Renaming built-in recipe refs or strategies.
- Renaming `workload_id`, `workloads:`, pack URIs, schemas.
- Changing exit codes, JSON, or freeze YAML.
- P3 industrial GO evidence / user study.
- Editing golden five-command bash fence on First Airflow DAG.

### Assumptions and constraints

- Built-in refs (immutable):
  - `mssql-to-clickhouse-incremental` → MSSQL → ClickHouse / `incremental_merge`
  - `postgres-to-clickhouse-incremental` → Postgres → ClickHouse / `incremental_merge`
  - `postgres-to-clickhouse-full-refresh` → Postgres → ClickHouse / `full_refresh`
- English-only public docs.
- Forbidden: `docs/airflow-self-service-public-contracts-v1.yaml`,
  `tests/test_airflow_v1_public_contract_freeze.py`, `src/**`, `packages/**`,
  `docs/schemas/**` field renames.

## Public contract

### CLI / Python / Manifest / Artifacts

No change.

### Compatibility and migration

Docs-only. Rollback = revert MR. Empty freeze diff required.

## Authoritative content contracts

### Glossary additions (must exist as `##` headings)

| Heading | Must convey |
|---|---|
| `## route` | Day-1 intent noun: source + sink + load strategy. Not a file you edit. Ops/certification may later attest the same route identity — preview/sample alone does **not** certify a route. |
| `## recipe` | Template/ref that scaffolds a pipeline for a route; built-in slug encodes source-to-sink. |
| `## connection_ref` | Logical connection alias for endpoints; resolved at workload start — not the route itself. |

Cross-links: First DAG, source-sink matrix. Keep `→` in new glossary/First DAG prose.

### Stable bridge marker (required substring)

Exact English marker (must appear once on each bridge page):

```text
Start from your route (source → sink)
```

**BRIDGE_PAGES (enumerated):**

| Page | Placement |
|---|---|
| `docs/airflow-self-service.md` | Beginner section / Audience area |
| `docs/index.md` | Fast paths table or adjacent prose |
| `docs/getting-started/quickstart.md` | After first successful run / next steps |
| `docs/getting-started/examples-gallery.md` | Intro before Flow table |
| `docs/airflow-recipes.md` | Near built-in recipe discussion |
| `docs/source-sink-matrix.md` | Intro paragraph |

### First Airflow DAG (additive)

**Must appear before** `## Golden path commands`:

> You are choosing an **MSSQL → ClickHouse** route (incremental merge). The
> built-in **recipe** `mssql-to-clickhouse-incremental` scaffolds that route into
> an editable **pipeline**. Preview and hermetic checks do not certify the route.

Authoritative built-in table (exact rows; may live in §2 or recipes page — **must**
appear on First Airflow DAG; recipes page may duplicate with the same refs):

| Recipe ref | Route (source → sink) | Strategy |
|---|---|---|
| `mssql-to-clickhouse-incremental` | MSSQL → ClickHouse | incremental merge (`incremental_merge`) |
| `mssql-to-clickhouse-full-refresh` | MSSQL → ClickHouse | full refresh (`full_refresh`) |
| `postgres-to-clickhouse-incremental` | PostgreSQL → ClickHouse | incremental merge (`incremental_merge`) |
| `postgres-to-clickhouse-full-refresh` | PostgreSQL → ClickHouse | full refresh (`full_refresh`) |

Canonical strategy spellings: `docs/load-strategies.md` / matrix. Do not invent names.

### Language-contract asserts

1. Glossary contains headings `## route`, `## recipe`, and `## connection_ref`.
2. First Airflow DAG: `index("MSSQL → ClickHouse") < index("## Golden path commands")`
   (Unicode arrow `→` required in that pre-golden callout).
3. First Airflow DAG markdown body contains all four built-in recipe refs
   `mssql-to-clickhouse-incremental`, `postgres-to-clickhouse-incremental`,
   `postgres-to-clickhouse-full-refresh`, and
   `mssql-to-clickhouse-full-refresh`.
4. Golden five commands remain exact (existing test unchanged).
5. Each BRIDGE_PAGES path contains substring
   `Start from your route (source → sink)`.
6. Freeze YAML empty diff in gates (`git diff --exit-code`
   `docs/airflow-self-service-public-contracts-v1.yaml`).

## Detailed algorithm

1. Inventory beginner entry points (done in exploration).
2. Patch glossary nouns.
3. Patch First DAG prose + recipe↔route table.
4. One-bridge sentences on hub, index, quickstart, examples-gallery, recipes,
   matrix intro.
5. Extend language contracts; run docs gates; prove freeze empty.

### Pseudocode

```text
assert_forbidden_untouched(freeze_yaml, freeze_tests)
patch_glossary(headings=[route, recipe, connection_ref])
patch_first_dag(callout_before_golden, recipe_route_table)  # golden fence untouched
for page in BRIDGE_PAGES:
    ensure_substring(page, "Start from your route (source → sink)")
extend_language_contracts(asserts=[1..6])
run(check_docs, language_tests, mkdocs_strict)
git_diff_exit_code(freeze_yaml)
```

## Architecture

Docs composition only. No new runtime components. Reuses ops route concept as
documentation vocabulary; does not expose `RouteKey` Python API to beginners.

### Alternatives

| Option | Decision |
|---|---|
| Scope A docs-only | **Adopt** (locked) |
| Scope B recipe list labels | Defer P2.1 |
| Rename recipe slugs to prettier IDs | Reject (freeze + catalog) |

### ADR

Not required.

### Quality-budget

Docs-only; keep glossary under ~120 lines after additions.

## Market comparison

Primary sources 2026-07-18.

| System | Relevant | Observed | Adopt/reject | Source |
|---|---|---|---|---|
| Airbyte | Connection as product noun | One clear day-1 object | Adopt “route” as day-1 intent noun | Airbyte docs / UX handbook |
| Fivetran | Connector pair framing | Source→destination | Adopt framing; N/A for OSS CLI | product docs |
| Cosmos | dbt select, not ELT pairs | N/A for source→sink | N/A | — |
| dlt | Source/resource naming | Resource-first | Reject as Airflow day-1 model | dlt docs |
| Informatica / Pentaho / SSIS / Beam / gusty | N/A | — | N/A | not Airflow self-service CJM |

## Measurable differentiation

```yaml
axis: beginner route framing in Airflow docs
scenario: CI language contracts after P2 docs land
baseline: glossary lacks route; First DAG leads with recipe without route table
metric: boolean asserts 1–6 (glossary headings, pre-golden route callout, three recipe refs, golden commands, bridge marker on BRIDGE_PAGES, freeze untouched)
target: all PASS
procedure: uv run pytest tests/test_docs_language_contracts.py -q && uv run dpone docs check-docs && uv run mkdocs build --strict && git diff --exit-code docs/airflow-self-service-public-contracts-v1.yaml
artifact: pytest + docs gate logs on the docs MR
limitations: does not measure human comprehension
```

## Security, privacy, and operations

No runtime/secrets change. Do not claim industrial GO.

## Test and certification plan

| Layer | Env | Expected |
|---|---|---|
| Contract | hermetic | P2 language asserts |
| Docs | hermetic | check-docs + mkdocs strict |
| Freeze | hermetic | empty diff |
| Live | — | N/A |

## Documentation plan

| Doc | Change |
|---|---|
| `airflow-pipeline-glossary.md` | route, recipe, connection_ref |
| `first-airflow-dag.md` | callout + table |
| `airflow-self-service.md` | bridge sentence |
| `index.md` | fast-path wording |
| `quickstart.md` | bridge sentence |
| `examples-gallery.md` | bridge sentence |
| `airflow-recipes.md` | built-in→route table |
| `source-sink-matrix.md` | Airflow pointer |
| `CHANGELOG.md` | Unreleased |
| backlog | P2 checkbox |

## Rollout and rollback

APPROVED → docs MR → review → merge → revert if needed.

## Agent execution plan

| Role | Owned | Forbidden |
|---|---|---|
| Docs writer | listed docs pages + glossary | freeze, src |
| Integrator | `test_docs_language_contracts.py`, CHANGELOG, backlog, quality-metrics if required | freeze tests |

## Approval checklist

- [x] Scope A locked by maintainer.
- [x] Exact glossary headings and recipe↔route table (= RECIPE_DEFAULTS).
- [x] Bridge marker + BRIDGE_PAGES enumerated and asserted.
- [x] Golden commands / freeze non-goals explicit; route ≠ certified on day one.
- [x] Metric = docs contracts only.
- [x] Maintainer status → `APPROVED` (owner GO after shape A `0.73.2` release).
- [x] Docs MR merged ([#353](https://github.com/PaulKov/dpone/pull/353)); status →
  `IMPLEMENTED`.
