# ClickHouse window boundary refactor implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore the unchanged architecture gates through a behavior-neutral
redistribution of ClickHouse window admission and generation readback.

**Architecture:** `ClickHouseWindowTarget` owns admission timing and publication.
Physical topology/schema support and generation metadata readback live beside
`WindowIO`; read-only aggregate SQL uses a narrow query-reader protocol. The old
admission module is compatibility-only, not an intermediate production hop.

**Tech stack:** Python, existing immutable window contracts and injected metadata
store, pytest, repository architecture producers, Ruff and mypy.

**Spec:** Internal-refactor exception in `AGENTS.md`; maintainer permission on
2026-09-30 to reduce coupling without weakening limits; existing
`docs/feature-design-bounded-streaming-window-v1.md` and ADR 0057 remain normative.
This plan changes neither the guarded-publication feature contract nor its
production activation status.

## Global constraints

### Approved autonomous revision (2026-09-30)

The maintainer explicitly authorized resolving all blockers without further
approval round trips. The original blocked candidate below remains historical
evidence; this revision supersedes its stop/ownership constraints, not the public
contract or quality gates. Native execution remains with the sole integrator.

The revised boundary keeps configuration validation, identity/guard ordering,
publication decisions and recovery in `ClickHouseWindowTarget`. Physical topology
admission and schema fingerprinting move beside `WindowIO` in the staging module;
target and old admission import paths re-export the same functions. The target's
`validate_target` patch point remains the callable used by its methods.

Create `src/dpone/runtime/sinks/clickhouse_window_queries.py` for read-only SQL
aggregate queries. `WindowQueryReader` exposes only `get_records` and
`get_records_iterator`; the existing `WindowConnector` extends it with mutation
and close. `read_window_metrics(reader, *, table_sql, column_sql, predicate_sql,
columns, window_only)` receives validated SQL identifiers and ordered pairs of
raw/quoted column names. It owns aggregate/day-query order, timestamp conversion,
iterator closure and the unchanged result shape; it has no domain/state imports.
`WindowIO.metrics` validates names and delegates. Other I/O methods, receipts,
generation readback, authority and SQL publication stay unchanged.

This is a behavior-neutral internal-refactor correction under AGENTS, not a new
feature or deployment authorization. Existing feature spec and ADR0057 remain
normative. No new CLI, manifest, receipt schema or user migration is introduced.

Additional owned files: the new query module and
`tests/test_clickhouse_window_queries.py`; no other production paths are added.
Read-only explorer, architect, test/certification and docs reviewers assessed
this boundary before implementation. Forecast: clustering 0.18187544, flow214;
all changed modules must meet warning350 as well as hard400 after formatting.
These forecasts are not passing evidence.

- [x] Add RED aggregate-query tests for exact emitted SQL/order, window-only and
  whole-table counts, NULL/naive UTC timestamps, empty day sets and iterator
  failure/closure. Add successful target override continuation characterization.
- [x] Move the two functions unchanged, retain target/old-path re-exports, and
  extract aggregate reads through the narrow reader protocol.
- [x] Run focused tests, unchanged ratchet preflight and actual graph/import
  producers. Fix root causes within the authorized internal scope if needed.
- [x] Update developer ownership docs and run docs/type/lint checks.
- [x] Run the full frozen-extras non-live suite without competing broad jobs;
  keep the prior timeout failure in the history, without increasing timeouts.
- [ ] Commit and immediately update PR240; run exact-commit module-size gate,
  obtain fresh-context review and resolve findings before completion.

- Base: `4fe95f15fae43e67098729381319a961330e23de`, branch
  `codex/odbc-range-reactivation`, existing isolated worktree.
- Do not change budgets, baselines, graph producers, schemas, lockfiles or CI.
- Preserve all existing public imports, method signatures, exception classes,
  error text, SQL text/order, metadata schemas and source-free recovery.
- No new routing/configuration option, authority implementation or live activation.
- Keep the direct guarded-kernel dependency on its canonical contracts visible.
- Only one writer; independent fresh-context review before completion.
- All module sizes and graph values come from repository producers. A simulated
  graph is planning evidence only, never a passing validation artifact.

## Historical diagnosis and rejected alternatives

The current commit adds the correct runtime/contracts/port triangle but crosses
both graph gates. Existing architecture tests reproduce the failure. A frozen
extras full-suite run previously produced 26,691 passes and two architecture
failures, without remaining missing-dependency failures.

An import-style change has no graph effect. Snapshot-policy deduplication alone
does not fix clustering. Consolidating the range quality bridge into generic
finalization support both broadens responsibility and still misses the gate;
do not include either change in this plan.

Moving admission into the target alone would exceed its SLOC limit. Move the
generation readback responsibility to its existing I/O owner first; do not
compress statements, erase documentation or invent modules to satisfy a number.

Graph projection for target-owned admission with an old-path compatibility shim
is clustering approximately 0.1819484862 and runtime-to-contract flow 214. Moving
readback between the same existing target/I/O modules adds no new dependency.
Actual checks on the implemented diff must confirm this projection; otherwise
stop and revisit the design, without relaxing a threshold.

## Review focus

1. Invalid plan/lease identity must still fail before metadata or network access.
2. Missing/corrupt/wrong-generation metadata must preserve exact failure behavior.
3. Count validation must still reject booleans, negative numbers and non-integers.
4. Readback must not initiate source reads, target SQL, cleanup or publication.
5. Old import paths, fingerprint bytes and local Atomic/plain MergeTree admission
   must behave identically, including unsupported topology and row policies.

## Original Task 1 (superseded by the approved autonomous revision)

**Files:**

- Modify `src/dpone/runtime/sinks/clickhouse_window_target.py`.
- Modify `src/dpone/runtime/sinks/clickhouse_window_staging.py`.
- Modify `src/dpone/runtime/sinks/clickhouse_window_admission.py`.
- Extend `tests/test_clickhouse_window_target.py`.
- Update the developer boundary description in
  `docs/feature-design-bounded-streaming-window-v1.md` if it names these owners.
- Record exact validation/review results in the PR; do not manufacture a live
  certificate or mark the guarded kernel production-ready.

**Interfaces:**

- Keep `ClickHouseWindowTarget.generation_total(plan, generation) -> int` and
  `generation_evidence(plan, generation) -> dict[str, object]` unchanged.
- Add `WindowIO.generation_total(plan: WindowPlan, generation: str) -> int` and
  `generation_evidence(plan: WindowPlan, generation: str) -> dict[str, object]`.
  They own the existing generation-name, metadata identity, count and evidence
  checks. The total wrapper calls `_identity(plan)` before delegating. The evidence
  wrapper first calls `self.generation_total(plan, generation)` (preserving public
  method dispatch), then delegates the second metadata read to the I/O evidence
  method. That I/O method does not call total again. Preserve timing fallback.
- Define the existing standalone `validate_configuration`, `validate_target`
  and `window_schema_fingerprint` functions in `clickhouse_window_target.py` with
  their current signatures and implementation behavior.
- `clickhouse_window_admission.py` re-exports those exact function objects. No
  production caller imports the shim. Existing target fingerprint imports remain.

- [x] Reproduce the two current failures in `tests/test_architecture_fitness_gate.py`
      and record baseline SLOC using repository `count_sloc`.
- [x] Add readback tests using the existing `build(tmp_path)` helper. Assert a
      `WindowIO` readback returns stored count/evidence, rejects missing or foreign
      metadata and invalid counts, preserves missing-timing fallback, and never
      calls the connector. Observe failure because the new I/O methods do not yet
      exist; keep all existing target behavior tests unchanged.
- [x] Add characterization tests for target wrapper validation order and exact
      compatibility-import identities. Pin fingerprints for the existing schema
      and verify evidence calls the target's `generation_total` before its second
      metadata read, including when that method is overridden by a subclass.
- [x] Move generation readback bodies into `WindowIO`; retain thin target wrappers.
      Do not change evidence contents or errors, or consolidate multiple reads into
      one as an incidental optimization.
- [x] Move the three admission functions into the target module, deduplicate only
      imports, and retain a documented old-path compatibility shim. Keep their
      validation order and SQL byte-for-byte unchanged.
- [ ] Run focused window, full-refresh, guarded-publication and architecture tests.
      Check actual module size, imports, graph fitness and layer metrics. If any
      budget remains red, stop; this plan does not authorize another ad hoc move.
- [x] Update the applicable developer description and plan result checklist.
- [ ] Run broad validation with the frozen extras profile below.
- [ ] Obtain a fresh-context review covering correctness, compatibility, cohesive
      responsibilities, retained authority, docs and exact-commit evidence.
- [ ] Commit, push and update existing PR #240. Do not create a duplicate PR,
      merge, publish or activate the ODBC route under this refactor task.

## Verification commands

Use the same extras for every `uv run` invocation to avoid changing the environment
under concurrent checks:

```bash
uv run --frozen --extra postgres --extra gcp --extra columnar --extra dbt-mssql --extra accel \
  pytest tests/test_clickhouse_window_target.py tests/test_clickhouse_guarded_publication.py \
  tests/test_clickhouse_full_refresh_publication.py tests/test_architecture_fitness_gate.py -q
```

With the same frozen extras prefix, run `ruff check .`, `ruff format --check .`,
`mypy --config-file mypy.ini`, `dpone docs check-import-rules`,
`dpone docs check-architecture-fitness` and
`dpone docs check-layer-metrics --baseline docs/layer_metrics_baseline.json`.
Run the module-size command from AGENTS with full base/head SHA values after
commit. Run the docs checks and strict MkDocs for changed documentation.

```bash
uv run --frozen --extra postgres --extra gcp --extra columnar --extra dbt-mssql --extra accel \
  pytest -m "not integration_live" -n auto --dist loadfile --tb=short
```

Report PASS/FAIL/SKIP/UNVERIFIED separately. Live database certification is outside
this behavior-neutral refactor and remains UNVERIFIED, not passed.

## Execution handoff

### Historical execution finding (2026-09-30, before autonomous revision)

The original implementation reached verification but was **BLOCKED**.
The repository architecture producer reports clustering
`0.18194848624091953`, below the unchanged hard cap (the green-target warning
remains). The module-size producer reports target **419 LOC / 385 SLOC** and
I/O module **337 SLOC**. Although both are below hard400, the target introduces
unbaselined warning debt above **350 SLOC**, which ratchet-v2 rejects.
The size forecast above overlooked this stricter no-new-debt condition.

No limit, baseline or metric producer was changed. The implementation is retained
as an uncommitted candidate; no merge/release readiness is claimed. Following this
plan's stop condition, a revised responsibility boundary must be approved before
another relocation. Live certification remains UNVERIFIED.

Fresh-context review confirmed preserved behavior and the blocking plan defect.
A subsequent in-memory graph/size analysis rejected a small evidence-builder
extraction as sufficient: the existing evidence module would raise clustering
to approximately `0.1822076207` even with primitive inputs; canonical contract
imports would also raise the flow to 215. A new stdlib-only helper could fit the
graph, but chunk-record construction plus the complete metadata-save expression
offer only 44 SLOC to extract. Saving the required 35 would leave at most nine
SLOC for all replacement calls, explicit arguments, persistence wrappers and
imports. No responsibility-coherent correction satisfying both gates is proven.
These are design forecasts, not certification and not authorization for another
implementation attempt.

Validation of the preserved working-tree candidate:

- PASS: 182 focused cases including architecture; 141 window/publication cases
  also rerun with an explicit test summary; 32 documentation language cases.
- PASS: Ruff lint/format, mypy (1,239 files), import rules, layer metrics,
  architecture fitness, documentation links, generated references, strict MkDocs.
- FAIL: module-size ratchet preflight (385 SLOC versus warning350).
- FAIL: full non-live run: 26,712 passed, one failed, 583 skipped, 1,100.46 seconds.
  The failure was a 20-second subprocess timeout in the unrelated doctor import
  runtime-state case with `PYTHONTRACEMALLOC=5`. Both cases in that parameter
  family passed unchanged on isolated rerun (12.61 seconds). The broad run is
  still recorded as FAIL; isolated success does not replace a green broad run.
- UNVERIFIED: exact-commit certification and live routes. No commit, merge or
  publication was performed. The candidate and logs remain available locally.

Logs are under `.superpowers/sdd/2026-09-30-clickhouse-window-coupling/`, including
`full-suite.log`, `timeout-rerun.log`, `focused.log`, `architecture.log`,
`layers.log`, and `module-size-preflight.log`. These working-tree observations
are not release receipts. Packaging is N/A to this internal refactor.

Recommended: native execution by the current integrator, followed by one
fresh-context reviewer. This is one tightly coupled relocation; parallel writers
would overlap the same two modules. Maintainer approved native implementation
on 2026-09-30 ("implement") under the requested Superpowers workflow.

### Revised implementation evidence (2026-09-30)

The approved autonomous revision resolves the original architecture and size
blockers without changing baselines, budgets, metric producers or CI. Actual
repository producers report target **350 SLOC**, staging **348 SLOC**, query
helper **61 SLOC**, clustering **0.18187544259163435**, and maximum runtime-to-
contract flow **214**. The unchanged size-ratchet preflight reports OK with the
existing 48 debt entries unchanged; the advisory clustering warning remains.

- PASS: 148 focused window/query/publication tests; query tests first failed
  because the implementation module did not exist (five failures).
- PASS: Ruff lint/format, mypy (1,239 configured files plus explicit checking of
  the new query module), import rules, architecture fitness and layer metrics.
- PASS: documentation links, generated-reference parity, 32 documentation
  language tests, strict MkDocs (121.89 seconds).
- PASS: AST comparison of the three admission functions; target lifecycle
  methods are unchanged except the tested metadata-readback delegation.
- PASS: full frozen-extras non-live suite, 26,720 passed, 583 skipped, ten
  third-party pytest import-rewrite warnings, 506.07 seconds, exit zero. The
  historical timeout did not reproduce; skips are not live certification.
- PASS: fresh-context whole-branch review with no Critical/Important/Minor
  findings, including independent repetition of all 148 focused cases.
- PENDING at this pre-commit snapshot: commit and exact-commit size receipt;
  final source identity and hosted checks are recorded in PR #240. The prior
  broad timeout remains FAIL in history.
- UNVERIFIED: live route certification and production authority/backend/router
  integration; this internal refactor neither activates nor certifies them.

Current logs use the `revised-` and `query-` prefixes in the evidence workspace
above. Working-tree gate results are not release receipts.
After completion, preserve this evidence workspace under
`test_artifacts/clickhouse-window-coupling-20260930/` rather than deleting logs.
