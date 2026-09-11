# DDA-02 completion and integration handoff

Date: 2026-09-10. PR: <https://github.com/PaulKov/dpone/pull/26>.
Reviewed implementation checkpoint: `3f37418f898bcd60f475ede8fbd89ca5ad24c7a4`.
Reviewed integrated-guide follow-up: `ad9d96bce22a3514caaf70b0007251f1dc0c89f2`.
Production helper commit: `3dea445d22d7b88b83c37d8f6383670cb6f91447`.
Immutable planning dependency: `f3682940f8864563cde0e6b6ecee60f746b49020`.
Audited production baseline: `d5ad9aaecc900c24df421b160ed36b4cfc726e45`.

## Result and scope

Two cohesive internal helpers are ready for component review/integration.
`digest_prepared_rows` consumes one full Mapping iterator, independently encodes
the business and full projections, retains the existing finite metadata
allowance and returns frozen existing-format digests and the verified count.
`build_prepared_insert` projects explicit ordered columns over the caller's
verified UNION ALL source, using canonical lineage and row-hash expressions.
No runtime wiring, normalization bypass or validity flag was introduced.

Owned implementation paths:

- `src/dpone/runtime/sinks/mssql_native_prepared_digests.py`
- `src/dpone/runtime/sinks/mssql_native_prepared_insert.py`
- `tests/test_mssql_native_integrity_readbacks.py`
- `tests/test_mssql_native_metadata_insert.py`
- `tests/test_mssql_native_metadata_insert_parity.py`
- `docs/delivery-acceleration/preparation.md`
- `test_artifacts/delivery-acceleration/dda-02/`

Audit task changes against the planning dependency, not against master alone:
the common planning import is not DDA-02 implementation ownership. No shared
coordinator, normalizer, schema, factory, fixture, navigation, changelog,
dependency declaration, lockfile, release, version or provider source was edited.
The initial dependency was imported through a normal fast-forward merge; all
pushes were ordinary pushes. No history was reset or force-pushed.

## Compatibility and documentation

CLI/manifest defaults, public admission, direct BCP consumers, native byte and
digest versions, journal fields and recovery remain unchanged. No migration is
required. Individual error classifications are retained; with multiple corrupt
rows, one-pass validation can report an earlier metadata error before a later
business error previously found first by the business-only pass. The guide
documents this ordering difference and preserves all publication authorities.

The English preparation guide provides exact source SQL/alias/type expectations,
normalizer validation/evidence integration, structural acceptance, diagnostics,
recovery and verification commands. It explains that component addition alone
does not change the shipped route. DDA-06 owns inbound navigation, shared docs,
operations journey, changelog and runtime activation.

## Validation outcomes

| Check | Status | Evidence / observation |
|---|---|---|
| Red tests before helper implementation | Expected FAIL | `red-digests.log`, `red-insert.log`: missing helper imports |
| Focused five-file suite | PASS | `focused-results.json`, `focused.log`: 57 tests; rerun after environment remediation on reviewed checkpoint |
| Task YAML and change-aware selection | PASS | `contract.log`, `plan.log` |
| Ruff lint and format | PASS | `ruff.log`, `format.log` |
| Project mypy | PASS | `mypy.log`: 1163 files |
| Explicit helper mypy | PASS | `helper-mypy.log`: 2 files |
| Import and layer rules | PASS | `imports.log`, `layers.log`; budgets unchanged |
| Exact-head module size | PASS | `size-results.json`, `size.log`; governed base/head recipe, no baseline edits |
| Docs/generated references/language/strict build | PASS | `docs-results.json` and corresponding logs, including revised guide |
| Complete non-live pytest, 2 workers | FAIL | `suite-results.json`, `pytest.log`: 20537 passed, 815 skipped, 28 failed, 2 collection errors |
| Declared test-environment extras installed | PASS | `environment-extras.log`, `environment.json`; `uv sync --frozen`, no tracked dependency edits |
| Complete last-failed rerun after extras | FAIL | `failed-rerun-results.json`: 27 passed, 25 skipped, 1 failed, no collection errors |
| Remaining doctor case isolated, without xdist | FAIL | `doctor-isolated-results.json`, `doctor-isolated.log`: existing 20-second subprocess timeout |
| Signed-catalog benchmark isolated diagnosis | PASS | `catalog-isolated-report.json`; independent reviewer also ran its two tests successfully |
| Independent reviews and docs fixes | PASS | `review.md`: fresh architecture review, docs review and second fresh review |
| Ownership audit and source binding | PASS | `scope-audit.json`, `validation-binding.json` |
| Live SQL Server/ClickHouse/BCP | SKIP | No disposable environment approved; none started |
| DDA-06 integrated structural checks | PASS, hermetic scope | Guide links the 57-case scoped run and coordinator tests at `49160c3982705b8576c50c0d06e740ae13991e08`; final frozen integration gates remain separate |
| Live route scan count / performance | UNVERIFIED | No approved live run or measurement |
| Packaging/release certification | N/A | No packaging or release change or publication authority |

The full-suite report is deliberately retained as FAIL. Most failures were
missing declared extras (`pyarrow`, `psycopg`, GCP, dbt, S3 and native acceleration)
in the original base/dev environment. Installing those extras fixed the
dependency-related cases. The original catalog benchmark aggregate failure did
not expose its individual gate; its exact historical cause is unknown. The
isolated report passes determinism, mutation detection, latency budget and
absence of network/subprocess calls, and does not load either DDA-02 helper.

The remaining reproducible failure is:

`tests/test_doctor_import_integration.py::test_import_probe_replays_effective_env_derived_runtime_state[PYTHONTRACEMALLOC-5-...]`

At line 457 its existing subprocess call exceeds `timeout=20`. It fails both in
the two-worker failed-set rerun and in isolation. The test and
`src/dpone/readiness/python_import_health.py` have no changes from the planning
dependency. Neither belongs to DDA-02's owned paths. Its underlying host/runtime
cause is not established; no timeout was weakened and no unrelated source was
edited. This is a concrete follow-up for the integration coordinator or a
separately scoped owner. A complete broad rerun after environment remediation
was not represented as a PASS.

The evidence producer records actual command exit codes, durations, source hashes
and original checkout status. Some checks began before the implementation commit;
`validation-binding.json` compares their helper/test bytes with the reviewed
checkpoint. The guide-only review fixes have their own subsequent docs PASS.
Evidence additions after the reviewed checkpoint do not change helper/test bytes.
Raw pytest logs retain their emitted trailing whitespace to preserve captured
output. Whitespace validation passes for source, documentation and JSON; an
unfiltered `git diff --check` also reports that raw log formatting.

## Exact integration instructions

1. DDA-06 has already cherry-picked the reviewed checkpoint's two commits with
   provenance, reporting local integration commits `a3c3a2e` and `f8ef4d8`.
   Do not apply the helpers again. Also apply the separate reviewed guide update
   `ad9d96bce22a3514caaf70b0007251f1dc0c89f2`, reflecting DDA-06 checkpoint
   `49160c3982705b8576c50c0d06e740ae13991e08`.
   The final evidence commit does not change helper code.
2. Pass `source_sql` as the explicit SELECT/UNION ALL body built from verified
   raw receipt stages. The helper wraps it as `FROM (...) AS r`. Pass an owned
   qualified `target_sql` and target-native ordered business schema from
   `resolved.types`, excluding framework columns.
3. Preserve the guide's common prevalidation and completion split. Direct BCP
   retains prevalidation -> metadata UPDATE -> completion. Bounded preparation
   performs canonical INSERT -> consumed evidence -> shared prevalidation and
   completion -> one full-column iterator for both digests.
4. Compare the business digest with existing aggregated receipt sums. Persist
   the full digest in the unchanged prepared recovery field. Keep all independent
   prepublication raw/prepared verification, owner/object/capacity checks, target
   clock UPDATE, transaction receipt and evidence/checkpoint ordering.
5. Retain DDA-06's proven four raw plus two prepared typed scans and zero
   preparation metadata UPDATEs in its actual runtime fixtures. Keep direct BCP and public
   SWITCH rejection regression coverage. DDA-02 does not certify those combined
   call sites merely because its iterator test passes.
6. Resolve or explicitly triage the reproducible doctor timeout through its
   authorized owner and run the required integrated/CI gates before merging.
   Obtain approved live evidence separately before any performance or production
   certification claim.

Readiness: **ready for component review and integration; not declared merge- or
release-ready while the broader gate remains FAIL**. The PR is reviewable with
the exact remaining limitation and reproducible evidence. No live service,
container, release, tag, publication, PR merge or provider change was performed.
