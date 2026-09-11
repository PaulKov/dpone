# DDA-01 completion and validation evidence

Historical checkpoint through `006e0a1`. Later independent-review fixes and
current source/checks are recorded in [review-v2.md](review-v2.md).

Date: 2026-09-10. PR: https://github.com/PaulKov/dpone/pull/28 (draft).
Production code: `7fcdabd43ed8656f1de52415fa66d8b7db6befd4`.
Reviewed documentation: `3e578759fc531f4a7701d977241f327b28883417`.
The later artifact-only commit records this report; it is not a new code identity.
Baseline: `d5ad9aaecc900c24df421b160ed36b4cfc726e45`.
Immutable planning dependency: `f3682940f8864563cde0e6b6ecee60f746b49020`.
Environment: macOS 26.3.2 arm64, Python 3.12.11, uv-managed local environment.

## Result and compatibility

Implemented immutable observation contracts, an observer port, bounded collection,
injected-clock phase/total scopes, explicit unavailable diagnostics, and the
versioned offline benchmark comparison CLI. The consumer validates identity,
retained raw evidence, typed correctness assertions, warmup/trial eligibility,
medians and campaign thresholds before accepting comparisons. Artifact handling
retains portable references and writes output atomically with explicit overwrite.

This is additive diagnostics and developer tooling. Existing manifests, public
CLI defaults, runtime call sites, tuples, journal/receipt schemas, state transitions
and SWITCH rejection are unchanged in DDA-01. There is no migration. Diagnostics
have no business-success or checkpoint authority. DDA-06 owns runtime composition
and shared documentation. One cohesive artifact I/O helper was approved in the
retained supplemental task contract; original frozen planning files are unchanged.

## Verification

Commands use `uv run`; the optional-dependency replay and final focused replay
explicitly use `--all-extras`. All paths below are relative to this report.

| Command or check | Status | Observed result / duration | Evidence |
| --- | --- | --- | --- |
| `python tools/agent_policy/select_checks.py --base-ref origin/master` | PASS | Change-aware plan produced | [validation-plan.log](validation-plan.log) |
| Original and supplemental task-contract validation | PASS | Both contracts valid | [original](contract-validation.log), [supplement](supplemental-contract-validation.log) |
| Focused observation + benchmark tests | PASS | 70 cases: 27 observation, 43 comparison | [focused-final.log](focused-final.log), [collection](focused-collection.log) |
| Final focused tests + docs language, with all extras | PASS | 102 passed entries, including all 70 DDA-01 cases and 32 docs checks | [focused-all-extras.log](focused-all-extras.log) |
| `ruff check .` / `ruff format --check .` | PASS | Clean; 5,811 files formatted | [ruff-final.log](ruff-final.log), [format-final.log](format-final.log) |
| `mypy --config-file mypy.ini` and focused mypy | PASS | 1,164 repository sources; six new production/tool files | [mypy-final.log](mypy-final.log), [mypy-focused.log](mypy-focused.log) |
| `dpone docs check-import-rules` | PASS | Import direction valid | [imports-final.log](imports-final.log) |
| `dpone docs check-module-size` with exact merge-base/head recipe | PASS | No new module-size debt at code 7fcdabd | [module-size.log](module-size.log) |
| `dpone docs check-layer-metrics --baseline docs/layer_metrics_baseline.json` | FAIL | runtime-to-contracts flow 216 exceeds tolerance 214 (baseline 209) | [layers-final.log](layers-final.log) |
| `dpone docs check-architecture-fitness` | FAIL (budget) | CLI exit 0 with warning; average clustering 0.18170059431282887 exceeds hard target 0.180 | [architecture-fitness.log](architecture-fitness.log) |
| Planning-source architecture comparison | PASS (observation only) | Baseline clustering 0.1813233335910147; DDA-01 increases existing debt | [baseline](architecture-fitness-planning-baseline.log), [source diff](planning-baseline-source-diff.log) |
| `dpone docs check-docs` | FAIL | Missing DDA-06-owned index.md link; corrected from an earlier erroneous PASS label | [docs-final.log](docs-final.log) |
| `dpone docs check-generated-references` | PASS | Generated references valid | [generated.log](generated.log) |
| `mkdocs build --strict` | PASS | Build completed in 65.38 s during final documentation review; two later wording-only precision edits were covered by the final 102-case replay | [mkdocs-final.log](mkdocs-final.log) |
| `PYTEST_XDIST_AUTO_NUM_WORKERS=2 uv run pytest -m 'not integration_live' -n auto --dist loadfile` | FAIL | 20,544 passed, 815 skipped, 27 failed, 2 collection errors; 1,772.42 s | [pytest-broad.log](pytest-broad.log) |
| `uv sync --locked --all-extras` | PASS | Installed missing optional test dependencies; no lock/dependency-file changes | [dependency-sync.log](dependency-sync.log) |
| Replay of every failed/collection-error file with all extras and two workers | FAIL | 685 passed, 1 failed in 137.19 s; all dependency/collection failures resolved; remaining cross-layer ratio 0.30024001745581497 > 0.300 | [pytest-replay.log](pytest-replay.log) |
| Actual DDA-05 retained SKIP fixture consumer smoke | PASS | Valid comparison stays UNVERIFIED with no ratio | [smoke](producer-consumer-smoke.log), [comparison](producer-consumer-comparison.json) |
| Actual DDA-05 timed hermetic producer/consumer smoke | PASS | Four timed samples plus retained fidelity/recovery profiles consumed; result UNVERIFIED with no live ratio | [smoke](hermetic-producer-consumer-smoke.log), [retained run](hermetic-producer-case/run.json), [comparison](hermetic-producer-case/comparison.json) |
| Fresh-context review and re-review after fixes | PASS | No remaining actionable findings in owned source/docs | [review.md](review.md) |
| Owned-path audit | PASS | No changes outside supplemental ownership after planning import | [path-audit.json](path-audit.json) |
| Live SQL, containers, route certification and performance | SKIP / UNVERIFIED | No approved disposable live environment; no measured acceleration claim | This report |
| Packaging, publication, tags and provider changes | N/A | Outside the change scope; no release action taken | This report |

Exact final focused invocation:

```bash
uv run --all-extras pytest tests/test_mssql_native_delivery_observations.py tests/test_mssql_native_delivery_benchmark.py tests/test_docs_language_contracts.py -q -rA
```

Exact replay invocation (also retained at the start of pytest-replay.log):

```bash
uv run --all-extras pytest -m 'not integration_live' -n 2 --dist loadfile tests/integration/postgres/test_postgres_snapshot_retry_live.py tests/test_architecture_fitness_gate.py tests/test_dbt_inline_publishing.py tests/test_native_acceleration_contracts.py tests/test_native_bcp_type_matrix.py tests/test_object_storage_fast_path_access.py tests/test_postgres_snapshot_retry.py tests/test_route_capability_runtime_factory.py tests/test_runtime_gcs_support.py tests/test_runtime_load_audit_state_contracts.py tests/test_runtime_state_and_reconciliation_contracts.py tests/test_semantic_refresh_clickhouse_http.py tests/test_tools_mssql_stress_config.py
```

`git diff --check` passes for source, tests, tools and documentation. The staged
raw test logs retain pytest trailing whitespace and an EOF blank line; the
unfiltered whitespace check therefore reports FAIL for those evidence files.
Their captured bytes were preserved rather than edited to change that result.

The original full run started before the final contract hardening commits and
before all optional dependencies were installed. It is not an exact-final-source
full PASS. The replay repairs its environment coverage and reruns all failed
files; it does not retroactively relabel the original result. Its sole remaining
failure is `test_architecture_fitness_current_repo_stays_inside_pre_release_cross_layer_budget`
at `tests/test_architecture_fitness_gate.py:149`. Final focused tests
cover the final production source. Optional dependencies resolved under the
existing lock; no live profiles, service startup or credentials were enabled.

The DDA-05 fixtures prove producer/consumer interoperability only. Their retained
subject/environment records remain as produced, including hermetic/dirty markers.
They are not live certification and cannot establish the 0.85/1.05 performance
acceptance targets. Deterministic synthetic unit tests also do not supply live
performance evidence. Structural acceptance remains UNVERIFIED because the frozen
comparison input schema has no structural-proof authority.

## Documentation and user journey

The English [observations guide](../../../docs/delivery-acceleration/observations.md)
explains the no-observer path, process boundaries, clock domains, units, provenance,
retry/cancellation behavior, unavailable metrics, safe compare command, output
and recovery from validation errors. Its Python and CLI examples execute/parse in
focused tests. The guide explicitly distinguishes integrated diagnostic totals
from legacy duration fields: without an independent target visibility probe,
DDA-06 runtime totals remain unavailable; the DDA-05 harness owns measured totals.
The overview link depends on DDA-06's owned index.md, as agreed with the integrator.
Shared navigation, changelog, overview and operations documentation belong to DDA-06.

## Remaining work and readiness

Ready for review and integration of the scoped implementation and retained
evidence. Not ready for merge or release while graph gates remain unresolved.
DDA-06/root have the exact layer-flow, cross-layer ratio and clustering results and own any genuine
shared dependency cleanup. No metric baseline, budget or facade was changed to
hide real dependencies. Integration must run its own current graph gates and
regression checks against the combined source commit.

Use [handoff.md](handoff.md) for the ordered cherry-picks and tested composition
instructions. This task does not mark the frozen feature specification IMPLEMENTED;
that shared change belongs to DDA-06 after integration evidence is linked.
