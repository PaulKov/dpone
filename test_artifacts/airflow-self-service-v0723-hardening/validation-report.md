# Airflow self-service v0.72.3 hardening validation

- Base: `origin/master@bfa778b566bdf29fbe31b2c7efc7a61d528ed1c9`
- Branch: `codex/v0.72.3-self-service-hardening`
- Validation date: 2026-07-15
- Feature specification:
  `docs/feature-design-airflow-self-service-v0723-hardening.md`

## Finding disposition

| Finding | Disposition | Direct evidence |
|---|---|---|
| Preview reported success without a provider-loadable workload | Fixed | `tests/test_airflow_self_service_preview_provider.py`, `tests/test_airflow_dag_loader.py` |
| Nested delivery security policy did not affect deployment identity | Fixed | `tests/test_airflow_release_deployment_contracts.py` |
| Static route metadata could authorize a production sample | Fixed fail-closed | `tests/test_airflow_safe_sample_policy.py`, `tests/test_airflow_self_service_cli.py` |
| Malformed deployment index could silently load zero DAGs | Fixed | `tests/test_airflow_deployment_index_provider.py` |
| Duplicate deployment-index artifact IDs could resolve ambiguously | Fixed fail-closed | `tests/test_airflow_deployment_index_provider.py` |
| Asset multi-writer ambiguity/cross-DAG cycles were not blockers | Fixed | `tests/test_airflow_asset_graph.py`, `tests/test_airflow_asset_cross_dag.py` |
| Inferred producer edge did not materialize a provider outlet | Fixed | `tests/test_airflow_inferred_pack_outlets.py`, `tests/test_airflow_asset_outlets.py` |
| Explicit schedules disagreed with build-inferred producer outlets | Fixed; non-inferred edges still require an outlet | `tests/test_airflow_asset_cross_dag_outlet.py` |
| Sample run identity/evidence could collide | Fixed with unique IDs and create-only evidence | `tests/test_airflow_safe_sample_runtime_evidence.py`, `tests/test_airflow_self_service_cli.py` |
| Interrupted ClickHouse sample could leak a target indefinitely | Fixed with server-side TTL plus best-effort cleanup | `tests/test_airflow_clickhouse_temporary_target_adapter.py` |
| Provider god modules escaped the core module-size/coverage gates | Fixed by cohesive extraction and CI gates | provider module-size and coverage results below |
| First-DAG docs/generated references drifted from runtime behavior | Fixed | docs checks and strict MkDocs results below |

## Frozen-plan completion verdict

The hardening patch repairs the confirmed `v0.72.2` regressions, but it does not
turn every frozen-plan phase into a completed production capability.

| Plan slice | Observed state after hardening | Verdict |
|---|---|---|
| Phase 0 trust blockers | Static/package/parse gates exist; live golden-route and remote materializer evidence are unavailable in this environment | PARTIAL / live UNVERIFIED |
| Phase 1A First DAG | Scaffold, static check, preview release/deployment/index and provider load path pass end to end | COMPLETE |
| Phase 1B First Safe Run | Contracts and fail-closed handoff exist; the beginner command stops before live copy because no certified executor/evidence adapter is configured | PARTIAL |
| Phase 1C compatibility | Airflow/Kubernetes/env bridges, GC, recovery and migration surfaces exist but the project backlog still labels production integrations partial | PARTIAL |
| Phase 2 authoring | Outside the `v0.72.3` hardening scope; no completion claim | NOT ASSESSED AS COMPLETE |
| Phase 3 Airflow-native operations | Outside the `v0.72.3` hardening scope; live assets/rerun/telemetry certification not demonstrated | NOT ASSESSED AS COMPLETE |
| Phase 4 standardization | No v1.0 conformance/reference-deployment evidence in this review | NOT COMPLETE |

Fresh-wheel golden-path observation:

| Command | Exit | Result |
|---|---:|---|
| `dpone init project --airflow` | 0 | Five project files created |
| `dpone init pipeline orders_daily --recipe mssql-to-clickhouse-incremental --airflow` | 0 | Authoring source, domain and test created |
| `dpone check pipelines/orders_daily` | 0 | Static, zero-network check passed |
| `dpone airflow preview orders_daily` | 0 | Non-runnable provider-loadable preview materialized |
| `dpone run pipelines/orders_daily --sample 1000 --target temporary` | 3 | Safe handoff/evidence produced; live copy blocked by `DPONE_SAFE_SAMPLE_CERTIFIED_COPY_EXECUTOR_NOT_CONFIGURED` |

## Automated validation

| Scope | Command/result | Status |
|---|---|---|
| Full non-live suite | `DPONE_TEST_USE_INSTALLED_PACKAGE=1 uv run pytest -m "not integration_live" -n auto --dist loadfile --cov=src/dpone --cov=packages/dpone-airflow-pack/src/dpone_airflow_pack --cov-report=xml`: 4261 passed, 472 skipped in 505.32s, total coverage 86.22% | PASS |
| Airflow provider contracts | Compatibility matrix contract, DAG loader contract, runtime v3 and interval rendering: 15 passed | PASS |
| Provider coverage | `coverage report --include=packages/dpone-airflow-pack/src/dpone_airflow_pack/*`: 81% | PASS |
| Lint | `uv run ruff check .` | PASS |
| Formatting | `uv run ruff format --check .`: 3033 files formatted | PASS |
| Type checking | `uv run mypy --config-file mypy.ini`: 484 source files | PASS |
| Import boundaries | `uv run dpone docs check-import-rules` | PASS |
| Layer metrics | Baseline gate, no regression | PASS |
| Core module size | Baseline gate, no hard-limit violation | PASS |
| Provider module size | No warnings or hard-limit violations | PASS |
| Generated references | 2/2 in sync | PASS |
| Documentation links | 415 Markdown files and 1732 local links | PASS |
| Documentation language | 4 passed | PASS |
| Strict documentation build | `uv run mkdocs build --strict` | PASS |
| Workflow security | 0 errors, 0 warnings; 8 contract tests passed | PASS |
| Agent policy | Setup validation plus 101 policy tests | PASS |
| Patch integrity | `git diff --check` | PASS |
| Package build | Core, native acceleration and Airflow provider wheel/sdist builds | PASS |
| Package metadata | `twine check` for all six artifacts | PASS |
| Fresh wheel smoke | Core and provider wheels installed into a new Python 3.12 venv; 12 CLI cases and canonical/legacy provider imports passed | PASS |

Build artifacts are generated under
`test_artifacts/airflow-self-service-v0723-hardening/dist/` and intentionally
remain ignored. They retain package version `0.72.2`; this branch is the
Unreleased implementation candidate for `0.72.3`, and release versioning is a
separate release action.

## CI and live status

| Check | Status | Reason |
|---|---|---|
| Pull request CI | PASS | [Run 29419481847](https://github.com/PaulKov/dpone/actions/runs/29419481847) on implementation commit `e6982e0f`: 16 successful checks, 0 failures; the Pages deployment job is intentionally skipped for pull requests. |
| Airflow 2.10/2.11/3.2/3.3 wheel matrix | PASS | All eight Airflow/Python 3.11/3.12 combinations passed in the pull request workflow. |
| Live MSSQL to ClickHouse sample | UNVERIFIED | No explicitly approved live environment or credentials were supplied. |
| Live Vault Kubernetes Auth | UNVERIFIED | No explicitly approved Kubernetes/Vault environment was supplied. |
| Live ClickHouse TTL expiry timing | UNVERIFIED | Unit contracts cover DDL and cleanup; ClickHouse TTL enforcement is asynchronous and requires live observation. |

No skipped or unavailable live check is counted as a pass.

## Remaining risk

The production sample path now requires a trusted composition root to supply
externally verified route certification IDs. The default CLI has no such
adapter and therefore fails closed. A concrete certification-store adapter and
live route evidence remain deployment responsibilities; bundled/user-authored
metadata cannot cross this trust boundary.

A fresh-context architecture reviewer raised one inferred-outlet concern. The
final implementation made the invariant explicit and added regressions:
`reason=inferred` is accepted only because compact-pack compilation materializes
the same sink outlet; non-inferred edges without an outlet produce a warning;
explicit asset schedules accept build-inferred outlets. The follow-up review
reported no blocking finding and recommended approval.

The context-only edge explanation helper does not independently recompute the
global cross-DAG cycle report. Publication remains fail-closed because the
authoritative `AirflowDagSpecBuilder.build()` path always applies that global
cycle gate. Extending context-only diagnostics is a low-priority UX follow-up,
not a publication safety gap.
