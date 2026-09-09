# Airflow composite run identity and reproducible rerun v1 validation

- Commit under test: uncommitted Phase 3 slice on
  `codex/airflow-self-service-roadmap`
- Base commit: `7d058420262e18216693e37b37240459293722ce`
- Base contract: `docs/feature-design-airflow-run-identity-rerun-v1.md`
- Verified at: `2026-07-17T06:34:21+03:00`
- Overall local status: PASS with explicitly UNVERIFIED external evidence

## Contract evidence

| Gate | Status | Observed result |
| --- | --- | --- |
| Focused identity/planner/provider/runtime/evidence/schema tests | PASS | Final focused suites passed, including malformed identity fail-closed, all four selection pairs, retention failures, mismatch correlation and schema equality |
| Full non-live regression | PASS | 4966 passed, 475 skipped, 0 failed in 297.35 seconds on the final local diff |
| Airflow regression selection | PASS | Complete `-k airflow` selection passed before the final dependency-only refactor |
| Ruff | PASS | Repository lint passed; 3213 files are formatted |
| Mypy | PASS | 583 source files, no issues |
| Import rules | PASS | No architectural import violations |
| Layer metrics | PASS | 4887 edges; cross-layer ratio 0.300 and max cross flow 97, within the project budgets |
| Module size | PASS | No hard-limit failures; existing warnings remain visible |
| Architecture fitness | PASS | Average clustering 0.178; cross-layer ratio below the strict 0.300 assertion; no findings |
| Workflow security | PASS | 0 errors, 0 warnings |
| Planner performance | PASS | Slowest complete CLI plan test was 0.76 seconds, below the 2-second target |
| Generated schemas/references | PASS | 3/3 generated references synchronized; identity, rerun-plan, XCom and evidence schemas passed producer tests |
| Documentation | PASS | 502 Markdown files and 1819 local links checked; language contracts and strict MkDocs build passed |
| Packaging | PASS | `dpone`, `dpone-native-accel` and `dpone-airflow-pack` 0.72.3 sdist/wheels built; Twine accepted all artifacts in `dist/` |
| Airflow 3.2.0 / Kubernetes provider 10.14.0 | PASS | 8 exact-runtime provider tests passed after reinstalling the current local provider wheel |
| Airflow 3.3.0 / Kubernetes provider 10.19.0 | PASS | 8 exact-runtime provider tests passed after reinstalling the current local provider wheel |
| Airflow 2.10.x and 2.11.x exact jobs | UNVERIFIED | Compatibility matrix remains configured; this local slice did not duplicate those isolated CI jobs |
| Live original GitDagBundle critical rerun | UNVERIFIED | No approved Airflow/Kubernetes environment or credentials were provided; the planner performs no remote mutation by design |
| Fresh-context agent review | UNVERIFIED | Agent spawn remained unavailable because the task-level agent thread limit was reached |

## Commands

```text
uv run python tools/agent_policy/select_checks.py --base-ref origin/master
uv run pytest tests/test_airflow_run_identity.py tests/test_airflow_rerun_plan.py ... -q
uv run pytest -k airflow -q
uv run pytest -k "cli or command" -q
uv run pytest -k "manifest or schema or compatibility" -q
uv run pytest -k "runtime or state or checkpoint or replay" -q
uv run pytest -m "not integration_live" -n auto --dist loadfile
uv run pytest tests/test_airflow_rerun_plan.py -q --durations=10
uv run ruff check .
uv run ruff format --check .
uv run mypy --config-file mypy.ini
uv run dpone docs check-import-rules
uv run dpone docs check-layer-metrics --baseline docs/layer_metrics_baseline.json
uv run dpone docs check-module-size --baseline docs/module_size_baseline.json
uv run dpone docs check-architecture-fitness
uv run dpone docs check-docs
uv run dpone docs check-generated-references
uv run dpone docs check-compatibility
uv run pytest tests/test_docs_language_contracts.py -q
uv run mkdocs build --strict
uv run python tools/agent_policy/workflow_security.py .
uv build
uv build packages/dpone-native-accel --out-dir dist
uv build packages/dpone-airflow-pack --out-dir dist
uv tool run twine check dist/*
.venv-airflow32/bin/python -m pytest tests/test_airflow_dag_loader_contract.py -q
.venv-airflow33/bin/python -m pytest tests/test_airflow_dag_loader_contract.py -q
```

## Public contract and compatibility

- `dpone.airflow-run-identity.v1` is additive, canonical, bounded to 16 KiB and
  contains only allowlisted non-secret identity fields.
- Existing indexes, packs, XCom summaries and evidence remain readable when the
  optional identity or bundle reference is absent.
- Index-backed tasks pin release, deployment, DAG spec and workload pack at
  parse time. Runtime never resolves `current`.
- Airflow bundle selection remains independent from dpone artifact selection.
- `dpone airflow rerun-plan` is local-only and plan-first; it does not call the
  Airflow API, metadata database, Variables, Connections, Vault or cache sync.
- A malformed present identity blocks `run-spec-exec` before any workload
  command runs and is emitted as a redacted structured blocker.

## Documentation and CJM impact

The beginner five-command path is unchanged. Operator documentation now
explains composite identity, original/latest bundle and artifact combinations,
retention failures and the diagnostic `rerun-plan` command. Provider docs state
the parse-side-effect contract and canonical runtime propagation behavior.

## Remaining risk

- Live clearing of an original critical Git-backed DAG run against retained
  production artifacts remains unverified and must not be claimed as certified.
- Airflow 2.10/2.11 exact compatibility remains a CI responsibility for this
  slice; only 3.2 and 3.3 were rerun locally.
- Exact Airflow tests emitted macOS pytest temporary-directory cleanup warnings
  when both isolated environments ran concurrently; all contract tests passed.
- Fresh independent review remains pending. Keep the pull request draft until CI
  and maintainer review complete.

The implementation is ready for CI and maintainer review, not yet for a live
production reproducible-rerun certification claim.
