# Airflow local promotion integrity validation report

- Date: 2026-07-15
- Branch: `codex/v0.72.5-local-promotion-integrity`
- Base commit: `64e0b3946281da9f4541f11894ff20c372fa210f`
- Specification:
  `docs/feature-design-airflow-local-promotion-integrity-v0725.md`
- Task contract:
  `test_artifacts/agent-policy/2026-07-15-v0725-local-promotion-integrity.yml`

## Result

`PASS` for the approved local promotion integrity scope. The public readiness
facade rejects missing, empty, or mismatched actor policy before mutation.
Preview and safe-sample local promotion use fixed facade actors and a
compare-and-swap guard based on the observed current deployment. The beginner
commands did not change.

This report does not certify live MSSQL, ClickHouse, Vault, Kubernetes, remote
artifact delivery, or power-loss behavior.

## Acceptance evidence

| Acceptance criterion | Status | Evidence |
|---|---|---|
| Empty or mismatched allowlist fails before mutation | PASS | Focused facade and CLI tests verify exit `4`, structured codes, and absence of pointer/current/audit writes |
| Missing actor fails before mutation | PASS | `test_cache_sync_result_rejects_missing_promoter_before_mutation` |
| Preview uses the fixed local actor and CAS | PASS | CLI regression plus the First DAG golden-path pointer contains `local://dpone-airflow-preview` |
| Safe sample uses the fixed local actor and CAS | PASS | CLI regression plus the golden-path pointer contains `local://dpone-safe-sample` and the preview deployment as previous identity |
| Concurrent stale promotion cannot overwrite current | PASS | `test_local_cache_sync_result_rejects_concurrent_stale_promotion` preserves the winner and returns `DPONE_CURRENT_POINTER_CAS_MISMATCH` |
| Public provider loads the preview | PASS | Golden path loaded `orders_daily` with one workload, zero skips, and zero errors |
| Five-command beginner contract remains unchanged | PASS | First DAG documentation and executable CLI journey |

## Commands and observed results

| Check | Status | Observed result |
|---|---|---|
| Focused Airflow cache/self-service suite | PASS | All selected tests passed |
| Full non-live suite | PASS | `4302 passed, 472 skipped` in `324.82s` |
| `ruff check .` | PASS | No findings |
| `ruff format --check .` | PASS | `3043 files already formatted` |
| `mypy --config-file mypy.ini` | PASS | `485 source files` |
| Import rules | PASS | No architectural import violations |
| Layer metrics | PASS | `4562` edges; reported cross-layer ratio `0.300`; no issues |
| Architecture fitness hard test | PASS | Exact cross-layer budget is below or equal to `0.300` |
| Module-size gate | PASS | No hard failures; existing warning set did not grow beyond the configured budget |
| Documentation links/contracts | PASS | `417` Markdown files and `1733` local links checked |
| Generated references | PASS | `2/2` references in sync |
| Generated quality metrics | PASS | Producer output is up to date |
| Documentation language tests | PASS | `4 passed` |
| Compatibility policy | PASS | `19` registry entries; documentation in sync |
| Strict MkDocs build | PASS | Completed in `7.34s` |
| Root/native/Airflow package builds | PASS | Wheels and sdists built outside the repository tree |
| Twine metadata check | PASS | All six wheel/sdist artifacts passed |
| Installed-wheel smoke, Python 3.12 | PASS | Clean venv; `12` manifest cases validated/rendered/reported |
| Workflow security | PASS | `0 errors, 0 warnings` |
| Agent task contract | PASS | `0 errors, 0 warnings` |
| `git diff --check` | PASS | No whitespace errors |

The first broad-suite attempt exposed a real architecture-budget regression:
one direct readiness-to-runtime-module import increased the exact cross-layer
ratio from `0.299868` to `0.300022`. The implementation now imports through the
existing `dpone.runtime.deployment_cache` facade. The hard threshold was not
weakened, the focused architecture test passed, and the full non-live suite was
rerun successfully.

## Executable beginner journey

The journey was executed in an isolated temporary project:

```bash
dpone init project --airflow
dpone init pipeline orders_daily --recipe mssql-to-clickhouse-incremental --airflow
dpone check pipelines/orders_daily
dpone airflow preview orders_daily
dpone run pipelines/orders_daily --sample 1000 --target temporary
```

The first four commands returned `0`. The public provider report contained:

```text
loaded: orders_daily
skipped: none
errors: none
workload_packs: orders_daily
promoted_by: local://dpone-airflow-preview
```

The sample command returned the expected fail-closed exit `3` with
`DPONE_SAFE_SAMPLE_CERTIFIED_COPY_EXECUTOR_NOT_CONFIGURED`; no live data copy
was claimed. Its local runnable deployment was nevertheless materialized and
promoted with `local://dpone-safe-sample`, preserving the preview deployment as
the previous pointer identity.

## Independent review

- Architecture/security reviewer: no high or medium findings. Its low test-gap
  suggestion was already covered by
  `test_airflow_cache_sync_cli_requires_explicit_platform_allowlist` and
  `test_airflow_cache_sync_cli_rejects_promoter_outside_allowlist`.
- Documentation/UX reviewer: no blocking finding. Its low request for direct
  Python API migration guidance was addressed in `docs/airflow-cache-sync.md`.
- Both reviewers confirmed that the five-command beginner journey remains
  unchanged and no new ADR is required for this correction.

## Live and residual risk

| Area | Status | Reason |
|---|---|---|
| MSSQL/ClickHouse live copy | N/A | No connector behavior changed |
| Vault/Kubernetes identity | N/A | No credential resolver or runtime identity behavior changed |
| Remote cache materializer | N/A | Scope is the local mutation facade |
| Power-loss certification | N/A | Atomic materializer internals are unchanged |

Actor strings are policy assertions, not authentication credentials. Cache
filesystem permissions and CI/workload identity remain the external security
boundary. A stale CAS is intentionally not retried automatically; the caller
must rerun after observing the new current deployment.

The scoped change is ready for code review and merge. It is not, by itself, a
claim that the complete Industrial Self-Service Airflow roadmap is finished or
that live routes are certified.
