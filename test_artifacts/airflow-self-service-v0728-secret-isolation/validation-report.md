# v0.72.8 Airflow Connection Secret attempt isolation validation

- Task: `DPONE-V0728-AIRFLOW-CONNECTION-SECRET-ISOLATION`
- Branch: `codex/v0.72.8-airflow-connection-secret-isolation`
- Base commit: `ae6e3498ce566ad3aba0881fddbd8fc19be8a534`
- Validated at: `2026-07-16`
- Implementation status: `PASS`
- Fresh-context review status: `PASS`
- Live Airflow/Kubernetes certification: `UNVERIFIED`

## Scope proved

The Airflow Connection compatibility bridge now derives one deterministic,
bounded Kubernetes Secret name for each Airflow task attempt from `dag_id`,
`task_id`, `run_id`, `try_number`, and `map_index`. Publication is create-only
and immutable: HTTP `409` fails closed instead of replacing another attempt's
credential object. The pod mounts the exact attempt Secret, and synchronous
cleanup deletes only that object.

The implementation retains no Connection URI or Kubernetes Secret payload on
the operator. Airflow context is validated before credential or Kubernetes I/O,
and provider parse paths remain free of Airflow Connection, Kubernetes, Vault,
database, and network access.

## Automated checks

| Check | Result | Evidence |
|---|---|---|
| Attempt-isolation and provider runtime tests | PASS | `28` tests collected; focused pytest exited `0` |
| Full non-live regression | PASS | `4382 passed, 472 skipped in 234.49s` |
| Airflow-focused regression | PASS | `uv run pytest -k airflow -q` exited `0` |
| Change-aware validation selection | PASS | Airflow/provider, Python, docs, compatibility, packaging and workflow-security gates selected |
| Ruff lint | PASS | `uv run ruff check .` exited `0` |
| Ruff formatting | PASS | `uv run ruff format --check .` exited `0` |
| Mypy | PASS | No issues in `495` source files |
| Import rules | PASS | No architectural import violations |
| Architecture fitness | PASS | No new dependency-policy violations |
| Layer metrics | PASS | Existing graph budgets preserved |
| Module-size budget | PASS | New modules and changed provider modules remain within hard limits |
| Documentation contracts | PASS | `dpone docs check-docs` exited `0` |
| Documentation language tests | PASS | `tests/test_docs_language_contracts.py` exited `0` |
| Generated references | PASS | Generated references are synchronized |
| Compatibility registry | PASS | Compatibility documentation and registry are synchronized |
| Strict MkDocs build | PASS | Site built with `--strict` |
| Workflow security | PASS | Workflow policy check exited `0` |
| Root package build | PASS | `dpone-0.72.2` sdist and wheel built |
| Airflow provider adapter build | PASS | `dpone_airflow_pack-0.72.2` sdist and wheel built |
| Native acceleration build | PASS | `dpone_native_accel-0.72.2` sdist and wheel built |
| Distribution metadata | PASS | Twine accepted all six artifacts |
| Isolated wheel import smoke | PASS | Python 3.12 environment imported the built wheel |

## Security and concurrency evidence

- The attempt identity includes all five supported Airflow identity fields.
- Missing, empty, boolean, or otherwise invalid identity fields fail before
  credential and Kubernetes adapters are called.
- Different DAG runs, tasks, retries, and map indexes derive different physical
  Secret names while retaining one stable logical pod volume slot.
- A thread-barrier concurrency test proves overlapping attempts publish, mount,
  and delete distinct Secret objects.
- Reusing the same operator instance with dictionary and object-style pod
  representations replaces the stable logical slot instead of accumulating
  stale attempt volumes or mounts.
- Secrets are created with `immutable: true`; the default projector never calls
  Kubernetes replace on conflict.
- Create `409` maps to a typed conflict. Create/delete failures expose only the
  operation, numeric status, and digest reference; raw Kubernetes responses,
  namespaces, physical names, task IDs, and credential material are redacted.
- Delete `404` is idempotent success. Pod execution failure still triggers exact
  attempt cleanup under `after_execute`.
- Operator attributes retain only `secret_ref`, `attempt_ref`, and cleanup
  policy; `stringData` and Connection URIs are absent.

## Fresh-context review

The first independent architecture/security review found two lifecycle defects
when one operator instance was reused: dictionary pod specs retained an earlier
attempt volume, and object-style fallback entries could do the same. Both paths
now use one stable logical volume key and replace that slot. Regression tests
cover both representations.

The final fresh-context architecture review reported no remaining actionable
correctness, architecture, compatibility, or secret-leakage findings. The
documentation/test review found only this missing validation artifact and one
prose defect; both were corrected before integration.

## Public contract and compatibility

- Existing compact-pack schemas and configured `secret_name` values remain
  valid; the configured name is interpreted as a human-readable base prefix.
- `AirflowConnectionSecretProjector.upsert` remains source-compatible but now
  has create-only semantics. Custom projector implementations must stop
  replacing an existing Secret on `409`.
- The legacy `airflow_connection_projected_secret` attribute remains for one
  compatibility cycle, but now contains only safe references.
- Deferrable tasks still require `cleanup_policy: retain`; this slice does not
  claim deferrable callback cleanup.
- No CLI, manifest schema, release/deployment identity, or beginner-path change
  is introduced.

## Documentation and CJM impact

Provider API, self-service architecture, Phase 1C backlog, compatibility guide,
GitOps/Airflow runbook, and changelog now document attempt ownership,
create-only conflict behavior, safe diagnostics, custom-projector migration,
and operational cleanup. The beginner journey is unchanged: isolation happens
automatically and requires no new user action or Airflow Python.

## Live certification

`UNVERIFIED`: no explicitly approved live Airflow/Kubernetes environment was
available for this validation. No live-cluster pass is claimed. Production
certification must execute overlapping DAG runs, a mapped task, and a retry on
the exact frozen commit, then verify distinct Secret UIDs, exact pod references,
eventual cleanup, and absence of credential values from Airflow/Kubernetes logs.

Vault, MSSQL, and ClickHouse live execution is `N/A` for this provider-lifecycle
slice because the changed behavior ends at task-attempt Secret publication,
mounting, and cleanup.

## Residual risk and readiness

The implementation is ready for code review and merge. Remaining Phase 1C work
is platform-managed garbage collection for retained/cleanup-failed Secrets,
cache recovery, broader operator diagnostics, and live matrix certification.
Those items do not weaken synchronous attempt isolation proved here, but they
remain required before the full Phase 1C production claim can be closed.
