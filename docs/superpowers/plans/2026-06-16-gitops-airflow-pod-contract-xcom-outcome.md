# GitOps Airflow Pod Contract And XCom Outcome Plan

> **Historical design record.** Commands below are preserved as implementation evidence and are not current runbooks. Use [Module-size debt ratchet](../../module-size-ratchet.md) for the supported exact-SHA command.

## Goal

Add a production-grade pod contract slice for custom dpone Airflow images running through KubernetesPodExecutor or KubernetesPodOperator, plus a final XCom outcome written by `run-spec-exec`.

## Scope

- Add `dpone gitops airflow pod-contract` to build a stable JSON contract, Kubernetes pod spec, and KubernetesPodOperator kwargs from an existing bundle, run-spec, and runtime profile.
- Add `dpone gitops airflow pod-doctor` to validate the generated pod contract offline before an Airflow deployment consumes it.
- Extend `dpone gitops airflow run-spec-exec` so every execution writes a final XCom summary with status, failed step, evidence digest, and step counts.
- Add public JSON schemas for pod contract, pod doctor, and XCom summary.
- Update user docs, developer docs, architecture docs, generated CLI reference, and quality metrics.

## Design

- Keep CLI modules limited to parser registration and service invocation.
- Keep service modules as orchestration only: path validation, artifact loading, artifact writing, and view construction.
- Put Kubernetes/Airflow contract construction in pure GitOps domain builders.
- Put validation rules in a small pod doctor component that consumes generic mappings and emits reusable `GitOpsIssue` records.
- Do not import Airflow or Kubernetes packages; all outputs remain static handoff artifacts for CI and GitOps.

## Test First Plan

- Add domain/service tests for pod contract rendering, KPO kwargs, pod spec invariants, and doctor blockers.
- Add run-spec-exec tests for successful and failed final XCom outcome files.
- Add CLI tests for the two new commands and the new `--xcom-output` flag.
- Add schema contract tests for the three new public schemas.
- Add docs contract tests requiring user docs, developer docs, CLI reference, architecture mention, and schema links.

## Verification

- `uv run pytest tests/test_gitops_airflow_runner_pack.py tests/test_cli_gitops_airflow_commands.py tests/test_gitops_schema_contracts.py tests/test_gitops_airflow_docs_contract.py -q`
- `uv run ruff check .`
- `uv run ruff format --check .`
- `uv run mypy --config-file mypy.ini`
- `uv run dpone docs update-cli-reference --check`
- `uv run dpone docs update-dev-metrics --check`
- `uv run dpone docs check-import-rules`
- `uv run dpone docs check-layer-metrics --baseline docs/layer_metrics_baseline.json`
- `uv run dpone docs check-module-size --baseline docs/module_size_baseline.json`
- `uv run pytest -m "not integration_live" --cov=src/dpone --cov-report=xml`
- `uv run mkdocs build --strict`
