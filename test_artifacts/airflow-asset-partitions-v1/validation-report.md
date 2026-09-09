# Airflow asset partitions v1 validation

- Commit under test: uncommitted Phase 3 slice on
  `codex/airflow-self-service-roadmap`
- Base contract: `docs/feature-design-airflow-asset-partitions-v1.md`
- Date: 2026-07-16
- Overall local status: PASS with explicitly UNVERIFIED external evidence

## Contract evidence

| Gate | Status | Observed result |
| --- | --- | --- |
| Focused asset/build/provider/runtime/schema suites | PASS | 224 tests passed in the combined contract run; the dedicated asset suite has 22 passing tests |
| Full non-live regression | PASS | 4932 passed, 475 skipped in 283.46 seconds on the final local diff |
| Ruff | PASS | all repository checks passed; 3201 files formatted |
| Mypy | PASS | 576 source files, no issues |
| Import rules | PASS | no architectural import violations |
| Layer metrics | PASS | cross-layer ratio 0.299; regression within budget |
| Module size | PASS | no hard-limit issue; partition planning split into a cohesive 173-line module |
| Architecture fitness | PASS | average clustering 0.179, below the 0.180 budget |
| Workflow security | PASS | 0 errors, 0 warnings |
| Parse SLO | PASS | 100 DAG / 500 workload contract completed in 1.28 seconds in the local test run; built-in cold/warm budgets passed |
| Generated schemas/references | PASS | 3/3 generated references synchronized; public DAG and XCom schemas passed producer equality checks |
| Documentation | PASS | 501 Markdown files and 1818 links checked; language contracts and strict MkDocs build passed |
| Packaging | PASS | `dpone`, `dpone-native-accel`, and `dpone-airflow-pack` 0.72.3 sdist/wheels built; Twine accepted all six artifacts |
| Airflow 3.2.0 / Python 3.12 | PASS | 12 exact-runtime tests passed; native timetable classes materialized and the scheduler key rendered into KPO env |
| Airflow 2.11.0 / Python 3.12 | PASS | 8 exact-runtime tests passed; existing cron/Dataset behavior degraded explicitly |
| Airflow 2.10.5 and 3.3.0 exact jobs | UNVERIFIED | workflow matrix is configured; local isolated runs were not duplicated for every CI pair |
| Live matching-partition event | UNVERIFIED | no approved Airflow/Kubernetes environment or credentials were provided |
| Fresh-context agent review | UNVERIFIED | agent spawn failed because the thread limit was reached |

## Commands

```text
uv run pytest tests/test_airflow_asset_partitions.py ... -q
uv run pytest tests/test_airflow_provider_parse_slo.py -q --durations=5
uv run pytest -m "not integration_live" -n auto --dist loadfile
uv run ruff check .
uv run ruff format --check .
uv run mypy --config-file mypy.ini
uv run dpone docs check-import-rules
uv run dpone docs check-layer-metrics --baseline docs/layer_metrics_baseline.json
uv run dpone docs check-module-size --baseline docs/module_size_baseline.json
uv run dpone docs check-architecture-fitness
uv run dpone docs check-compatibility
uv run python tools/agent_policy/workflow_security.py .
uv run dpone docs check-docs
uv run dpone docs check-generated-references
uv run pytest tests/test_docs_language_contracts.py -q
uv run mkdocs build --strict
uv build
uv build packages/dpone-native-accel
uv build packages/dpone-airflow-pack
uv tool run twine check <isolated-dist>/*
```

Exact Airflow evidence:

```text
/tmp/dpone-airflow32.TAxYeb/airflow-3.2-junit.xml
/tmp/dpone-airflow211.I6Vfer/airflow-2.11-junit.xml
```

## Compatibility and safety result

- Existing unpartitioned authoring remains byte-shape compatible: no
  `partition_plan` is emitted when no partition is declared.
- Airflow parse adds no network, database, metadata, Variable, Connection,
  Vault, Kubernetes, or cache-refresh operation.
- Inherited consumer workloads receive the DAG partition plan even when the
  compact pack intentionally contains no duplicate partition declaration.
- DAG/pack mismatch, malformed metadata, mixed contracts, missing schedule,
  invalid timezone, partial SDK capability, missing native key and unbounded
  runtime keys fail closed.
- Earlier Airflow releases cannot report native partition isolation.

## Remaining risk

The code is ready for CI and maintainer review, but not for a live production
partition-certification claim. Keep the pull request draft until the full exact
Airflow matrix and fresh human/agent review pass. A real Airflow 3.2+
producer-event-to-consumer-partition run remains the certification follow-up.
