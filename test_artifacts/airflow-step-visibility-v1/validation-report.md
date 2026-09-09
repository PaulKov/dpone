# Airflow step visibility v1 validation

Validated on 2026-07-16 from the working tree based on commit `649b2c36`.

## Result

- Implementation: PASS
- Compatibility: PASS
- Documentation and CJM: PASS
- Scheduler-static benchmark: PASS
- Live Airflow/Kubernetes MSSQL to ClickHouse certification: UNVERIFIED

The live route was not executed because this task had no explicitly approved
live environment or credentials. It is not counted as a pass.

## Contract evidence

- Newly produced DAG-spec nodes publish an explicit selector, visibility,
  pack reference, and visible-task estimate.
- Compact schema-v3 packs publish immutable selector-indexed process plans.
- Internal batch dependencies are preserved from manifest compilation through
  compact-pack metadata and self-service preview edges.
- Selected incompatible packs fail closed; selector-less packs and
  `DponeTaskGroup.from_pack` preserve whole-pack compatibility.
- Inline outcome validation covers synchronous `execute` and deferrable
  `trigger_reentry`; the exact-Airflow matrix contract test verifies the
  provider callback returns the XCom sidecar payload.
- Airflow parse remains free of network, database, secret, Variable,
  Connection, and cache-refresh calls.

## Checks

| Check | Status | Evidence |
| --- | --- | --- |
| Change-aware check selection | PASS | `tools/agent_policy/select_checks.py --base-ref origin/master` |
| Focused visibility/provider/schema tests | PASS | 93 tests |
| Full Airflow selection | PASS | `pytest -k airflow -q` |
| Full non-live suite | PASS | 4874 passed, 473 skipped |
| Ruff lint and format | PASS | 3164 files formatted |
| mypy | PASS | 533 source files |
| Import rules | PASS | no violations |
| Layer metrics | PASS | cross-layer ratio 0.300 |
| Architecture fitness | PASS | average clustering 0.179, budget 0.180 |
| Module size | PASS | no new hard-limit violations |
| Docs and generated references | PASS | 497 Markdown files, 1808 local links, 2/2 references |
| MkDocs strict build | PASS | site built successfully |
| Compatibility policy | PASS | 19 registry entries, docs in sync |
| Core, native accel, Airflow pack builds | PASS | sdists and wheels built |
| Twine package validation | PASS | all distributions passed |
| Scheduler-static benchmark | PASS | 100 DAGs, 500 nodes, 30 repetitions, p95 4.635 ms |

## Review disposition

A fresh-context review identified three concerns. Complete-pack escape-hatch
compatibility and intra-workload preview dependencies were corrected. The
deferrable KPO concern was checked against the official provider 10.1.0 and
10.5.0 source: `trigger_reentry` returns the extracted XCom sidecar value in
both versions. A real-Airflow contract test now guards this behavior in the
existing compatibility matrix.

## Remaining risk

Exact scheduler/runtime behavior in a live Airflow/Kubernetes deployment and
the MSSQL to ClickHouse route remain UNVERIFIED for this slice. Those checks
require an approved environment and must be recorded separately before making
route-certification claims.
