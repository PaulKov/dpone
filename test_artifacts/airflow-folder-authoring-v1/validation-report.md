# Bounded folder authoring v1 validation report

- Date: 2026-07-16
- Branch: `codex/airflow-self-service-roadmap`
- Base commit: `30e95886`
- Scope: local build-plane authoring, compact-pack dependency integrity, and
  pre-I/O safe-sample pin verification

## Contract evidence

| Contract | Status | Evidence |
| --- | --- | --- |
| Explicit folder root and fragment schema | PASS | `tests/test_airflow_folder_authoring_v1.py` |
| Classic/flow/folder semantic equivalence | PASS | focused authoring tests |
| No undeclared directory scan | PASS | malformed orphan is ignored by the explicit-list test |
| Traversal, backslash, symlink, alias, and resource limits | PASS | negative folder-loader tests |
| 100-fragment upper bound | PASS | bounded 100-fragment contract test |
| Fragment-local `sql_file` source mapping | PASS | normalized path and pack dependency test |
| Compile-to-pack TOCTOU parity | PASS | changed-fragment and changed-SQL build blocker tests |
| SQL parity in classic, flow, and folder modes | PASS | compiler pin and mutation matrix tests |
| Runtime folder dependency pinning | PASS | exact and changed fragment graph tests |
| Atomic multi-file scaffold conflict | PASS | no partial authoring/catalog writes test |
| Airflow parse-side effects | PASS | provider input remains generated index/spec/pack only; full non-live regression suite |
| Live connector certification | N/A | this feature performs no connector, Vault, Kubernetes, or Airflow runtime I/O |

## Validation commands

| Command | Result |
| --- | --- |
| Focused authoring/self-service/pinning/runtime/pack tests | PASS |
| `uv run ruff check .` | PASS |
| `uv run ruff format --check .` | PASS |
| `uv run mypy --config-file mypy.ini` | PASS: 509 source files |
| `uv run dpone docs check-import-rules` | PASS |
| `uv run dpone docs check-layer-metrics --baseline docs/layer_metrics_baseline.json` | PASS |
| `uv run dpone docs check-module-size --baseline docs/module_size_baseline.json` | PASS |
| Architecture fitness hard gates | PASS: clustering `0.1797533629`, cross-layer ratio `0.2999146758` |
| `uv run pytest -m "not integration_live" -n auto --dist loadfile` | PASS: 4,652 passed, 473 skipped in 209.20s |
| `uv run dpone docs check-docs` | PASS: 453 Markdown files, 1,766 local links |
| `uv run dpone docs check-generated-references` | PASS |
| `uv run pytest tests/test_docs_language_contracts.py -q` | PASS: 4 passed |
| `uv run mkdocs build --strict` | PASS |
| Main, native-accel, and Airflow-provider package builds | PASS |
| `twine check` for six wheel/sdist artifacts | PASS |
| Main wheel contains both flow JSON Schemas | PASS |

## Review evidence

The fresh docs/UX review found compatibility text drift, missing authoring error
pages, absent `docs_url` values, and weak advanced-mode framing. The first fresh
architecture/security review found descriptor-race, SQL-pin, preview-error, and
provenance gaps. All findings were corrected. The final fresh review returned
`APPROVE` after exact `kind/path/sha256` parity covered both fragments and SQL;
the suggested classic/flow mutation matrix was also added before integration.

## Residual limitations

- Human usability testing is roadmap-level Phase 1/2 evidence and is not
  replaced by this hermetic feature report.
- Live MSSQL/ClickHouse, Vault, Kubernetes, and Airflow certification remains
  governed by their independent route/runtime profiles; no live claim is made
  here.
