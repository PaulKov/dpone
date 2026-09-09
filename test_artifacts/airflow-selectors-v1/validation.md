# Airflow selectors v1 validation

- Commit: working tree for the integrating pull request
- Date: 2026-07-16
- Environment: macOS 15.3 arm64, Python 3.11.14
- Specification: `docs/feature-design-airflow-selectors-v1.md`
- Task contract: `test_artifacts/agent-policy/airflow-selectors-v1.yml`

| Check | Status | Result |
|---|---|---|
| Focused selector, run, self-service CLI, and GitOps schema tests | PASS | 156 passed |
| Selector plus architecture fitness tests | PASS | 89 passed |
| Ruff lint and format | PASS | 3142 files checked |
| Mypy | PASS | 523 source files checked |
| Import rules | PASS | No violations |
| Layer metrics | PASS | Cross-layer ratio 0.299; no regression issue |
| Module size | PASS | No hard-limit violations |
| Architecture fitness | PASS | Clustering 0.180; cross-layer ratio 0.299 |
| Generated references | PASS | CLI and GitOps schema references in sync |
| Documentation links and language contracts | PASS | 491 Markdown files and 1795 local links checked |
| MkDocs strict build | PASS | Site built without warnings treated as errors |
| Airflow provider package | PASS | Wheel and sdist built; `twine check` passed |
| Full non-live pytest | PASS | 4745 passed, 473 skipped |
| Selector benchmark | PASS | 500 nodes, 1000 edges, 100 runs; p95 133.183 ms <= 150 ms |
| Live connector certification | N/A | Build-plane selection adds no connector route |

The fresh-context architecture review identified selector validation parity,
release identity, process selection, edge pruning, dependency direction,
bounded input, TOCTOU, state parsing, target confinement, and explanation-path
risks. Each finding is covered by a focused regression test in
`tests/test_airflow_selectors_v1.py` and passed before this report was written.

Residual risk is limited to workload metadata quality in user-maintained domain
catalogs. Invalid, ambiguous, over-budget, cyclic, or drifting inputs fail closed
before release materialization.
