# Airflow hermetic tests v1 validation report

- Date: 2026-07-16
- Branch: `codex/airflow-self-service-roadmap`
- Base commit: `c3feaa4e`
- Scope: staged implementation of `docs/feature-design-airflow-hermetic-tests-v1.md`
- Result: `PASS` for the credential-free hermetic contract

## What was validated

The staged tree adds `dpone test`, the bounded `dpone.test.v1` contract,
deterministic in-memory execution, safe reports, scaffolded fixtures for every
supported authoring mode, public schemas, documentation, and benchmark
evidence. It does not execute connectors, credentials, Airflow, Vault,
Kubernetes, or live routes and therefore does not certify those capabilities.

## Results

| Gate | Result | Evidence |
| --- | --- | --- |
| Focused hermetic/self-service tests | PASS | `tests/test_airflow_hermetic_tests_v1.py`, folder, recipe, CLI, and schema contract tests |
| Full non-live suite | PASS | `4845 passed, 473 skipped in 262.10s` |
| Ruff lint and format | PASS | all checks passed; 3155 files formatted |
| Mypy | PASS | no issues in 532 source files |
| Import rules | PASS | no architectural import violations |
| Layer metrics | PASS | cross-layer ratio `0.300`; baseline delta `+0.002` |
| Module-size budget | PASS | no hard-limit violations or allowlisted debt |
| Documentation | PASS | 496 Markdown files and 1808 local links checked |
| Generated references | PASS | 2/2 references in sync |
| Compatibility registry | PASS | 19 entries in sync |
| MkDocs strict build | PASS | completed without warnings or errors |
| Packaging | PASS | `dpone`, `dpone-native-accel`, and `dpone-airflow-pack` sdist/wheel builds; all six artifacts passed `twine check` |
| Fresh-context review | PASS | final staged-diff review returned `NO FINDINGS` |
| Live connector/route checks | N/A | the feature is explicitly credential-free and does not claim route certification |
| Native Windows runtime | UNVERIFIED | Windows confinement is covered by unit contracts; no native Windows runner was available |

## Benchmark

The committed `benchmark.json` is produced by
`tools/airflow_hermetic_tests_benchmark.py` on macOS 15.3 arm64, Python 3.11.14,
12 logical CPUs. It executes 100 two-row hermetic tests for 20 runs.

| Metric | Result | Budget |
| --- | ---: | ---: |
| p50 | 0.613 s | informational |
| p95 | 0.726 s | <= 5.000 s |
| Peak RSS | 27.5 MiB | informational |
| Stable test IDs | 100/100 | 100/100 |

## Review history and residual risk

An earlier fresh-context review found two safety issues: a concurrent
first-create report race treated an equivalent winner as fatal, and an
unreadable individual input was classified as missing. Both were fixed and
covered by regression tests. The final fresh-context review confirmed the
fixes and reported no findings.

Residual risk is limited to native Windows filesystem behavior and the normal
gap between hermetic semantics and live connector execution. Neither is
reported as passed. The unrelated untracked `.cursor/` directory was excluded
from the diff and all evidence.
