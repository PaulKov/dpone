# Final remediation check receipts

Evaluated source: `7c25ce910ff4e3a929b46aa01237840ae92ef512`. Every receipt
records unchanged source identity and a verified raw-log hash. Integrity PASS
does not relabel the local full-suite timeout as successful.

| Check | Status | Seconds | Evidence |
|---|---|---:|---|
| `remediation-retry-airflow-contracts` | **PASS** | 2.58 | [receipt](remediation-retry-airflow-contracts.json) / [log](remediation-retry-airflow-contracts.log) |
| `remediation-retry-architecture-tests` | **PASS** | 96.277 | [receipt](remediation-retry-architecture-tests.json) / [log](remediation-retry-architecture-tests.log) |
| `remediation-retry-architecture` | **PASS** | 23.953 | [receipt](remediation-retry-architecture.json) / [log](remediation-retry-architecture.log) |
| `remediation-retry-authority-regressions` | **PASS** | 8.567 | [receipt](remediation-retry-authority-regressions.json) / [log](remediation-retry-authority-regressions.log) |
| `remediation-retry-build-airflow-pack` | **PASS** | 0.675 | [receipt](remediation-retry-build-airflow-pack.json) / [log](remediation-retry-build-airflow-pack.log) |
| `remediation-retry-build-airflow-provider` | **PASS** | 0.577 | [receipt](remediation-retry-build-airflow-provider.json) / [log](remediation-retry-build-airflow-provider.log) |
| `remediation-retry-compatibility` | **PASS** | 1.861 | [receipt](remediation-retry-compatibility.json) / [log](remediation-retry-compatibility.log) |
| `remediation-retry-docs` | **PASS** | 3.417 | [receipt](remediation-retry-docs.json) / [log](remediation-retry-docs.log) |
| `remediation-retry-format` | **PASS** | 0.58 | [receipt](remediation-retry-format.json) / [log](remediation-retry-format.log) |
| `remediation-retry-full-nonlive` | **FAIL** | 2160.325 | [receipt](remediation-retry-full-nonlive.json) / [log](remediation-retry-full-nonlive.log) |
| `remediation-retry-hosted-head` | **PASS** | 19.965 | [receipt](remediation-retry-hosted-head.json) / [log](remediation-retry-hosted-head.log) |
| `remediation-retry-hosted-required` | **PASS** | 19.283 | [receipt](remediation-retry-hosted-required.json) / [log](remediation-retry-hosted-required.log) |
| `remediation-retry-imports` | **PASS** | 11.871 | [receipt](remediation-retry-imports.json) / [log](remediation-retry-imports.log) |
| `remediation-retry-language` | **PASS** | 5.907 | [receipt](remediation-retry-language.json) / [log](remediation-retry-language.log) |
| `remediation-retry-layers` | **PASS** | 15.959 | [receipt](remediation-retry-layers.json) / [log](remediation-retry-layers.log) |
| `remediation-retry-metrics-freshness` | **PASS** | 11.951 | [receipt](remediation-retry-metrics-freshness.json) / [log](remediation-retry-metrics-freshness.log) |
| `remediation-retry-mkdocs` | **PASS** | 44.974 | [receipt](remediation-retry-mkdocs.json) / [log](remediation-retry-mkdocs.log) |
| `remediation-retry-modules` | **PASS** | 6.625 | [receipt](remediation-retry-modules.json) / [log](remediation-retry-modules.log) |
| `remediation-retry-mypy` | **PASS** | 3.071 | [receipt](remediation-retry-mypy.json) / [log](remediation-retry-mypy.log) |
| `remediation-retry-native-focused` | **PASS** | 100.2 | [receipt](remediation-retry-native-focused.json) / [log](remediation-retry-native-focused.log) |
| `remediation-retry-ownership` | **PASS** | 6.682 | [receipt](remediation-retry-ownership.json) / [log](remediation-retry-ownership.log) |
| `remediation-retry-package-metadata` | **PASS** | 1.528 | [receipt](remediation-retry-package-metadata.json) / [log](remediation-retry-package-metadata.log) |
| `remediation-retry-references` | **PASS** | 3.064 | [receipt](remediation-retry-references.json) / [log](remediation-retry-references.log) |
| `remediation-retry-ruff` | **PASS** | 0.675 | [receipt](remediation-retry-ruff.json) / [log](remediation-retry-ruff.log) |
| `remediation-retry-selection` | **PASS** | 0.741 | [receipt](remediation-retry-selection.json) / [log](remediation-retry-selection.log) |
| `remediation-dbt-replay` | **PASS** | 19.268 | [receipt](remediation-dbt-replay.json) / [log](remediation-dbt-replay.log) |

The local full suite recorded **21,271 passed, one failed, zero errors and
570 skipped**. Its sole failure is the existing dbt demo subprocess 120-second
timeout. The unchanged full 93-test dbt module passes in a quiet focused replay.
All 21 required GitHub checks and the complete 16-shard CI population pass on the same
source. See [local analysis](remediation-retry-full-analysis.json),
[CI population audit](remediation-ci-7c25ce9/full-population-audit.json) and
[receipt integrity audit](remediation-receipt-audit.json).

Live SQL/BCP/performance remains **SKIP / UNVERIFIED**. The host was initially
shared with unrelated full suites; elapsed time is not performance evidence.
