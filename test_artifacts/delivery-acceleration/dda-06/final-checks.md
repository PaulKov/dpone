# Final validation receipts

Evaluated source: `be7655ac2e36cd1e9601217c6b0a1fbd10e2eb3f`. Each completed receipt below records identical
before/after source identity and a verified raw-log hash. Command labels are
shortened for readability; each linked JSON contains the exact argument vector.
Integrity PASS is separate from test or architecture acceptance.

| Command | Status | Observed result | Seconds | Evidence |
|---|---|---|---:|---|
| `pytest: 21 native/producer files` | **PASS** | 490 tests; 0 failures; 0 skipped | 169.3 | [receipt](final-native-focused.json) / [log](final-native-focused.log) |
| `pytest: 8 lease/schema/authority files` | **PASS** | 296 tests; 0 failures; 0 skipped | 10.456 | [receipt](final-authority-regressions.json) / [log](final-authority-regressions.log) |
| `pytest: complete non-live suite, two workers` | **FAIL** | 21,240 passed; 2 clustering failures; 0 errors; 570 skipped | 1262.844 | [receipt](final-full-nonlive.json) / [log](final-full-nonlive.log) |
| `ruff check .` | **PASS** | Completed; exact command and raw output in receipt | 1.405 | [receipt](final-ruff.json) / [log](final-ruff.log) |
| `ruff format --check .` | **PASS** | Completed; exact command and raw output in receipt | 1.391 | [receipt](final-format.json) / [log](final-format.log) |
| `mypy --config-file mypy.ini` | **PASS** | Completed; exact command and raw output in receipt | 8.333 | [receipt](final-mypy.json) / [log](final-mypy.log) |
| `dpone docs check-import-rules` | **PASS** | Completed; exact command and raw output in receipt | 22.446 | [receipt](final-imports.json) / [log](final-imports.log) |
| `dpone docs check-module-size (exact base/head)` | **PASS** | Completed; exact command and raw output in receipt | 15.347 | [receipt](final-modules.json) / [log](final-modules.log) |
| `dpone docs check-layer-metrics` | **FAIL** | FAIL: runtime→contracts flow 215 > 214 | 21.177 | [receipt](final-layers.json) / [log](final-layers.log) |
| `dpone docs check-architecture-fitness` | **FAIL** | FAIL: clustering 0.18322547520065463 > 0.182 | 40.724 | [receipt](final-architecture.json) / [log](final-architecture.log) |
| `agent_policy/select_checks.py` | **PASS** | Completed; exact command and raw output in receipt | 1.944 | [receipt](final-selection.json) / [log](final-selection.log) |
| `dda-06/audit_ownership.py` | **PASS** | Completed; exact command and raw output in receipt | 18.398 | [receipt](final-ownership.json) / [log](final-ownership.log) |
| `dpone docs check-docs` | **PASS** | Completed; exact command and raw output in receipt | 11.735 | [receipt](final-docs.json) / [log](final-docs.log) |
| `dpone docs check-generated-references` | **PASS** | Completed; exact command and raw output in receipt | 4.689 | [receipt](final-references.json) / [log](final-references.log) |
| `pytest: docs language contracts` | **PASS** | Completed; exact command and raw output in receipt | 11.12 | [receipt](final-language.json) / [log](final-language.log) |
| `mkdocs build --strict` | **PASS** | Completed; exact command and raw output in receipt | 54.958 | [receipt](final-mkdocs.json) / [log](final-mkdocs.log) |
| `dpone docs update-dev-metrics --check` | **PASS** | Completed; exact command and raw output in receipt | 24.948 | [receipt](final-metrics-freshness.json) / [log](final-metrics-freshness.log) |
| `dpone docs check-airflow-public-contracts` | **PASS** | Completed; exact command and raw output in receipt | 5.433 | [receipt](final-airflow-contracts.json) / [log](final-airflow-contracts.log) |
| `uv build: dpone-airflow-pack` | **PASS** | Completed; exact command and raw output in receipt | 2.222 | [receipt](final-build-airflow-pack.json) / [log](final-build-airflow-pack.log) |
| `uv build: apache-airflow-providers-dpone` | **PASS** | Completed; exact command and raw output in receipt | 2.357 | [receipt](final-build-airflow-provider.json) / [log](final-build-airflow-provider.log) |
| `twine check: four fresh Airflow distributions` | **PASS** | Completed; exact command and raw output in receipt | 2.537 | [receipt](final-package-metadata.json) / [log](final-package-metadata.log) |

The required full non-live suite completed after DDA-05 released the exclusive
CPU slot. Its two failed tests both assert the same clustering value above the
0.182 checker limit. The table records wrapper duration; pytest reports 1,257.00
seconds and JUnit records 1,256.479 seconds. The separately executed layer gate
fails on runtime-to-contracts flow 215 versus 214. No non-architecture test
failures or collection errors were recorded. See the retained
[JUnit](final-full-nonlive-junit.xml) and [failure/skip analysis](full-suite-analysis.json).
All 570 skips retain their actual reasons and are not promoted to passes.
Live SQL/BCP/performance checks are
**SKIP / UNVERIFIED** because no disposable environment is approved.

Architecture **HOLD** remains open; the generated metrics and passing focused,
type, packaging and documentation checks do not clear it.
