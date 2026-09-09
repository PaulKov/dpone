# dbt runtime correctness remediation v1 validation

- Status: PASS for non-live implementation gates
- Date: 2026-07-28
- Branch: `codex/dbt-airflow-self-service-v1`
- Code commit: `dd9b51db0afbf7d1bded0ab7175ab29bee359a71`
- Code tree: `1f0dbe281142f0d89c6b25c551514bd7e511f333`
- Integrated base: `origin/master@53ec0b4f` (`v0.73.22`)
- Specification:
  `docs/feature-design-dbt-runtime-correctness-remediation-v1.md`

This report binds validation to the code commit above. The later evidence-only
commit changes this report and the specification status; it does not change
production code.

## Implemented contract

- Selection locks contain the complete graph and the distinct exact
  `run_results.json` expectation set. Ephemeral models remain in graph identity
  but not result identity; selected unit tests gate transfer.
- `quality.dbt_warning_policy` is platform-owned, defaults to `fail`, treats
  `no-op` as success, and rejects warning, skipped, partial, failed, or unknown
  outcomes according to the approved matrix.
- `run_results.json` v6 is validated offline against the vendored official
  schema before dpone identity and outcome checks.
- POSIX timeout terminates the complete process group. Output collectors have a
  shared deadline; unsupported non-file streams fail closed before a reader
  thread starts.
- Package declarations require a current dbt-compatible lock and resolved
  package tree. Standalone `env_var(...)` values are injected only for lock
  verification and are not persisted.
- Immutable pack-derived warning, toolchain, schema, selection, and evidence
  identities cannot be weakened by promoted dev evidence.
- The three dbt service-to-GitOps imports introduced by the branch were moved
  behind narrow readiness adapters; the merged layer budget remains green.

## Executed checks

| Check | Result | Evidence |
|---|---|---|
| Focused dbt remediation tests | PASS | The repository's canonical selection test is `tests/test_dbt_selection_resolution.py`; it replaces the stale plan filename `test_dbt_workflow_selection.py`. Selection, runtime, bundle and release E2E suite passed. |
| Extended dbt runtime/schema/evidence suite | PASS | Selection, runtime, project bundle, release E2E, prod mirror, dev evidence, official schema, process supervision, schema parity, evidence security and fixture tests passed. |
| Full non-live pytest | PASS | `8355 passed, 558 skipped, 1 warning` in 109.82 seconds. Skips were not interpreted as passes. |
| Ruff check and format | PASS | Whole repository clean. |
| mypy | PASS | `765` source files plus direct checks of both new readiness adapters. |
| Import rules | PASS | No architectural import violation. |
| Layer metrics | PASS | `6065` edges, cross ratio `0.288`, maximum cross-layer flow `99`, threshold `99`. |
| Module size | PASS | No module exceeds the hard LOC/SLOC budget. |
| Documentation checks | PASS | `656` Markdown files and `2445` local links; language contracts and generated references pass. |
| MkDocs strict build | PASS | Site built without warning/error. |
| Compatibility and Airflow public contracts | PASS | Compatibility registry is synchronized; Airflow CLI/package/Python/schema surfaces remain compatible. |
| Agent governance and workflow security | PASS | Governance artifact and workflow policy checks pass. |
| Package build | PASS | dpone, native accel, pack reader and Airflow provider `0.73.22` wheel/sdist pairs built. |
| Twine metadata | PASS | All eight exact-version artifacts pass. |
| Wheel content | PASS | Official run-results v6 schema and dbt runtime/supervision/readiness modules are present in the dpone wheel. |
| Deterministic package-lock parity | PASS | Literal and environment-rendered package hashes match dbt-core `1.10.13`; stale declarations fail closed. |
| Independent fresh-context review | PASS | Architecture, test/certification and docs/UX reviewers returned final GO/READY with no open finding. |

The first post-merge full-suite attempt found the local editable
`dpone-native-accel` distribution still at `0.73.21` while the merged source was
`0.73.22`. The workspace package was synchronized, its exact metadata contract
passed, and the complete suite was rerun successfully. No source change was
used to hide that environment mismatch.

## Certification status

| Capability | Status | Reason |
|---|---|---|
| Production activation implementation | PASS | Static workflow, immutable identity, attestation and fail-closed contract tests pass. |
| Live MSSQL to ClickHouse route | UNVERIFIED | No approved live database environment was used for this remediation. |
| Live Airflow/Kubernetes/Vault execution | UNVERIFIED | No approved live control-plane environment or credentials were used. |
| POSIX process-tree cleanup | PASS | Real parent/child process-group tests pass on macOS POSIX. Production KPO target remains Linux. |
| Windows/non-POSIX process-tree behavior | UNVERIFIED | Parent fallback and unsafe-stream tests pass, but no real Windows runner was used. |
| Cosmos coexistence sentinel | UNVERIFIED locally | The repository workflow declares two pinned probe rows; their result is accepted only from CI on this exact PR head. |
| Full Cosmos graph integration | N/A | Explicitly outside v1; Cosmos is not dpone topology or runtime authority. |
| Wider Airflow/Cosmos matrix | UNVERIFIED | Only exact CI rows may become PASS. |
| Owner attestation | UNVERIFIED / manual | The maintainer must review the final diff and check the PR owner-attestation boxes after technical CI is green. |

## Residual risk

- The layer flow is at its allowed threshold (`99`); future service-to-GitOps
  imports must use a contract/port/readiness boundary or reduce existing debt.
- The single full-suite warning is Python's documented macOS warning about
  `fork()` from a multithreaded test process; no test failed or hung.
- Live certification, CI Cosmos rows and owner attestation remain external
  merge gates. None is reported as PASS in this local evidence.

## Readiness

The exact code commit is ready for PR CI and maintainer review. Merge remains
blocked until required GitHub checks are green and the owner attestation is
performed manually.
