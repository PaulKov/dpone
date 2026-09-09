# dbt MSSQL adapter guardrails v1 validation

- Status: PASS for exact non-live implementation gates
- Date: 2026-07-29
- Branch: `codex/dbt-airflow-self-service-v1`
- Code commit: `301a5e7b5ab51e765ba492eab5d2bfa570604354`
- Code tree: `80654a855d9b1128412e2c7048a2e3c1c3821798`
- Generated-doc commit: `241862735343f6b9789c5a5a3893b9186c78a06c`
- Generated-doc tree: `f1fdbf234116e6190e69d76c8c3135c6eb38a91d`
- Integrated base: `origin/master@438acdacc9d2c9ad0c5f8bc7c6ac3dabbd01357e`
- Specification:
  `docs/feature-design-dbt-mssql-adapter-guardrails-v1.md`

This report binds runtime validation to the code commit and tree above. The
generated-doc commit refreshes the producer-owned quality snapshot from that
tracked tree and removes a stale duplicate manual metric authority. The later
evidence-only commit updates this report, the completion report, and the
generated governance receipt; neither later commit changes production code,
schemas, examples, or runtime behavior.

## Implemented contract

- SQL Server execution remains pinned to the generated framework/invocation
  macro authority, strict adapter and graph policy, identifier-only
  structurally non-null merge keys, workflow-local eager tests, `pyodbc`, one
  total SQL execute attempt, and the documented timeout hierarchy.
- ClickHouse validates NULL/duplicate key safety on the exact post-lineage
  finalization table before target lookup or mutation.
- Validation passes frozen config/handle semantics through an opaque,
  sink-owned, one-shot token registry. Copied, mutated, forged, replayed,
  cross-finalizer, and retired tokens fail before target mutation.
- Validation snapshots preserve the injected live source connector by identity
  while deep-copying surrounding declarative options and staged-handle
  semantics. Native clients that reject pickling/deepcopy no longer fail the
  pre-target gate.
- Every staged finalizer invocation enters the target-invoked boundary,
  regardless of optional validator support. An exception is commit-unknown;
  nested current and pending staging is retained and automatic replay is
  blocked.
- Nested partial-finalize evidence records finalized members, retained members,
  exact operation tables, cleanup status, and `safe_to_retry: false` in the
  `load_governance_failed` step.
- A cleanup failure after confirmed target finalization is a stable
  `staged_cleanup_failed` outcome with `target_outcome: committed` and
  `safe_to_retry: false`; it cannot be mistaken for a retryable load failure.
- Pre-target validation failures preserve the primary error, attempt cleanup
  exactly once, and require single-node or all-replica absence proof before a
  new attempt.

## Executed checks

| Check | Result | Evidence |
|---|---|---|
| Full non-live pytest | PASS | `8782 passed, 558 skipped, 1 warning` in 101.41 seconds. JUnit: `/private/tmp/dpone-pr454-validation-snapshot-junit.xml`; skips were not interpreted as passes. |
| ClickHouse/nested/governance lifecycle matrix | PASS | 87 integrated lifecycle tests locally; fresh certifier additionally ran 52 authored tests, 6 explicit regression selectors, 15 compatibility tests, and adversarial identity/isolation/drift/replay probes on the exact code commit/tree. |
| Original macro/key/eager guardrail matrix | PASS | Independent certifier: 289 tests on exact code commit/tree. |
| Recovery documentation matrix | PASS | Independent docs/UX reviewer: 125 focused tests and manual token mutation/replay probe. |
| Live-client validation snapshot | PASS | Direct ClickHouse and governed receipt paths preserve an uncopyable `_source_connector` by identity while freezing declarative config/handle semantics; drift and replay fail before target mutation. |
| Token authority adversarial cases | PASS | Copy/deep-copy, mutation, forged, replayed, cross-finalizer, multiple-token retirement, foreign staging identity, and abort-before-failed-cleanup cases reject unsafe finalization. |
| Nested mutation failure cases | PASS | First/current member mutate-then-raise retains staging, blocks abort/replay, publishes exact recovery evidence, and preserves the primary error when evidence recording fails. |
| Post-commit cleanup cases | PASS | Direct and governed cleanup failures expose committed/non-retryable outcome; cleanup helpers without token-retirement capability remain compatible. |
| Ruff check and format | PASS | Whole repository clean; 4,046 files formatted. |
| mypy | PASS | No issues in 781 source files. |
| Import rules | PASS | No architectural import violations. |
| Layer metrics | PASS | 58 layers, 6,248 edges, cross-layer ratio 0.294, maximum cross-layer flow 99; within baseline allowance. |
| Module-size gate | PASS | No production module exceeds the 400-SLOC hard limit; `finalization.py` is 368 SLOC and warning count remains 58. |
| Architecture fitness | PASS | Average clustering 0.180, cross-layer ratio 0.294, maximum fan-out 21; no finding. |
| Documentation checks | PASS | 663 Markdown files and 2,488 local links; generated references 3/3; generated quality metrics current for 4,045 tracked Python files; 31 language-contract tests. |
| MkDocs strict build | PASS | Site built without warnings or errors. |
| Compatibility policy | PASS | 19 registry entries; documentation block is synchronized. |
| Airflow public contracts | PASS | CLI 21/21, package 1/1, Python 11/11, schemas 59/59 compatible. |
| Agent governance and workflow security | PASS | Generated receipt: `test_artifacts/agent-policy/agent_governance_gate.json`, bound to code commit `301a5e7b5ab51e765ba492eab5d2bfa570604354`. |
| Package builds | PASS | dpone, native accel, Airflow pack, and Airflow provider `0.73.24` wheel/sdist pairs built from the exact code tree; Twine passed all eight files in `/private/tmp/dpone-pr454-packaging.5LdQ5X`. |
| Twine metadata | PASS | All eight wheel/sdist artifacts pass. |
| Fresh architecture review | PASS | Exact `301a5e7b` review found no P0/P1; identity-bound runtime references and frozen declarative semantics are correctly separated. |
| Fresh test/certification review | PASS | GO on exact `301a5e7b`; no P0/P1 after adversarial uncopyable-client, drift, replay, and receipt-binding probes. |
| Fresh docs/UX review | PASS | Exact `24186273` re-review found no P0/P1 after producer-exact metrics regeneration and removal of stale duplicate numeric authority. |

The full-suite warning is Python's macOS warning about `fork()` from a
multithreaded test process. No test failed or hung.

## Skip accounting

The JUnit report contains exactly 558 skipped test cases. They remain SKIP,
not PASS. The skipped population is the repository's opt-in live/integration
matrix and environment-dependent collection paths, including source-to-sink
profiles, ClickHouse, MSSQL, PostgreSQL, MySQL, Kafka, Schema Registry, replay
services, real Airflow, and nested-live execution. No approved live endpoint or
credential was supplied for this task.

## Certification status

| Capability | Status | Reason |
|---|---|---|
| Non-live preview implementation | PASS | Exact code commit/tree passed focused, broad, docs, package, governance, compatibility, and independent review gates. |
| Live MSSQL timeout/retry/non-mutation injection | UNVERIFIED | No approved live SQL Server environment or credentials were used. |
| Live MSSQL-to-ClickHouse route | UNVERIFIED | No approved live database route was used. |
| Real Airflow/Kubernetes/Vault execution | UNVERIFIED | No approved live control-plane environment was used. |
| Guaranteed post-build evidence reserve | UNVERIFIED / not claimed | `P + 300` is the whole-task outer cutoff; a hard external cutoff may prevent terminal evidence publication. |
| GitHub CI on the final PR head | UNVERIFIED locally | Only GitHub checks on the pushed final commit may establish this gate. |
| Owner attestation | UNVERIFIED / manual | The maintainer must review the final diff and complete repository-required attestation. |

## Residual risk and follow-up

- Production activation remains blocked on approved live MSSQL failure
  injection and real ClickHouse/Airflow/Kubernetes execution evidence.
- A missing terminal evidence record or staged-finalizer exception after target
  invocation must be reconciled as commit-unknown; automatic retry is
  forbidden.
- Average clustering is exactly at the current 0.180 hard ceiling. The gate
  passes, but future dependency additions require care.
- Warning-level module debt remains within the checked baseline and did not
  increase in count; it is not a release certification claim.

## Readiness

The exact code and generated-doc commits are ready for PR CI and maintainer
review as a merge-ready preview implementation. It is not production- or
release-certified. Merge remains conditional on green GitHub checks for the
final pushed head and manual owner requirements.
