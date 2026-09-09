# dbt MSSQL adapter guardrails v1 completion report

## 1. What changed and why

The PR closes the remaining SQL Server and ClickHouse staged-finalization
safety gaps in the approved self-service preview:

- exact SQL Server macro, graph, merge-key, eager-test, runtime, and evidence
  authority remains fail-closed;
- ClickHouse validation tokens are opaque, sink-owned, immutable to callers,
  single-use, and retired with attempt staging;
- validation snapshots keep injected live source connectors identity-bound
  while freezing every surrounding declarative option and staged handle;
- every staged finalizer invocation is treated as target-invoked on exception,
  including sinks without the optional validator;
- nested partial finalization retains reconciliation staging and publishes
  exact finalized/retained member evidence;
- cleanup failure after a confirmed commit is explicitly non-retryable.

These changes prevent false safe retry, staging destruction after an uncertain
target outcome, copied-token replay, and duplicate append after post-commit
cleanup failure.

## 2. Public contract and compatibility impact

Released successful staged-load flows remain compatible. Sinks without optional
validation still use their handle-based positive path, and cleanup-only test or
adapter helpers do not need to implement token retirement.

Failure behavior is intentionally stricter: once any staged finalizer is
invoked, an exception is not treated as proof of rollback. Nested callers
receive `nested_package_partial_finalize`; confirmed target cleanup failures
receive `staged_cleanup_failed` with `target_outcome: committed`. Both expose
`safe_to_retry: false`.

The corrected dbt contracts are unreleased preview contracts. Older
branch-local locks, packs, releases, deployments, and evidence must be
regenerated from source into a new immutable output directory.

## 3. Tests and checks

- PASS: exact full non-live suite, `8782 passed, 558 skipped, 1 warning`.
- PASS: 87 integrated lifecycle tests plus fresh independent direct/governed
  uncopyable-client, semantic-drift, replay, and binding probes.
- PASS: original macro/key/eager matrix, 289 tests.
- PASS: recovery/docs review, 125 focused tests.
- PASS: Ruff, formatting, mypy (781 source files), import rules, layer metrics,
  module size, and architecture fitness.
- PASS: docs (663 files / 2,488 links), generated references, producer-exact
  quality metrics, compatibility, Airflow public contracts, language
  contracts, and strict MkDocs build.
- PASS: governance setup/receipt, task contract, branch-protection policy,
  workflow security, and agent-policy test suite.
- PASS: four `0.73.24` package builds and Twine metadata for all eight
  wheel/sdist artifacts.
- PASS: architecture and certification reviews on exact code commit
  `301a5e7b5ab51e765ba492eab5d2bfa570604354`, tree
  `80654a855d9b1128412e2c7048a2e3c1c3821798`; docs/UX re-review on exact
  generated-doc commit `241862735343f6b9789c5a5a3893b9186c78a06c`,
  tree `f1fdbf234116e6190e69d76c8c3135c6eb38a91d`.
- UNVERIFIED: live MSSQL, ClickHouse, Airflow, Kubernetes, Vault, and guaranteed
  post-build evidence reserve.

## 4. Evidence and artifact paths

- `test_artifacts/dbt-mssql-adapter-guardrails-v1/validation-report.md`
- `test_artifacts/dbt-mssql-adapter-guardrails-v1/completion-report.md`
- `test_artifacts/dbt-mssql-adapter-guardrails-v1/task-contract.yml`
- `test_artifacts/agent-policy/agent_governance_gate.json`
- `docs/feature-design-dbt-mssql-adapter-guardrails-v1.md`
- Local exact-run JUnit:
  `/private/tmp/dpone-pr454-validation-snapshot-junit.xml`
- Local package artifacts:
  `/private/tmp/dpone-pr454-packaging.5LdQ5X`

## 5. Documentation and CJM impact

The ClickHouse guide, error catalog, runbook, feature specification, ADR,
changelog, and generated quality dashboard now agree on:

- pre-target validation and exact attempt-table cleanup proof;
- `COMMIT_UNKNOWN` staging retention and no automatic retry;
- nested finalized/retained members and operation-table identities;
- `staged_cleanup_failed` as a confirmed commit, not a retryable load failure;
- identity-bound live source clients versus frozen declarative validation
  semantics;
- single-node versus all-replica verification, reviewed `ON CLUSTER`, and the
  prohibition on guessed clusters, wildcards, or business-target cleanup.

The recovery journey tells an operator when to freeze retries, which evidence
to retain, how to reconcile each load strategy, and when exact staging cleanup
or a new attempt is allowed.

## 6. Remaining risk and follow-up

Live database and control-plane failure injection remains a release blocker,
not a local PASS. GitHub CI must run on the final pushed head. Average
clustering is at the 0.180 ceiling, and warning-level module debt remains
bounded by the current baseline.

## 7. Readiness

The implementation and retained non-live evidence are ready for final PR CI and
maintainer review. The result is merge-ready as a preview after the final
pushed head has green required checks. It is not ready for production release
certification until the explicitly unverified live gates pass.
