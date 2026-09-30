# ClickHouse Prepared Publication Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Recover one existing `PREPARED` ClickHouse full-refresh operation without source re-export or duplicate DDL, and refuse execution when a strict dispatch permit cannot be proven.

**Architecture:** A small recovery service validates the original authority and physical generation, then reuses the existing one-shot publication and reconciliation services. The authority port advertises strict permit capability; the current ReplacingMergeTree adapter explicitly refuses recovery execution, while the existing admitted KeeperMap adapter can issue a linearizable permit. A thin `dpone ops` command exposes plan/execute without separate business logic.

**Tech Stack:** Python 3, pytest, ClickHouse cluster publication ports/adapters, argparse command registry.

**Spec:** `docs/superpowers/specs/2026-09-30-clickhouse-prepared-publication-recovery-design.md`

## Global Constraints

- Only the original operation ID may recover its `PREPARED` authority; no foreign adoption or manual authority rewrite.
- `PREPARED` recovery requires a strict atomic authority permit; ReplacingMergeTree read-back is insufficient.
- A DDL ambiguity, replica mismatch, quality-evidence mismatch or unknown mutation outcome must fail closed.
- Recovery must not re-read the source or re-export candidate rows, and may issue at most one correlated DDL.
- No customer identifiers, credentials or row data in public tests, docs or evidence.
- Production authority migration and all-writer admission are separate coordinated work; this plan does not enable recovery on a legacy authority.

## Review Focus

1. DDL queue retention expired: Task 2 tests that inability to prove no prior dispatch blocks execution, not merely absence from current queue.
2. Two workers race on the same `PREPARED` version: Task 3 tests one strict permit and one DDL call.
3. Candidate UUID matches but schema digest differs: Task 2 tests rejection before mutation.
4. Authority write acknowledges but readback is unavailable: Task 3 tests unknown outcome and zero DDL calls.
5. Target changes after dry-run: Task 3 tests plan digest/version mismatch and re-preflight before CAS.

---

### Task 1: Authority capability and compatibility boundary

**Files:**
- Modify: `src/dpone/ports/clickhouse_cluster_publication.py`
- Modify: `src/dpone/runtime/sinks/clickhouse_cluster_publication_authority.py`
- Modify: `src/dpone/runtime/sinks/clickhouse_quality_authority.py`
- Test: `tests/test_clickhouse_cluster_publication_contract.py`
- Test: `tests/test_clickhouse_quality_authority.py`

**Interfaces:**
- Produces: `ClusterPublicationAuthorityPort.supports_linearizable_dispatch_permit() -> bool`.
- Legacy adapter returns `False`; admitted KeeperMap adapter returns `True` only after `require_ready` succeeds.

- [ ] **Step 1: Write failing capability tests.** Add `test_legacy_authority_has_no_linearizable_permit` (`is False`), `test_unadmitted_keeper_has_no_linearizable_permit` (`is False`), and `test_admitted_keeper_has_linearizable_permit` (`is True`).
- [ ] **Step 2: Run red tests.** `pytest -q tests/test_clickhouse_cluster_publication_contract.py tests/test_clickhouse_quality_authority.py -k linearizable_dispatch_permit`; expect attribute/test failure.
- [ ] **Step 3: Implement the port method and adapter methods.** Do not alter existing ordinary publication semantics or fabricate a permit from a read-only plan.
- [ ] **Step 4: Run green tests and compatibility tests.** Run the two files above in full; expect all passed.
- [ ] **Step 5: Commit.** `git commit -m "feat(clickhouse): expose strict publication permit capability"`.

### Task 2: Exact `PREPARED` preflight and redacted plan

**Files:**
- Create: `src/dpone/runtime/sinks/clickhouse_prepared_recovery.py`
- Modify: `src/dpone/ports/clickhouse_cluster_publication.py`
- Modify: `src/dpone/runtime/sinks/clickhouse_cluster_publication_ddl.py`
- Test: `tests/test_clickhouse_prepared_recovery.py`

**Interfaces:**
- Produces: immutable `PreparedRecoveryPlan` with target key, operation ID, authority version, original record digest, inventory digest, generation digests, deterministic publish token/query digest, and plan digest; its `to_public_dict()` redacts physical identifiers.
- Produces: `plan_prepared_recovery(catalog: ClusterPublicationCatalogPort, authority: ClusterPublicationAuthorityPort, ddl: ClusterPublicationDdlPort, *, cluster: str, database: str, target: str, operation_id: str, expected_version: int) -> PreparedRecoveryPlan`.
- Extends DDL port with `prove_no_prior_publication(record: AuthorityRecord, *, cluster: str) -> bool`; an unavailable history/retention boundary returns false or raises an unknown-outcome error, never true.

- [ ] **Step 1: Write failing preflight tests.** Add `test_plan_accepts_exact_prepared_generation`, `test_plan_rejects_schema_digest_drift`, `test_plan_rejects_unknown_ddl_history`, `test_plan_rejects_foreign_operation`, `test_plan_rejects_replica_row_mismatch`, and `test_plan_rejects_quality_mismatch`. The accepted case preserves original operation/version; each rejected case asserts zero authority/DDL mutations. Add parameterized inventory, predecessor, DDL-field and zero-row-policy variants.
- [ ] **Step 2: Run red tests.** `pytest -q tests/test_clickhouse_prepared_recovery.py -k plan`; expect missing API failures.
- [ ] **Step 3: Implement the pure plan service and DDL proof adapter.** Reuse `require_pre_dispatch_generation` and `_correlation_token`; do not interpret a missing queue entry alone as absence of a past DDL. An evidence source without a complete retention guarantee returns unknown.
- [ ] **Step 4: Run green tests.** `pytest -q tests/test_clickhouse_prepared_recovery.py -k plan`; expect all passed.
- [ ] **Step 5: Commit.** `git commit -m "feat(clickhouse): plan exact prepared recovery"`.

### Task 3: Single-permit execution and original-operation replay

**Files:**
- Modify: `src/dpone/runtime/sinks/clickhouse_prepared_recovery.py`
- Modify: `src/dpone/runtime/sinks/clickhouse_cluster_full_refresh_publication.py`
- Test: `tests/test_clickhouse_prepared_recovery.py`
- Test: `tests/test_clickhouse_cluster_full_refresh_publication.py`
- Test: `tests/integration/clickhouse_cluster/test_clickhouse_cluster_publication_faults_live.py`

**Interfaces:**
- Produces: `execute_prepared_recovery(plan: PreparedRecoveryPlan, *, confirmation_digest: str) -> ClusterFullRefreshReceipt` on a service constructed with catalog, authority, DDL and existing reconciliation/cleanup callbacks.
- Consumes Task 1 capability and Task 2 plan; re-reads every proof immediately before one CAS.
- Returns the existing operation receipt, not a newly invented load result.

- [ ] **Step 1: Write failing execution tests.** Add `test_execute_refuses_legacy_authority`, `test_execute_rejects_stale_plan`, `test_only_cas_winner_dispatches`, `test_unknown_cas_never_dispatches`, `test_crash_after_cas_reconciles_without_redispatch`, and `test_completed_operation_replays_receipt`. The happy path asserts exactly one DDL and zero source calls; rejection paths assert zero DDL. Add foreign-operation and changed-target variants.
- [ ] **Step 2: Run red tests.** `pytest -q tests/test_clickhouse_prepared_recovery.py -k execute`; expect missing API failures.
- [ ] **Step 3: Implement execution.** Validate confirmation digest, rerun preflight, require strict capability, CAS original record `PREPARED -> DISPATCHING` with deterministic token/query digest, dispatch only with verified permit, then invoke existing `_reconcile_existing` and `cleanup`; never catch unknown outcome to issue another DDL.
- [ ] **Step 4: Run green unit/contract tests.** `pytest -q tests/test_clickhouse_prepared_recovery.py tests/test_clickhouse_cluster_full_refresh_publication.py`; expect all passed.
- [ ] **Step 5: Run fault integration tests against an admitted strict authority.** `pytest -q tests/integration/clickhouse_cluster/test_clickhouse_cluster_publication_faults_live.py`; expect one DDL and terminal replica proof in evidence. If the environment lacks strict authority, record `UNVERIFIED` rather than substituting the legacy adapter.
- [ ] **Step 6: Commit.** `git commit -m "feat(clickhouse): resume prepared publication with strict permit"`.

### Task 4: Operator CLI, documentation, and full verification

**Files:**
- Create: `src/dpone/commands/prepared_recovery_cmd.py`
- Create: `src/dpone/app/prepared_recovery_composition.py`
- Modify: `src/dpone/commands/registry_ops.py`
- Create: `tests/test_prepared_recovery_cli.py`
- Create: `docs/clickhouse-prepared-recovery.md`
- Modify: `CHANGELOG.md`
- Modify: `docs/adr-index.md`
- Create: `docs/adr/clickhouse-strict-publication-authority.md`

**Interfaces:**
- Produces: `dpone ops clickhouse-prepared-recovery plan|execute --binding-set ... --connection-registry ... --connection-ref ... --cluster ... --database ... --target ... --operation-id ... --authority-version ... [--confirmation-digest ...] --format json`.
- CLI plan has no mutation; execute is unavailable without the matching digest and Task 1 strict capability. Exit codes are 0 success, 2 proven safety block, 1 unknown/operational failure.
- Composition uses `BindingCredentialResolver.resolve(connection_ref)` and `ResolvedConnectorFactory.create` from existing runtime credentials modules; the CLI never accepts raw passwords.

- [ ] **Step 1: Write failing CLI tests.** Add `test_plan_json_is_redacted_and_read_only`, `test_execute_requires_matching_digest`, `test_safety_block_exits_two`, `test_unknown_outcome_exits_one`, and `test_other_ops_commands_unchanged`. Assert no credential or row values in output.
- [ ] **Step 2: Run red tests.** `pytest -q tests/test_prepared_recovery_cli.py`; expect missing command.
- [ ] **Step 3: Add the thin command, composition helper and registry entry.** Read the two binding documents with the existing YAML codec; resolve the logical ref through `BindingCredentialResolver`, create the connector through `ResolvedConnectorFactory`, then call the Task 2/3 service. Do not duplicate business logic in argparse code.
- [ ] **Step 4: Write how-to, ADR, and changelog.** State strict authority migration/all-writer admission prerequisite; show dry-run and execute examples using synthetic identities, plus reconciliation and rollback limits.
- [ ] **Step 5: Run green focused and broad gates.** `pytest -q tests/test_prepared_recovery_cli.py tests/test_clickhouse_prepared_recovery.py`; then `ruff check src/dpone tests`, `mypy src/dpone`, `pytest -q`, and `python -m mkdocs build --strict`. Record exact totals and failures.
- [ ] **Step 6: Commit.** `git commit -m "docs(clickhouse): expose guarded prepared recovery workflow"`.

### Task 5: Independent review and release handoff

**Files:**
- Review only: the final branch diff against `origin/master`.
- Evidence: PR review/CI receipts, no customer data.

**Interfaces:**
- Produces an independent review verdict tied to exact commit SHA, covering race safety, data loss, compatibility, tests, docs and evidence.

- [ ] **Step 1: Push the implementation branch and open a focused PR.** Keep design approval and implementation commits traceable.
- [ ] **Step 2: Obtain a fresh-context subagent review.** Reviewer did not implement the feature; record exact SHA, findings and verdict.
- [ ] **Step 3: Resolve blocking findings, rerun affected tests, and obtain follow-up review of changed scope.** No self-approval substitute.
- [ ] **Step 4: Wait for exact-head CI and required maintainer review; merge by normal repository rules.** Do not release merely because a local suite passes.
- [ ] **Step 5: Prepare separate environment admission plan.** Verify a strict authority shared by every writer, migrate existing authority only via approved infrastructure procedure, then release/promote; never run execute against legacy ReplacingMergeTree.

## Operational handoff after implementation

The current production incident is **not** cleared by publishing this OSS code alone. On the affected environment, first prove the exact authority backend and all-writer admission, then run plan mode for the original operation, execute only if it returns `ready`, verify terminal authority/DDL/replica evidence, and only then permit one controlled workload run plus data-quality and freshness checks. If strict admission cannot be established, keep the operation blocked and escalate the infrastructure migration; do not fabricate a green DAG or mutate authority manually.
