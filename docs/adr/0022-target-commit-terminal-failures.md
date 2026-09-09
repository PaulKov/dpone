# ADR 0022: Durable target commit journal and fence

- Status: Proposed
- Date: 2026-07-18

## Context

Target data, checkpoints, evidence, source state, and Airflow task state do not
share one distributed transaction. A runtime process can die after target
mutation but before XCom or other evidence is durable. Process-local state,
typed exceptions, and `AirflowFailException` cannot prevent the next Airflow
attempt from executing the target mutation again.

The reviewed process-local proposal therefore does not satisfy dpone's
no-silent-duplication invariant and is not accepted for production.

## Proposed decision

Before target mutation, dpone will:

1. derive a stable `operation_id` and `intent_digest`;
2. create a durable journal intent;
3. establish a target-side idempotency fence or an equivalent
   connector-certified reconciliation mechanism;
4. transition journal state through compare-and-set operations;
5. require every retry to reconcile the same operation before target I/O.

The proposed durable states are:

- `PREPARING`;
- `PREPARED`;
- `COMMITTING`;
- `COMMIT_UNKNOWN`;
- `TARGET_COMMITTED`;
- `COMMITTED_INCOMPLETE`;
- `COMPLETE`;
- `FAILED_PRE_COMMIT`.

Contracts belong in `dpone.contracts`, the journal interface in `dpone.ports`,
and storage implementations in `dpone.adapters`. Runtime policy remains
orchestrator-neutral. Airflow exceptions improve task behavior but are not the
correctness boundary.

No connector may claim automatic retry protection without a certified,
queryable target fence. When a target outcome cannot be proven, recovery is
manual and fail-closed.

## Consequences

If accepted and implemented, retries can survive pod, worker, and XCom loss
without repeating a certified target mutation. The design adds a durable
availability dependency, connector capability work, retention, recovery
operations, and route-level failure-injection certification.

Until those pieces exist, dpone must not publish `retryable: false` or terminal
target-commit fields as a complete duplicate-write prevention guarantee.

## Implemented interim mitigation

The patch after `0.73.1` implements a narrower fail-closed guard while this ADR
remains Proposed. Before native target mutation, dpone writes a bounded
`target_commit_guard` into the existing partition-checkpoint diagnostics for
every active partition. A later resume scans the complete source/target/strategy
route before sink I/O and blocks on an `armed`, `checkpoint_incomplete`,
`target_outcome_unknown`, malformed or unknown guard.

The mitigation preserves the original target/checkpoint exception and prevents
automatic retry after an observed unsafe checkpoint transition. It does not
provide a stable operation identity, CAS, concurrent-worker exclusion,
process-death atomicity, target-side fence or automatic reconciliation. Existing
checkpoints remain readable. Older workers ignore the additive diagnostic, so
deployment requires draining old workers before automatic execution resumes.

See the
[approved target-commit guard specification](../feature-design-native-transfer-checkpoint-guard-v0732.md).

## Acceptance before status can become Accepted

- stable operation identity schema approved;
- journal port and one CAS-capable adapter implemented;
- one connector fence implemented;
- hard-process-kill and concurrent-worker tests pass;
- recovery CLI and operator runbook approved;
- one live route is certified with Airflow retries enabled;
- migration and retention policies are documented.

ADR 0045 accepts one bounded V2 semantic-refresh design that implements the
journal, SQL Server fence, retained evidence, and queryable ClickHouse UUID
boundary described here. This ADR remains `Proposed`: the accepted design does
not satisfy this ADR's production acceptance gate until the exact live route,
hard-kill, concurrency, retention, and operator-recovery evidence exists.

## Related

- [Feature design: durable target commit recovery](../feature-design-target-commit-terminal-failures.md)
- [ADR 0013: Reproducible rerun and retention](0013-reproducible-rerun-and-retention.md)
- [ADR 0045: Semantic refresh uses fenced multi-boundary operations](0045-durable-semantic-refresh-runtime.md)
