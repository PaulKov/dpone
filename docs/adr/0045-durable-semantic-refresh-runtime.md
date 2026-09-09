# ADR 0045: Semantic refresh uses fenced multi-boundary operations

- Status: Accepted
- Date: 2026-08-08
- Amends: ADR 0022, ADR 0034

## Context

The native dbt publishing path in ADR 0034 intentionally has one dbt build and
independent model transfers. Airflow state and XCom cannot prove whether a SQL
Server model transaction, an artifact upload, or a ClickHouse table exchange
committed after a worker or network failure. Retrying those boundaries without
durable identity can duplicate or silently corrupt data.

SQL Server and ClickHouse also use different equality, transaction, and object
identity rules. A generic incremental-merge label is therefore insufficient.
The supported behavior must name one exact capability cell, compiler proof,
writer authority, transport protocol, publication algorithm, and recovery
contract.

## Decision

V2 introduces an additive semantic-refresh protocol. V1 artifacts and runtime
behavior remain readable and unchanged; V1 and V2 cannot be mixed inside one
workflow.

The first capability cell is `scope_stable_event_fact`:

- one UTC day represented as a half-open interval;
- an existing, complete, baselined SQL Server table;
- platform-owned scoped `UPDATE` followed by `INSERT`, with missing source keys
  retained;
- one SQL Server database for the model and control rows;
- one exclusive workflow writer with a transaction-local fence check;
- immutable before/after images and `dpone_parquet_v1` sealed artifacts;
- one single-node, single-replica ClickHouse `Atomic` database and ordinary
  `MergeTree` tables;
- a full-table shadow proven by exact target-local multiset comparison and one
  `EXCHANGE TABLES` publication;
- deterministic sequential model publication without workflow-wide atomicity;
- no automatic SQL retry.

Version 0.74 exposes this cell only as a local diagnostic preview. The
application composition root and canonical MSSQL activation service both
require an injected production-activation guard. The shipped guard always
raises `DPONE_SEMANTIC_REFRESH_PRODUCTION_ACTIVATION_UNAVAILABLE` before
authority persistence, physical target mutation, or admission of a new V2
DagRun from an already-present activation receipt. Local integration tests may
inject an explicit test-only guard; that is not production authority.

The 0.74 preview freezes
`dpone.semantic-refresh-attempt-continuation-receipt.v1` as one immediate
same-DagRun successor (`N -> N+1`) bound to the original attempt and fence.
This receipt is an admission condition, not retry authority. Any monotonic
current-holder chain, detached termination binding, or broader retry requires
an additive receipt v2 and a separately accepted post-0.74 decision; v1 is
never rewritten in place.

Worker admission sets and acknowledges the driver query timeout from the
smallest protected `max_statement_seconds` before creating the SQL cursor;
continuation rechecks that bound before SQL. It then performs a locked row and
conservative serialized-byte preflight before `FOR JSON`. Missing timeout
authority, a failed timeout acknowledgement, or any row/byte limit fails before
target JSON materialization.

### Identity and workflow modes

Identity is derived only downstream:

```text
contracts and policies
  -> exact dbt closure and model-definition proof
  -> semantic model operations
  -> semantic workflow plan
  -> optional replacement plan
  -> deployment-bound, run-neutral Airflow DAG projection
  -> actual logical DagRun admission at the first worker boundary
  -> workflow-execution binding
  -> attempt binding
  -> runtime receipts, state, and evidence
```

Attempt, pod, Airflow try, fencing epoch, observed counts, and timestamps never
change semantic operation identity. A recovery plan may reference an existing
workflow plan; an upstream semantic plan never references a recovery digest.
The immutable deployment index contains no future DagRun identifier, workflow
execution binding, attempt, or fence. Airflow parses only a content-addressed
run-neutral projection from that index. The first worker reads the actual
logical DagRun identity from the trusted Airflow task context, persists the
exact run authority create-only, and all later workers resolve the durable
binding by plan and logical run identity. Parse-time database I/O, mutable
"current run" pointers, and XCom authority are prohibited.
The same projection digest binds the existing compiled workflow profile's
schedule, start date, timezone, catchup, active-run/task limits, owner, and
tags. DAG parsing accepts no caller/default timing or scheduling values; an
incomplete static DAG policy fails closed before the DAG enters Airflow
globals.

Deployment activation receipts are create-only per
`(release_id, deployment_id, plan_bundle_sha256)`. Their MSSQL physical key is
`(deployment_id, plan_bundle_sha256)`, with one invariant release/store identity
per deployment. A failed-precommit successor keeps the exact release,
deployment, pre-release/package, model/scope and assurance closure, persists a
new plan-scoped receipt, and preserves the predecessor receipt; it never uses
a mutable revision or `latest` pointer. The planned authority freezes its
`persisted_at` value for exact acknowledgement-loss replay. Persisting this
successor receipt does not repeat initial baseline/owner/head activation. The
actual-DagRun admission transaction authenticates the run pack against that
exact composite receipt before acquiring guards.

The ordinary provider remains a lightweight scheduler package. This V2 route
chooses the explicit in-process Kubernetes-worker topology: the Airflow 3.3
scheduler/worker image pins `dpone[semantic-refresh-airflow]`, the provider and
the reader to one release line. DAG construction composes only lazy factories
and performs no external I/O; database, Vault, object-store and ClickHouse
access starts inside task execution. Installing `dpone[full]` or allowing
version-skew is not an authorized substitute. A future KPO-only split is a
separate architecture change, not an implicit fallback.

Every workflow declares one of `normal`, `failed_precommit_replacement`, or
`complete_scope_replay`. The complete selected mutating-node inventory must
equal the operation and expected-outcome inventories. A replacement additionally
requires the same complete action inventory. Modes have no per-model override
or V2-to-V1 fallback.

### dbt and SQL Server boundary

V2 pins dbt Core 1.10.13, dbt-sqlserver 1.10.1, Python/driver/runtime artifacts,
the full materialization and macro closure, engine compatibility, invocation,
and project/profile policies. It admits only exact selected, contracted
incremental models using `dpone_scope_merge`. Ephemeral resources, hooks,
custom materializations, full refresh, unmanaged mutations, and mutable
adapter lifecycle options fail closed.

Model SQL is a read-only, target-independent SELECT. Dependency proof permits
base tables and recursively resolved same-database, non-encrypted,
`SCHEMABINDING` views and inline table-valued functions. Scalar, multi-statement
and CLR functions, procedures, synonyms, external or linked-server access,
dynamic SQL, unresolved metadata, and dependency drift are unsupported.

The platform strategy runs inside dbt's existing transaction. It locks and
validates the actual durable guard row, captures the complete scope preimage,
checks source and target keys, performs `UPDATE` then `INSERT`, captures the
postimage, and writes one immutable build receipt before the adapter commit.
The receipt and images, not a process exit code, classify the SQL Server
outcome.

### Journal, replacement, and publication

Writer admission atomically creates the full guard set, fencing epochs,
workflow binding, resource reservation, and `PREPARING` journals. A stale or
unverifiable writer cannot mutate either engine.

An exception after the worker-admission commit call starts is reported as
`DPONE_SEMANTIC_REFRESH_WORKER_ADMISSION_COMMIT_UNKNOWN`; the failed connection
handle is closed without claiming rollback or success. This does not prove a
pooled physical session was quarantined. The 0.74 preview requires operator
reconciliation before another attempt and does not implement an automatic
distinct-session commit-outcome classifier.

SQL Server outcomes are `NOT_INVOKED`, `ROLLED_BACK`,
`COMMITTED_WITH_IMAGES`, and `COMMIT_UNKNOWN`. Receipt absence alone is never
rollback proof. `COMMIT_UNKNOWN` blocks retry, replacement, and transfer until
reconciliation.

The terminal failed-workflow summary is a versioned closed contract. Its exact
operation inventory and per-model evidence are committed with the
`FAILED_PRE_COMMIT` journal transition and complete guard release. It excludes
`COMMIT_UNKNOWN`, so an unresolved model cannot be hidden inside a terminal
replacement predecessor.

Guard release does not by itself authorize reuse of a failed target. Before
any ordinary run or workflow successor can reacquire that target guard, the
controller must prove the deterministic ClickHouse staging/shadow relations
absent, prove the protected target UUID unchanged, prove the exact five
aggregate allocation rows `RELEASED`, and persist their create-only cleanup
acknowledgement. Cleanup authority is unavailable for `COMMIT_UNKNOWN`.

A terminal failed-precommit workflow is corrected by one new workflow-level
replacement plan. Models with committed images restore the exact predecessor
preimage, verify it, capture a new clean preimage, and rebuild in one fenced
transaction. Models that were rolled back or never invoked build fresh. A
replacement cannot proceed over an unknown outcome.

The journal is operation-state authority. A conditionally created,
version-pinned manifest is sealed-artifact membership authority; SHA-256 binds
each chunk's bytes. ClickHouse staging and shadow tables remain in the target
`Atomic` database. Before exchange, the runtime proves:

```text
shadow == old_target LEFT ANTI JOIN staging ON effective_key
          UNION ALL staging
```

using exact bidirectional grouped-multiset comparison over all business columns
and multiplicity. The actual UUID mapping proves exchange outcome. Target head,
scope head, checkpoint, terminal journal state, and evidence digests publish in
one serializable SQL Server transaction.

The V2 strategy and public evidence use `workflow_execution_id` for the actual
logical DagRun. Existing SQL control tables retain a compatibility column named
`workflow_id`; within this schema version it is an exact alias of
`workflow_execution_id`, not the stable workflow name. Stable workflow identity
is the authenticated workflow-plan digest plus its canonical `workflow_name`.
New contracts must not copy the legacy alias.

SQL Server catalog proof is bound to a positive monotonic DDL epoch. The dbt
mutation transaction holds a shared application lock and compares that epoch;
database DDL requires the exclusive lock and increments the epoch. ClickHouse
publication authenticates the complete physical design before and after
exchange. The predecessor generation is retained until the serializable
`COMPLETE` publication succeeds; its later cleanup is maintenance and cannot
rewrite a committed terminal state.

## Consequences

- Author metadata and the beginner command surface do not gain recovery or
  adapter switches.
- The first cell deliberately excludes text keys, moving current-state rows,
  delete-missing semantics, greenfield bootstrap, replicated ClickHouse,
  workflow-wide atomic publication, and cross-DagRun resume.
- Direct SQL readers may observe a new generation of one model while a later
  model still has its old generation. Success Assets appear only after a fully
  complete durable workflow summary.
- `datetime2(6)` requires ongoing protected UTC assurance because the physical
  SQL Server type contains no timezone.
- Version 0.74 is a local diagnostic preview, not a production certification.
  Production remains unavailable even if the exact MSSQL, ClickHouse, Airflow,
  Kubernetes, Vault, and artifact-store fault campaign passes. A later release
  must ship the reviewed exact-UUID predecessor-retention controller, an
  additive certified allow-guard, and current retained live evidence.

## Related

- [V2 semantic refresh feature specification](../feature-design-semantic-refresh-runtime-v2.md)
- [ADR 0022: Durable target commit journal and fence](0022-target-commit-terminal-failures.md)
- [ADR 0034: Native dbt self-service](0034-native-dbt-self-service-multi-repo.md)
