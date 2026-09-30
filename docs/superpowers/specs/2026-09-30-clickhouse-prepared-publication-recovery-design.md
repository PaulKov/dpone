# Design: safe recovery of a prepared ClickHouse cluster publication

- Status: APPROVED
- Owner: dpone maintainers
- Issue: prepared full-refresh publication recovery
- Target release: next compatible minor release after approval
- Last verified: 2026-09-30

## Executive summary

A full-refresh load can finish staging and quality checks, persist a `PREPARED`
publication authority, then fail before the publication DDL is recorded. The
candidate remains intact, while later loads fail because another operation owns
the target. Recovery must finish the original operation without re-extracting
or issuing a second, uncorrelated `EXCHANGE`.

The proposed capability is an explicit, auditable recovery of that exact
operation. It rechecks immutable candidate and predecessor identities and
replica health, proves that no publication dispatch occurred, obtains a
**strict single-writer dispatch permit**, and uses the existing DDL correlation
and reconciliation path. If any proof is missing, it fails closed. The current
`ReplicatedReplacingMergeTree` authority adapter is *not* a strict CAS; it may
not issue a recovery permit merely because a versioned insert was read back.

## Personas and journey

| Persona | Goal | Current pain | Success signal |
| --- | --- | --- | --- |
| Operator | Resume a stranded load without duplicate publication | Later loads receive an authority conflict | Original operation completes with one correlated DDL receipt |
| Workload owner | Restore scheduled freshness | Downstream DAG tasks remain blocked | Next scheduled run succeeds after original recovery |
| Reviewer | Prove publication safety | A green task alone cannot prove physical state | Exact authority, DDL, replica and quality evidence is retained |

The operator discovers the original operation from the audit and authority
record, runs a read-only preflight, arranges a strict admission backend, then
executes recovery for the original operation ID. The operator observes the
same operation through `DISPATCHING`, `COMMITTED`, cleanup and `COMPLETED`, and
only then allows a new workload run. A failed or ambiguous preflight provides
an actionable reason and performs no mutation.

## Scope and non-goals

In scope: one-shot recovery of an existing `PREPARED` cluster full-refresh
operation, exact-generation and no-prior-dispatch proofs, strict permit
capability negotiation, dry-run diagnostics, replay-safe reconciliation,
audit/evidence and operator documentation.

Out of scope: deleting or rewriting an authority row, bypassing a quality
gate, silently adopting a foreign candidate, automatic retries of a failed
`EXCHANGE`, migrating all authority writers in this change, or claiming
row-wise source/target equality from aggregate checks. Ordinary publication
behavior remains unchanged until its authority backend advertises strict CAS.

## Public contract

An operator-facing recovery command accepts cluster, database, target,
operation ID and expected authority version as explicit inputs. It defaults to
read-only `plan`; `execute` requires an explicit confirmation of the plan
digest. It prints JSON with status (`ready`, `blocked`, `in_progress`,
`completed`), reason code, redacted generation identities, replica summary and
correlation ID. No connection secrets or row data enter the output. Exit 0
means the requested mode completed, exit 2 means a proven safety block, and
exit 1 means operational failure/unknown outcome. Python API invokes the same
service and returns typed results; the CLI contains no independent recovery
logic.

No pipeline manifest or schema option changes. Old manifests retain their
behavior. Evidence records bind target key, original operation ID, authority
version, candidate/predecessor UUID and schema digest, inventory digest,
quality-evidence digest when present, dispatch token, DDL query digest and
terminal per-replica outcome. The original authority remains the source of
truth. Recovery is idempotent for the same operation ID; a different operation
ID cannot claim `PREPARED` state. The dry-run never reserves a permit.

## Algorithm and state semantics

1. Read authority, inventory, candidate and target on every declared replica.
   Require exactly the requested target, original operation ID and version.
2. Require `PREPARED`, dispatch epoch zero, and no DDL entry, correlation
   token or query digest. Search the DDL queue and query history for the
   deterministic operation scope; any possible prior dispatch is `blocked`,
   not a reason to issue another DDL.
3. Require candidate and predecessor UUID/schema identities equal to the
   authority record on every replica, healthy replication, exact staged row
   count, unchanged inventory, and valid sealed quality evidence if the
   original operation carried it. A timeout or an unavailable replica is
   `blocked` or `unknown`, never a permit.
4. Require a backend capability that provides an atomic, linearizable
   compare-and-swap and unique dispatch permit across all writers for this
   target. `ReplicatedReplacingMergeTree` read-back is explicitly insufficient.
5. Compare-and-swap the same operation from `PREPARED` to `DISPATCHING` with a
   deterministic correlation token and DDL query digest. Re-read and validate
   the winning permit, then use the existing one-shot dispatch path. Record
   intent before DDL; after any dispatch exception, reconcile by exact token,
   digest and per-replica generation instead of dispatching again.
6. Use existing `COMMITTED` and cleanup transitions. Report `COMPLETED` only
   after exact terminal DDL and physical replica checks. A repeated request
   returns the proven terminal receipt, or resumes reconciliation without a
   second dispatch.

```text
record = authority.read_exact(target_key, operation_id, version)
proof = inspect_prepared(record, all_replicas, ddl_queue, quality_evidence)
if not proof.no_prior_dispatch or not proof.same_generations: fail_closed()
if not authority.supports_linearizable_permit: fail_closed()
if mode == plan: return proof.redacted_plan()
permit = authority.cas_prepared_to_dispatching(record, plan_digest)
dispatch_once_or_reconcile(permit)
reconcile_exact_ddl_and_generations()
complete_existing_operation()
```

`PREPARED -> DISPATCHING -> COMMITTED -> CLEANUP_DISPATCHING -> COMPLETED`
is the only successful transition. A concurrent worker losing CAS does not
dispatch. A process crash before CAS permits a new preflight; a crash after
CAS uses the existing exact DDL reconciliation. A partial or unknown DDL
outcome remains unresolved for operator inspection. No rollback can undo a
completed `EXCHANGE`; rollback means stopping further dispatch and retaining
evidence. Empty candidates are allowed only when the authored full-refresh
contract explicitly permits zero rows. Schema drift, duplicate authority
versions, missing metadata, topology drift and unsupported backends all fail
closed.

## Architecture and dependency direction

| Component | Responsibility |
| --- | --- |
| Recovery service | Pure orchestration of preflight, state transition and reconciliation |
| Catalog/DDL ports | Read physical replica and exact DDL evidence; dispatch through the existing permit path |
| Authority port | Capability negotiation and linearizable CAS/permit |
| CLI adapter | Parse inputs and render redacted typed result |

The runtime depends on ports, not concrete ClickHouse clients. The current
ReplacingMergeTree authority adapter reports strict-permit capability `false`;
an adapter backed by a genuine atomic authority must be deployed and admitted
for **all writers sharing the target** before execute mode is enabled. A
single process mutex or an Airflow pause is not sufficient. This migration is
a separate coordinated infrastructure prerequisite, not a flag flipped by
this command. Expected new code is split into small service/port/CLI modules
within the repository's `max_sloc: 400` budget; no large conditional is added
to the publication service. An ADR is required for the strict authority
backend and rollout because it changes cross-writer coordination.

## Alternatives

| Alternative | Decision |
| --- | --- |
| Delete the stuck authority and start a new load | Reject: loses the original operation's fencing/evidence |
| Treat ReplacingMergeTree version read-back as strict CAS | Reject: concurrent inserts can both pass the predicate |
| Retry `EXCHANGE` without correlation | Reject: a committed or in-flight DDL could be applied twice |
| Operator-only recovery using one strict authority and exact original identity | Adopt |

## Market comparison

Official documentation checked 2026-09-30. [dlt's production guide](https://dlthub.com/docs/running-in-production/running)
describes retrying a pending load package without rerunning completed jobs and
retaining retry history. Adopt its principle of resuming the original work
identity; unlike a file/job retry, ClickHouse DDL requires independent exact
publication proof. [Airbyte's heartbeat documentation](https://docs.airbyte.com/platform/understanding-airbyte/heartbeats)
describes failing an unresponsive sync attempt; it does not establish a
general-purpose atomic DDL recovery contract, so no such behavior is inferred.
Informatica, Fivetran, Pentaho, SSIS, gusty, Astronomer Cosmos and Apache Beam
are N/A here: the comparison is narrowly about a stranded physical ClickHouse
cluster publication, not scheduling, connector extraction or generic stream
checkpoints. This is a scope judgment, not a claim that those systems lack
recovery features.

Measurable axis: for a synthetic three-replica, 10-million-row candidate
stranded in `PREPARED`, recovery must issue at most one correlated DDL, perform
zero source reads and zero row re-exports, and reach `COMPLETED` only with
three matching generation observations. Unit/chaos artifacts record DDL call
count, source call count, state history and replica proof. No unqualified
market-superiority claim is made.

## Security, tests and rollout

The command uses existing connection aliases and least-privilege read access
for plan mode; execute additionally needs the admitted authority mutation and
DDL permissions. Logs redact credentials and data. Alert on unresolved
`PREPARED` age and on any `unknown` DDL outcome.

Tests cover: exact happy path; no DDL entry; an existing/ambiguous DDL entry;
concurrent CAS losers; same-operation replay; foreign-operation rejection;
candidate or predecessor UUID/schema drift; replica lag and timeout; zero-row
policy; quality-evidence mismatch; process crashes before/after CAS and DDL;
unknown mutation outcome; RRT capability refusal; and old-manifest behavior.
Integration tests use a real strict authority backend and three-replica
ClickHouse, with fault injection at each state boundary. Certification must
retain redacted plan, authority and DDL receipts. Documentation adds an
operator how-to, state-machine reference and incident runbook.

Roll out the strict authority backend to every writer first, verify capability
and compatibility, then enable the recovery command. Initial operation is
operator-invoked only. If observations diverge, stop before dispatch and
retain the original authority. Rollback disables new recoveries while leaving
already-dispatched operations to the existing reconciliation path; it never
rewrites authority or repeats DDL. Production use requires a separate
environment-specific admission and exact physical preflight.

## Agent execution and approval

One integrator owns the service and shared contracts. A separate reviewer with
fresh context reviews the final implementation commit for concurrency,
data-loss, compatibility and evidence before merge. Public documents and
synthetic tests contain no customer identifiers or production operation IDs.

- [x] User problem and journey are defined.
- [x] State/failure semantics and compatibility are explicit.
- [x] Alternatives and primary-source comparison are recorded.
- [x] Test, evidence, documentation and rollback paths are specified.
- [x] Maintainer reviewed this written specification and set `APPROVED` (2026-09-30).
