# Composition activation contract

The base coordinator provides complete parent source, physical-admission and
occurrence contracts. **Public composition activation and actual worker execution
remain unavailable until protected backend adapters and worker fencing are
installed and verified.** No production factory or new cache-sync CLI option is
provided by this base change. The existing default returns
`DPONE_COMPOSITION_ADMISSION_UNAVAILABLE`.

This page describes the implemented integration boundary and the required
follow-up acceptance. See [composition operations](release-composition-operations.md)
for artifact delivery, and [the approved specification](feature-specs/composition-activation-execution.md)
for the full execution scope. A successful source read, plan, fake-adapter test,
cache install or launcher prepare does not certify SQL execution.

The separate [ClickHouse snapshot component](composition-clickhouse-snapshots.md)
defines strict whole-snapshot intents and one-time EXCHANGE/recovery policy.
Concrete ClickHouse gate, catalog, storage and worker qualification remain
required before that component can participate in actual parent execution.

## Concrete SQL Server persistence

`dpone.adapters.composition_mssql_store.MssqlCompositionActivationStore`
implements the occurrence store with real DB-API SQL statements. It accepts an
injected factory for independent connections, an externally pinned
`expected_service_id`, and an optional `control_schema`. It retains the complete
MSSQL and ClickHouse resource closure in one protected SQL control database.
ClickHouse rows in that ledger do not themselves prove ClickHouse permissions or
writer exclusion.

The platform installs the SQL from
`dpone.adapters.composition_mssql_schema.render_composition_mssql_schema()`
explicitly, then provisions and protects the authority and domain records.
Reapplying the initial DDL fails rather than adopting existing tables. Runtime
operations never create, repair or enroll their own authority. A matching schema
version, eight table names and service UUID are structural prerequisites; the
protected backend must separately verify database continuity, role permissions,
exclusive enrollment and the installed writer gates before using this store.

The store uses one short transaction-owned application lock in the control
database to serialize ledger changes. This lock is not a writer-session fence.
Requests retain canonical UTF-8 bytes and their original catalog observations.
Each mutation closes its transaction connection and independently rereads the
exact request, state, complete guard partition and epochs. A lost commit
acknowledgement permits only exact reconciliation; it never triggers a blind
mutation replay. An unavailable or changed readback requires operator recovery.

Before retirement, the store reads all durable attempts, their complete epoch
partitions and the immutable issued-principal journal. Resolved attempts require
the exact protected `CLOSED_GATES`, `QUIESCENCE` and `OUTCOME` proof triplet. Each
proof binds attempt, parent request, guard-epoch subject, backend service and
issued principal identities. `OUTCOME` also binds its terminal state; a caller
cannot promote a protected failed outcome to success. Unknown or running attempts
retain ownership. A proof document's shape or caller-supplied digest grants no
authority: trusted backend producers must create the actual observations and
protected records.

An unowned domain is not sufficient for successor admission. The store uses the
historical activation partitions to find previous owners, then reopens their
complete attempt/proof closure. Missing attempt partitions, missing proof bytes
and terminal-state tampering reject reservation before any epoch is advanced.

Offline adapter tests use explicit DB-API doubles. The separate synthetic SQL
component runner exercises real control transactions; neither is the full
current/provider/native/generated/ordinary execution campaign. Public activation
remains unavailable until the complete protected backend and worker path is
installed and verified.

## SQL Server attempt and connection gates

`MssqlCompositionAttemptStore` in `dpone.adapters.composition_mssql_attempts`
accepts the same connection factory, pinned service UUID and control schema.
`admit_once(attempt)` persists all selected epochs before credential issuance.
`read_exact(attempt)` audits durable state and does not authorize another executor.
Admission checks the complete protected proof triplet of relevant previous
terminal attempts; three nonempty receipt digests are insufficient.

`finalize(attempt, state=..., outcome_evidence_sha256=...)` selects a protected
OUTCOME proof for a RUNNING attempt. The argument names the proof digest; its
inner `evidence_sha256` names the actual producer's outcome evidence. Completion
requires matching closed-gate and quiescence proofs for every issued principal,
including other backends. A SQL gate cannot supply business-outcome evidence.
The explicit `reconcile_unknown(...)` operation accepts a newly protected success
or failure proof only for an audited COMMIT_UNKNOWN attempt. It issues no
credentials and cannot change an already successful or failed attempt.

`MssqlCompositionLoginGate` in `dpone.adapters.composition_mssql_login_gate`
additionally requires the exact `control_database`. `issue_once(attempt)` first
commits a unique login SID/name to the protected journal, then creates its bounded
target users and verifies READY on another connection. Only that invocation
receives the memory-only password. A replay cannot reset, enable or recreate the
principal. Driver tracing must be disabled on these injected connections.

`close(attempt)` irreversibly closes the reconnect gate and verifies the exact
disabled SID before persisting closure evidence. `prove_quiescence(attempt)`
requires server-visible absence of that original SID's sessions and transactions.
Missing or uncertain principal state remains blocking. Process exit, gate table
presence and login disablement alone are insufficient quiescence evidence.

The platform separately installs
`render_composition_mssql_login_gate(control_database=...)` from
`dpone.adapters.composition_mssql_gate_schema`. Execute its `GO` batches preserving
the trigger definition bytes. Provision the disabled gate reader, its minimum
catalog/DMV grants and the protected control-table read permission as documented
by the renderer. Enroll newly isolated target databases with their actual
database identity, managed schemas and exact bounded writer role. Runtime checks
the installed trigger definitions and permissions; it does not provision them.
For this initial cell the trusted controller must own each dedicated target
database, the bounded role must be owned by `dbo`, and managed objects must use
inherited or `dbo` ownership. Read-only principals may not acquire writer authority
through object or role ownership.
The controller's exact `dbo` principal may retain its membership in the fixed
`db_owner` role. This exception requires principal ID 1 and the original
controller SID; it does not admit any other `db_owner` member.
The catalog policy reports fixed `login_database_policy_*` refusal reasons for
role/object ownership, unsupported objects, unmanaged scope, assemblies,
authentication, ambient grants and memberships. These declared policy failures
are distinct from `control_operation_unknown`, which indicates an unavailable or
uncertain driver operation. A diagnostic reason never relaxes the predicate.
LOGON synchronization and DMV visibility still require real tests on the pinned
SQL Server version before worker activation is enabled.

The gate-reader permission probe restores the controller before it emits its
result set. A driver that reads only the first row must not leave subsequent
ledger or login operations running under the reader's restricted identity.

## Disposable SQL component check

The `Composition SQL Server component` GitHub workflow uses a clean candidate
checkout, Linux x86-64, Docker and Microsoft ODBC Driver 18. Its equivalent command
on an explicitly approved disposable runner is:

```bash
uv sync --locked --extra mssql
uv run python tools/composition_mssql_synthetic.py \
  --profile gate \
  --output-dir "$RUNNER_TEMP/composition-mssql-gate-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
```

The output must be a new directory outside the checkout. The runner creates its
own pinned SQL Server container, random loopback port and synthetic database;
it does not accept an existing service. It retains only sanitized `summary.json`
and `junit.xml`, recording exact source, image, driver and server identity. All
expected cases must execute without skips. A cleanup failure changes the result
to FAIL. No raw driver diagnostics or credentials belong in these artifacts.

The workflow runs two independent profiles, each in its own fresh container:

| Profile | Required cases | Evidence scope |
|---|---|---|
| `store` (default) | 7 | Real ledger DDL, whole-parent transactions, conflicts, lost acknowledgements and retirement |
| `gate` | 16 | Real issued credentials, target permissions and continuity, monotonic LOGON closure, races, in-flight transactions and explicit unknown recovery |

The gate profile uses the actual closed-gate and quiescence producers. Its
test-only outcome producer binds observed SQL and independent reconciliation;
it does not qualify the future native/transfer worker outcome producer. The
runner rejects a missing, skipped, duplicate or foreign case, including results
from the other profile. JUnit retains bounded numeric SQL error identifiers and
fixed domain reasons for failure diagnosis; raw driver messages stay suppressed.
Default non-live collection skips both profiles without opening a connection.

This component's ClickHouse rows are synthetic ledger metadata. Route
qualification and complete worker execution remain separate observations;
a component PASS must not be reported as their certification. LOGON conclusions
require a green `gate` result on the exact source and pinned server version;
the `store` profile supplies no such evidence.

## Required downstream matrix

| Workload | Required route | Base contract | Actual parent execution |
|---|---|---|---|
| Native dbt | Verified SQL Server execution-pack.v2 | Full native workflow/model/helper ownership | UNVERIFIED; protected SQL login gate and worker integration pending |
| Native-generated transfer | MSSQL to ClickHouse full_refresh | Separate producer-aware classifier; positive cumulative max_source_bytes required | UNVERIFIED; protected CH gate, Atomic publication and canonical route evidence pending |
| Ordinary transfer | PostgreSQL to MSSQL full_refresh | Explicit external target_atomic state; table extraction only | UNVERIFIED; parent fence inside the actual target transaction pending |
| Other cells, including ordinary MSSQL to ClickHouse | Not in this initial matrix | Rejected | Unsupported |

The required downstream spans SQL Server and ClickHouse. A SQL Server-only
coordinator or synthetic pass does not satisfy this matrix. The same named
execution cell cannot make an ordinary declaration eligible for the native-only
ClickHouse route.

## Exact Python boundaries

Read a complete parent with the production source verifier:

```python
from pathlib import Path
from dpone.app.release_composition import build_composition_source_reader

sources = build_composition_source_reader().read_sources(
    Path("synthetic/composed"),
    expected_release_id=expected_parent_release_id,
)
```

The returned `CompositionSourceSnapshot` retains the native child identity,
ordinary inventory identity, every final workload pin, complete relation writes,
and bounded original transfer manifests. Native-generated transfers are included.
The reader revalidates the entire producer closure before returning. Ordinary
archives retain support for declared SQL files and generated runtime manifests.
Their delivery acceptance is separate from the narrower execution matrix.
State-bearing ordinary MSSQL packs use
`AirflowCompactPackBuilder.build(..., outlet_binding="logical")`; the verifier
reconstructs the same projection. Aliases gain physical authority only from later
verified deployment bindings, never from the logical outlet.

`dpone.manifest.composition_execution_plan.plan_composition_execution(sources)`
classifies the complete workload union. Native-generated declarations retain
`runtime`, `quality`, `gitops`, `sink.mode`, source/target options and their byte
budget. They are not stripped into an ordinary-manifest shape. The protected
backend must subsequently validate all option effects, physical design, state,
permissions, enrollment and writer-session capabilities.

Application integrators construct
`dpone.services.composition_activation_coordinator.CompositionActivationCoordinator`
with keyword capabilities `inputs`, `preparation` and `stores`.
`CompositionActivationPreparation(physical=...)` uses
`CompositionPhysicalAdmissionService(backend=...)` to merge aliases before
physical observation. The ports document the protected transaction and session
requirements; in-memory or filesystem-only implementations do not meet them.

The coordinator exposes:

- `prepare(*, projection_root, activation_id, environment, release_id,
  deployment_id, previous_deployment_id)`;
- `activate(prepared, *, projection_root)`;
- `require_active(...)` with the same coordinates as `prepare`;
- `begin_retirement(active, *, projection_root)`;
- `finalize_retirement(retiring, *, projection_root)`.

The additive materializer parameter is
`DeploymentCacheMaterializer(..., composition_activation_coordinator=coordinator)`.
It is separate from the existing native-only `workspace_activation` capability.
A native receipt, untyped result, missing constituent, stale request or changed
fencing epoch cannot acknowledge parent activation. Production deployment
composition must supply all protected capabilities before using this parameter.

## Identity and physical collision algorithm

1. Verify original native and ordinary sources, final transport, parent identity,
   selected native trios and exact source-to-workload membership.
2. Bind the activation to the sealed deployment/runtime context and a UUIDv4
   occurrence. Classify every workload; reject an unavailable cell before any
   reservation or predecessor drain.
3. Resolve every binding to its protected service incarnation and physical
   collision domain. Aliases, credentials, hostnames and engine versions are not
   physical identities.
4. Group all writes by the resulting domain. Ask the backend for one complete
   catalog/collation comparison per group, including native intermediate, backup
   and helper relations and all modeled transfer effects. Query-local equivalence
   IDs from separate alias queries are not comparable.
5. Reject missing/duplicate slots or any equivalent write coordinates. Persist
   the immutable request containing full workload/write membership, stable
   resources and the original observation digest.
6. On later phases, reread the immutable request and reverify source, runtime and
   stable physical bindings. Legitimate table create/modify timestamps must not
   change the original request identity.

`dpone.contracts.composition_control` is the explicit contract boundary for
application services and ports. It only reexports cohesive DTOs/policies; it
performs no I/O, backend selection or policy registration.

## Lifecycle and failure semantics

```mermaid
stateDiagram-v2
    [*] --> PREPARED: complete admission / protected reservation
    PREPARED --> ACTIVE: current CAS commit / exact acknowledgement
    ACTIVE --> RETIRING: close new attempt admission
    RETIRING --> RETIRED: closed gates / quiescence / resolved outcomes
```

The store must reserve all guards without expiration, close predecessor admission,
prove its attempts quiescent with resolved outcomes, and transfer exact epochs
under protected control transactions. Cross-service data writes remain separate
transactions. Drain-first handover does not promise zero downtime.

The cache validates a typed PREPARED receipt before switching `current` and the
same full request/epochs in ACTIVE afterward. A failed post-pointer acknowledgement
returns `DPONE_COMPOSITION_ACTIVATION_COMMIT_UNKNOWN` with
`state_may_have_changed: true` and `recovery_required: true`. Retain sealed inputs,
original request and control evidence. Old pointer bytes alone do not prove the
old occurrence remains executable after a protected ownership transfer.

`CompositionAttemptIdentity` binds the parent activation request, constituent,
workload pack, verified plan, actual DAG run/task/try/map coordinates and complete
applicable guard epochs. Protected admission must evaluate the entire ledger in
the same transaction as reservation. The pure policy rejects RUNNING replay,
stale or missing epochs, retired parents and overlapping RUNNING or COMMIT_UNKNOWN
attempts across native and ordinary workloads.

A terminal receipt requires independent closed-gate, server-quiescence and durable
outcome evidence. A closed gate or exited process does not resolve an unknown SQL
commit. No TTL may release its resource ownership. These policy checks still need
to be connected to actual worker sessions and target transactions.

## Downstream CI and recovery acceptance

The required isolated synthetic campaign is:

```text
verified native + ordinary producer outputs
  -> complete parent -> deployment -> current ACTIVE
  -> provider loads every expected DAG -> explicit DAG triggers
  -> actual native dbt SQL -> generated MSSQL-to-ClickHouse transfer
  -> actual ordinary PostgreSQL-to-MSSQL transfer
  -> independent row/schema/state/evidence reconciliation
```

Native DAG ordering does not establish cross-constituent dependencies. With
`schedule: null`, trigger the relevant DAGs explicitly. Do not handcraft runtime
plans or rewrite producer/receipt identities.

The campaign must retain exact source commit, environment versions, parent and
child identities, activation, all workload/run/attempt IDs and complete observed
results. It must reconcile Decimal/numeric precision and scale, GUID, NULL,
vanished rows, empty full snapshots and measured cumulative source bytes. Source
stream bytes are distinct from post-transcode HTTP bytes.

Required faults include physical aliases/collation collisions, unsupported cells,
stale CAS/context/epochs, replayed RUNNING attempts, lost gate issuance ACK,
credential reconnection races, current commit without ACTIVE ACK, committed SQL
without evidence, delayed ClickHouse staging INSERTs and uncertain EXCHANGE.
ClickHouse recovery must compare persisted before/after UUID identities: blindly
retrying EXCHANGE can exchange the tables back. Close and drain stage writers
before exposing that UUID as the target. Deployment rollback does not undo SQL.

Current acceptance gaps are explicit:

- Protected SQL Server enrollment, one-time login issuance, LOGON closure barrier,
  DMV quiescence and actual dbt/ordinary worker credential injection.
- ClickHouse physical enrollment, per-attempt writer closure, staged snapshot
  publication with atomic EXCHANGE and durable recovery intent.
- Genuine route qualification and the separately approved
  [nonproduction authority family](feature-specs/nonproduction-composition-authority.md)
  for the synthetic campaign. Existing production/native-v2 requirements retain
  their meaning; local synthetic receipts cannot be relabeled as production.
- The public authority-aware app factory/CLI and complete current/provider/SQL
  reconciliation run. Local ARM64 Docker is not the vendor-supported SQL Server
  container cell; use an isolated Linux x86-64 runner for that proof.

The base contracts are reviewable independently. These gaps remain requirements
for completing the feature and declaring downstream readiness.
