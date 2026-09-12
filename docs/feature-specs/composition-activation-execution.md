# Feature design: composition activation and execution

- Status: APPROVED
- Owner: dpone maintainers; root integration agent
- Base: 67c8ade61732fb1bff267ce3bd26f2af776e79e6 (published 0.75.0)
- Target release: TBD; publication authorization is separate
- Last verified: 2026-09-11

## Executive summary and authorization

The maintainer delegated implementation of a focused follow-up to verified release
composition: complete native plus supported ordinary physical admission,
activation/current/recovery, parent attempt fencing and actual synthetic
execution. This specification records that explicit implementation authorization, reaffirmed by the maintainer's approval on 2026-09-10. The earlier 0.75.0
publication approval does not authorize another publication.

The separately approved [nonproduction authority amendment](nonproduction-composition-authority.md)
adds an explicit scoped native family for actual synthetic execution. Existing
production/native-v2 authority remains unchanged.

The design preserves the exact native constituent, source verification and
v3 parent identity. Native-only admission is never authority for the parent.
Unknown capabilities or missing physical/session authority reject the complete
parent before reservations or draining the current occurrence.

## Personas and customer journey

Data engineers independently compile native dbt projects and ordinary transfers.
Platform engineers provision isolated protected control authority, bind every
logical connection, and prepare a complete immutable deployment. Operators
promote through the existing cache-sync/desired-state path, load all constituent
DAGs through the provider, trigger explicitly unscheduled synthetic DAGs, and
reconcile actual native and ordinary results. A successful composition build,
cache install, READY marker or launcher prepare is not SQL execution.

## Scope

The implementation is split into focused dependent PRs. The base coordinator
is not sufficient for downstream readiness; the complete required matrix is:

| Workload | Initial execution cell |
|---|---|
| Native dbt | Verified SQL Server execution-pack.v2 payload under production native-v2 authority or the explicitly scoped nonproduction family; protected per-attempt SQL connection gate |
| Ordinary transfer | PostgreSQL table extraction to MSSQL, full_refresh, generic target transaction, external target_atomic state |
| Native-generated transfer (required downstream) | MSSQL to ClickHouse full_refresh; protected physical ownership, Atomic snapshot publication and parent attempt fencing |
| MSSQL to MSSQL transfer | Deferred capability unless separately proven; not a substitute for the required ClickHouse cell |
| Mutation targets | Explicitly protected SQL Server and single-node ClickHouse services; complete writer footprints and ownership; reject unmodeled cluster topology |
| Other connectors/strategies/custom hooks/query-side-effects/nested/snapshot/backfill routes | Unsupported; reject the complete parent |
| Native-only v2 | Preserve existing wire, IDs, APIs and dispatch; no reinterpretation of held historical guards |

ClickHouse acceptance includes deletion of disappeared source rows, exact decimal/numeric, GUID and NULL semantics, and bounded transport bytes. Admission and recovery must cover both services; no single-SQLServer-only result is downstream ready.

Cross-constituent scheduling dependencies, zero-downtime handover, cross-database
atomic data execution, arbitrary connector support and production certification
are non-goals. SQL already committed is not undone by deployment rollback.

## Target public contract for backend integration

The base coordinator implements the contracts described in
[the integration reference](../composition-activation-contract.md). The app factory
and CLI option below remain target APIs for the dependent backend integration;
they are not advertised as available by the base PR.

Retain release-inventory/release-compose/build/materialize meaning. Add optional
`--workspace-authority-connection-ref` to public `dpone airflow cache-sync` and
route it through the same app-owned parent-aware coordinator factory used by
desired-state reconciliation. No authority reference continues to fail closed.
The existing desired-state authority already projects this protected reference.

The app factory must select a separate v3 coordinator after verified schema
admission; it must not pass a parent to the native-only coordinator. The additive Python composition root is
`dpone.app.composition_activation.build_composition_activation_coordinator(*,
cache_root: Path, authority_connection_ref: str, control_schema: str =
"dpone_control")`. It returns a separate `CompositionActivationCoordinator` with
`prepare`, `activate`, `require_active`, `begin_retirement` and
`finalize_retirement` occurrence operations. Existing native factories retain
native-only behavior. `DeploymentCacheMaterializer` accepts a separate optional
`composition_activation_coordinator` capability. Its absence cannot authorize v3.

The authority reference selects a verified connection-registry binding. Signed
connection properties pin `composition_service_id` (canonical UUID) and existing
`database_authorities`. Provisioning uses the reviewed SQL artifact installed by
the platform administrator on an isolated service, outside cache-sync. No CLI
argument asserts a physical identity, enrollment success or permission proof.
Control admission records use `dpone.composition-activation-request.v1` and
`dpone.composition-activation-receipt.v1`; attempt records use
`dpone.composition-attempt.v1`. They are distinct from native v2 authority.

Downstream CI invokes the existing producer/materialize/build/cache-sync/provider
path. It requires complete source membership, successful physical admission,
exact ACTIVE occurrence, all DAGs loaded, actual subprocess execution of every
workload, independently reconciled rows, and durable terminal evidence. Each
stage has its own observed status. A missing live cell is UNVERIFIED.

A bounded ordinary source `state` declaration must be admitted explicitly and
reconstructed by the existing producer-closure verifier. Runtime must not inject
an undocumented state policy. Preserve native v1/v2 and all existing integrity,
source descriptor, selected artifact trio and credential-context checks.

## Detailed algorithm

1. Read and independently verify the complete immutable v3 parent, native source
   snapshot, ordinary inventory and flat ownership closure.
2. Verify exact deployment/binding/connection-registry/credential-runtime subject
   and select the protected control authority. Resolve logical aliases only now.
3. Derive every real mutation footprint, including native intermediate/backup
   slots and ordinary target/staging/receipt effects. Validate protected stable
   service/database incarnation independently of alias, principal, engine version
   and credential material. Compare all cross-owner slots with one complete SQL
   catalog/collation observation per physical database.
4. Reject any physical collision, unsupported workload, ambiguous ownership,
   incomplete footprint or missing writer/session capability before mutation.
5. Persist an immutable complete parent activation request and original catalog
   evidence. Later lifecycle phases load that request and check stable identity;
   legitimate catalog changes must not change its admission subject.
6. Drain a predecessor before transferring overlapping resources. Close new
   admission, require all attempts terminated with closed reconnect authority and
   server-side quiescence, and atomically transfer exact guard epochs under a
   protected transaction. Reservations are non-expiring.
7. Require exact PREPARED before the existing CAS/current commit, then exact
   ACTIVE after it. A failure after transfer/pointer mutation is activation
   incomplete and preserves all request/projection evidence. Old pointer bytes
   do not imply an old retired occurrence can still execute.
8. At actual worker admission bind parent, activation, constituent, workload,
   pack, plan and Airflow attempt. A duplicate RUNNING attempt never starts a
   second executor. Unknown outcomes block overlapping work and ownership reuse.
9. Native SQL execution uses newly issued per-attempt SQL connection authority;
   ordinary execution additionally checks the parent fence inside the actual
   MSSQL target transaction before DDL/DML and before terminal receipt commit.
10. Close the per-attempt connection gate before proving all its SQL sessions and
    transactions have ended. Only then seal terminal evidence. Missing proof
    keeps non-expiring reservations and requires reconciliation.

## Native writer-session mechanism

Use protected per-attempt SQL Server LOGIN authority, with ephemeral credentials
issued only to the admitted executor through a resolver capability. Preserve
signed dbt source and macros. Never fall back to original target credentials.
Target database users may join only a preprovisioned bounded writer role; worker
principals cannot administer the connection gate or protected control tables.
Credentials stay in memory/child environment and are not written to evidence,
source, logs or committed fixtures. dbt profiles retain environment references.

The issuer must bind stable physical endpoints, journal its one-time principal
identity, fail closed on uncertain creation/enablement, and never reopen a gate
for a replayed RUNNING attempt. Closing disables reconnection first; a protected
server observation must then prove zero sessions/transactions for that issued
principal before releasing its writer scope. Process exit or a helper-connection
lock alone does not prove quiescence. Crash/unknown outcomes retain authority;
recovery closes the known gate and proves quiescence before a fresh attempt.

The first cell enrolls fresh, dedicated databases exclusively for v3. The
protected catalog pins service incarnation, database continuity, managed schemas,
bounded writer role and permission policy. Existing native-v2 databases are not
migrated automatically. Ordinary binding credentials have read-only authority;
only issued attempt logins gain the managed writer role. Enrollment must exclude
ambient writers, legacy worker credentials, impersonation, contained users and
Windows authentication fallback; a boolean exclusive flag is not sufficient.

## Kubernetes supervisor execution boundary

The maintainer selected the Kubernetes root-supervisor boundary on 2026-09-11.
The deployment projection gains an optional
`dpone.composition-supervisor.v1` object. It is mandatory for a v3 composition
and forbidden as authority for native-v2 or ordinary releases. It contains an
administrator-provisioned ReadWriteMany PVC claim and one reserved numeric
UID/GID range. The range must contain at least 1,000,000 identities, fit below
`2^31`, and be absent from the runtime image's accounts and platform workloads.
No workload pack, manifest, Airflow parameter or task environment may override
this object.

The provider projects the object only after verifying the current deployment
and v3 release authority. Its base container runs the dpone supervisor as UID 0
with a read-only root filesystem, `allowPrivilegeEscalation=false`,
`seccompProfile=RuntimeDefault`, and only `CHOWN`, `FOWNER`,
`DAC_READ_SEARCH`, `SETUID`, `SETGID`, and `KILL` capabilities. The fetched
worktree stays read-only. A memory-backed volume is mounted for one-shot dbt
profiles. The PVC is mounted at the fixed supervisor root and retains attempt
allocation tombstones, capture originals and recovery evidence across pod and
Airflow retries. Missing PVC, root identity, capability, tmpfs, complete
`/proc` visibility or reserved identity range rejects v3 before source or
subprocess I/O.

The supervisor allocates child identities under an exclusive PVC lock. It
derives a deterministic first candidate from the full attempt digest and uses
bounded deterministic probing within the reserved range. Separate immutable
attempt and UID/GID tombstones are fsynced before directory allocation. A
candidate already owned by another attempt is skipped; conflicting or
unreadable records reject. The same RUNNING attempt is rejected by protected
SQL admission before allocation and never receives a second executor permit.
Tombstones are not automatically deleted in this version. Reclamation requires
a separate reviewed operation after the parent is `RETIRED`, all attempts are
terminal, evidence is archived, and no process or transaction remains.

The verified launcher derives a composition marker only from the authenticated
v3 release bytes. Native-v2 and non-composition commands retain their existing
path. For v3, the root process alone receives control and source authority.
Native dbt runs as the allocated child with only issued one-time MSSQL
credentials. Ordinary transfer keeps its read-only PostgreSQL source and
receives issued MSSQL sink/state credentials through an invocation-scoped
dependency-injection overlay. The actual MSSQL finalizer must execute the
parent fence inside the target transaction before mutation and before receipt
commit. Process exit, filesystem ownership and generic `LoadResult` are never
OUTCOME proof.

Gate states are JOURNALED -> READY -> CLOSING -> CLOSED, with no reverse edge.
A controller-generated immutable SID and deterministic bounded login name are
journaled before creation. Login/user/role creation and READY commit together.
Credentials are delivered once after exact durable readback. Lost acknowledgement
never causes password reset or a second executor for the same attempt.
A synchronous server LOGON gate must serialize issued-login authentication with
closure; disabled-login plus one empty DMV snapshot is insufficient. Closing
requires independent disabled-SID readback, complete DMV privileges, zero sessions
identified by original_security_id, and zero attributable transactions. Names/SIDs
are retained and never reused. Any uncertainty blocks admission and retirement.
Attempt states are RUNNING -> SUCCEEDED/FAILED/COMMIT_UNKNOWN. An unknown SQL
outcome remains blocking even after quiescence until explicit reconciliation.

## Architecture

Canonical contracts own identities, capabilities, transitions and receipt checks.
Manifest readers own bounded complete source acquisition and reconstruction.
Narrow ports separate immutable inputs, physical observation, occurrence
persistence and issued connection gates. Runtime owns actual execution and
mutation integration; adapters own SQL control/catalog/session I/O. App roots
construct dependencies. Existing command/provider layers remain thin.

Use a separate v3 coordinator, no mode boolean or generic plugin registry.
Reuse source verifiers, cache CAS/saga mechanisms and the generic MSSQL target
transaction algorithm. Exclusive database enrollment must prevent legacy/native-v2/semantic-refresh
credentials from writing enrolled targets. Their existing guard keys and receipts
are unchanged outside enrollment; a new guard namespace alone is insufficient.

[ADR0059](../adr/0059-composition-parent-activation.md) extends ADR0058. No budgets, baselines or exclusions change;
new modules obey canonical max_sloc and coupling/architecture checks.

## Validation and synthetic environment

Required offline coverage includes complete-union capabilities, alias/principal/collation
collisions, helper footprints, stable admission evidence, lifecycle/CAS failures,
RUNNING replay, unknown commits, issuer crash windows, reconnect denial,
quiescence and native-v2 regressions. Existing tests remain authoritative.

The live acceptance path is current -> provider loader -> generated init-fetch ->
actual worker subprocess -> SQL data/evidence -> independent reconciliation.
It must exercise all constituents and retain exact C/environment/run/attempt and
row-count or deterministic content evidence. prepare is not execution.

The local Docker host is ARM64; SQL Server containers are officially supported
on Linux x86-64. A disposable GitHub-hosted Linux x86-64 synthetic job is the
preferred actual SQL environment. No existing business systems, credentials,
private names, URLs, tables, datasets or worktrees are inputs. If this isolated
cell cannot execute, report that concrete acceptance gap as UNVERIFIED.

## Documentation and rollout

Add an execution tutorial, exact support matrix/API reference, physical admission
and gate-provisioning instructions, lifecycle/recovery runbook and downstream CI
contract. Update composition operations/reference, architecture/ADR, CLI
reference, compatibility/changelog and navigation. Keep artifact-only tutorials
accurate. Explain drain-first availability gaps, operator recovery, explicit DAG
triggering, partial workload results and that rollback does not undo SQL.

## Approval and execution plan

Read-only explorer, architect, certification and docs/UX analyses are complete.
The root integrator is the sole writer in a clean worktree with a validated task
contract. Read-only agents provide bounded design and independent review. Any
future parallel writers require separate worktrees and disjoint path contracts. Shared
schemas, CLI factories, workflows, dependencies, changelog and navigation belong
to the integrator unless explicitly delegated. Fresh independent review and all
applicable broad checks are required before the PR is ready.

## Current official comparison

Sources checked 2026-09-10. These are adopted design patterns, not claims that
other products lack equivalent safeguards.

| System/source | Adopted | Rejected for this scope |
|---|---|---|
| [dlt 1.30.0 state](https://dlthub.com/docs/general-usage/state) | Explicit load/state identity and partial-commit recovery | Blanket atomic retry guarantees |
| [Airbyte jobs](https://docs.airbyte.com/platform/understanding-airbyte/jobs), rolling | Separate connection check, job and attempt | Treating successful CHECK as a completed data run |
| [Cosmos multi-project](https://astronomer.github.io/astronomer-cosmos/guides/multi_project/multi-project.html), 1.15.1 | Complete manifests and explicit project membership/dependencies | Inferring cross-project execution ordering from bundle membership |
| [gusty](https://github.com/pipeline-tools/gusty), 0.23.1 | Declarative task ownership | Runtime autodiscovery inside immutable constituents |
| Informatica, Fivetran, Pentaho, SSIS, Apache Beam | N/A: this change does not replace their ETL authoring/runtime engines | No comparative runtime performance claim |

The measurable target relative to dpone 0.75.0 is complete synthetic parent
execution from current/provider entrypoints with 100% expected/observed workload
membership and deterministic target rows. The procedure must retain exact source,
environment, attempts and reconciliation artifacts. Offline tests do not meet
this metric.

## Delivery sequence and downstream dependency

1. Complete, producer-verified parent source and capability contract, immutable
   physical admission subject, and typed occurrence/attempt state machines.
2. Wire protected backend admission, current/recovery and actual native/ordinary
   execution; exercise explicit identity/epoch/session failure boundaries.
3. Complete MSSQL-to-ClickHouse full_refresh admission and Atomic snapshot
   publication with parent-scoped fencing and recovery, plus the required type
   and transport budget reconciliation in isolated synthetic CI.

A focused base-coordinator PR may precede the ClickHouse integration PR. Neither
its merge nor a MSSQL-only synthetic PASS is acceptance for the required complete
downstream scenario. Public activation remains fail closed until all workload
capabilities in the selected parent are provided and verified.


## Native materialization evidence interpretation

The internal dbt outcome expectation's schema fingerprint identifies declared
column and type obligations from the verified source manifest. Empty or untyped
manifest declarations remain explicit absence of those obligations; they do not
assert an inferred complete physical schema. The actual SQL observer must prove
each selected target exists as the expected table or view and satisfies every
declared obligation, retaining complete bounded catalog originals under the
protected outcome producer. A matching fingerprint alone supplies no evidence.
Full fixture schema and data parity remain separate mandatory live campaign
checks. This interpretation preserves support for dbt models without enforced
column contracts and does not broaden their admitted write footprint.
