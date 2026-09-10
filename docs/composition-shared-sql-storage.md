# Shared SQL storage implementation contract

This developer reference implements the already approved
[shared physical ownership design](composition-shared-ownership.md) and
[ADR 0061](adr/0061-shared-composition-physical-ownership.md).
The first SQL schema-v2 implementation covers the shared journal, execution
store, login gates and historical proof checks. **Schema-v2 live qualification
remains unverified.** Existing scoped execution factories remain closed.
Qualification records participate in complete exclusion checks; qualification
issuance, source sealing and atomic ownership transfer still require their
protected implementations and campaign evidence.

Existing execution fingerprints normalize some strings, including backslashes.
Do not replace them with SQL HASHBYTES over the original document. Each original
family retains its decoder/hash algorithm and separate exact-byte comparison.
The derived qualification invocation key is specified in the shared ownership
design; it changes neither the original operation document nor execution keys.

## Mandatory schema-v2 core

All digest-text columns use `varchar(71) COLLATE Latin1_General_100_BIN2`.
Discriminators/state use binary collations and exact length checks to reject
padded variants. UUID SQL columns remain `uniqueidentifier`; do not introduce
new zero/version rejection for old control-service or execution-domain values.
New qualification records retain their own stricter pure validation.

| Table | Exact logical columns and constraints |
|---|---|
| `composition_authority` | Existing singleton PK=1, schema_version CHECK=2, service_id. Same expected control service and same actual database/principal. No automatic migration or parallel database. |
| `composition_owners` | owner_key PK; owner_kind closed execution/qualification; owner_id; subject_sha256; subject_document; state. UNIQUE(owner_kind,owner_id), UNIQUE(owner_kind,subject_sha256), and UNIQUE(owner_key,owner_kind,subject_sha256) for the operation FK. Execution states PREPARED/ACTIVE/RETIRING/RETIRED; qualification states PREPARED/ACTIVE/SEALING/SEALED/TRANSFERRED/RETIRED. Documents are bounded at 8 MiB execution, 1 MiB qualification. All identity/original columns immutable. |
| `composition_domains` | Existing guard_id PK, connector/service_id/physical_subject_sha256, fencing_epoch bigint>=0; replace owner_activation_id with nullable owner_key FK owners. Connector CHECK adds postgres only here. Preserve UNIQUE(connector,service_id,physical_subject_sha256). A non-null owner requires positive epoch. Identity immutable; only exact acquisition/release CAS can change owner/epoch. |
| `composition_owner_domains` | owner_key FK owners, guard_id FK domains, claim_document varbinary(max) bounded at 8 MiB, fencing_epoch bigint>0. PK(owner_key,guard_id); UNIQUE(guard_id,fencing_epoch); UNIQUE(owner_key,guard_id,fencing_epoch) supports exact operation partition FK. Immutable rows and original bytes. Indexed by guard_id for diagnostics, but that index is not the complete-history authority. |
| `composition_operations` | operation_key PK, operation_family closed execution/qualification, owner_key, owner_subject_sha256, replay_key, operation_document, state, closed_gates_sha256 NULL, quiescence_sha256 NULL, outcome_evidence_sha256 NULL. UNIQUE(operation_family,replay_key); UNIQUE(operation_key,owner_key); UNIQUE(operation_key,operation_family). Composite FK(owner_key,operation_family,owner_subject_sha256) references owners(owner_key,owner_kind,subject_sha256). Exact execution replay_key=operation_key; qualification replay key independently recomputed from its original. Documents bounded at 8 MiB execution, 1 MiB qualification. States RUNNING/SUCCEEDED/FAILED/COMMIT_UNKNOWN; terminal success/failure requires all three selected hashes non-null but this CHECK never substitutes for proof validation. |
| `composition_operation_domains` | operation_key, owner_key, guard_id, fencing_epoch bigint>0. PK(operation_key,guard_id). Composite FK(operation_key,owner_key) references operations; composite FK(owner_key,guard_id,fencing_epoch) references owner_domains. Immutable complete partition. No independently writable ownership state. |
| `composition_issued_authorities` | Existing connector/service/principal columns and global UNIQUE(connector,service_id,principal_id); replace attempt_sha256 with operation_key FK operations. PK(operation_key,connector,service_id,principal_id). First slice retains the existing closed MSSQL/ClickHouse principal contract; no invented PostgreSQL principal variant. Immutable rows. |
| `composition_proofs` | operation_key, operation_family with CHECK='execution', kind CLOSED_GATES/QUIESCENCE/OUTCOME, proof_sha256, proof_document bounded at 8 MiB. PK(operation_key,kind,proof_sha256); composite FK(operation_key,operation_family) references operations. Original existing CompositionAttemptProof decoder and hash remain authoritative. Immutable rows. |

The legacy duplicated activation-request and guard-epoch hashes on operation and
proof rows need not remain storage columns. Reopen the unchanged original attempt
and proof bytes and compare their existing parent/epoch fields directly. The
operation's owner_subject_sha256 equals the original execution request hash;
the composite owner FK prevents substituting another owner's subject. No new
common epoch-hash formula is invented for qualification.

Owner-domain claim_document has an explicit owner-selected decoder:

- Execution: retain the exact current `resource_document(CompositionPhysicalResource)`
  bytes. Expected rows come from original request.resources. Do not construct a
  new CompositionPhysicalClaim from an old resource. The legacy resource and
  domain types have different service-ID validation, including zero/legacy UUID
  behavior that this migration must not tighten.
- Qualification: expected rows come from the frozen owner's complete claims
  tuple and each claim's exact canonical to_dict representation. The owner
  original, not a scope digest or surviving partition subset, proves completeness.

The stored document partition is bounded before fetching/decoding, and full
original-byte equality includes length. SQL CHECK/FK constraints supplement
typed readback; they cannot authenticate a grant, a physical enrollment or a
business outcome.

## Catalog reference and controlled installation

The connection-owning boundary inspects complete table metadata on every
transaction. Its protected principal needs database and schema `VIEW DEFINITION`.
An effective metadata `DENY` on any current user-token principal rejects the
audit; table visibility alone cannot establish visibility of incoming foreign
keys or row-security policies in other schemas. Platform provisioning must also
exclude concurrent DDL, alternate writers and permission or trigger bypass.
Runtime catalog reads do not grant or repair those privileges.

The initial DDL sets `ANSI_NULLS` and `QUOTED_IDENTIFIER` explicitly. Fixed named
constraints and exact invariant trigger definitions bind the physical layout;
immutable originals also compare their byte lengths. The triggers observe the
already-held global transaction lock. They do not acquire it on a writer's behalf.
Only initial authority/domain enrollment has the documented bootstrap exception.
Runtime mutations use `OUTPUT INTO` because the tables have AFTER triggers.

SQL Server's stored CHECK expressions must be captured from the reviewed DDL in
a newly owned synthetic database. `tools/composition_mssql_check_catalog.py`
provides separate core and gate capture/render functions. They preserve original
expression text and reject an incomplete inventory, unexpected flags or a
different DDL digest. The checked-in reference modules are generated outputs;
never reconstruct their strings manually or adopt a runtime database as a
reference.

While these references are empty, the corresponding catalog audit rejects with
`control_schema_reference` or `login_gate_schema_reference`. The first controlled
capture run is therefore expected to fail admission. Retain its original JUnit
capture properties and failed result. Verify source, producer, server image and
version, database compatibility level, session SET observations and artifact
identity before feeding those originals to the generator. Commit the generated
references, then rerun all four exact SQL profiles from that commit. A successful
capture or an older schema-v1 pass is not a schema-v2 qualification result.

Use the [disposable component runner](composition-activation-contract.md#disposable-sql-component-check)
with profiles `store` and `gate` for the captures, retaining each original
`summary.json` and `junit.xml`. The core expressions are the JSON value of JUnit
property `dpone.composition.check_catalog`; its companion is
`dpone.composition.check_catalog_context`. The gate properties are
`dpone.gate.check_catalog` and `dpone.gate.check_catalog_context`. Missing or
unverified context blocks acceptance of a reference. Match these observations to
the source and image identities in that run's summary and original archive.

After verifying provenance, pass the decoded core JSON to
`tools.composition_mssql_check_catalog.render_reference` and the gate JSON to
`render_gate_reference`. Write their returned text unchanged to
`src/dpone/adapters/composition_mssql_check_definitions.py` and
`src/dpone/adapters/composition_mssql_gate_check_definitions.py`, respectively.
Keep the original captures alongside the generated-source hash manifest. The
next committed-source run must execute all `store`, `gate`, `trust` and
`registration` cases with no skips and successful cleanup.

## Optional MSSQL gate deployment and proof family

The optional deployment remains exactly login_gates, mssql_enrollments,
mssql_managed_schemas and mssql_gate_evidence, plus their exact existing server
LOGON and immutable/monotonic modules. Replace both gate and gate-evidence attempt
FKs with operation_key associations. Gate records additionally select execution
through a checked operation_family='execution' composite FK. Preserve original
SID, login name, gate state and evidence bytes. Preserve enrollment/managed-schema
FKs, immutable definitions and physical IDs.

The server LOGON barrier remains common for every current v3 issued name and
retains exact SID/name/READY checks and its current synchronous lock behavior.
The monotonic gate trigger joins by operation_key after migration. The v3 prefix
and existing execution principal generation are unchanged. There is no new
qualification credential prefix or branch that bypasses this barrier.

Base `begin`, owner preparation and trust/registration reads do not require an
uninstalled gate deployment. The store7 live fixture explicitly has no invented
issued gate/quiescence proof and remains a base-only component.

Every actual gate entrypoint requires the complete gate catalog and installed
server policy. Terminal proof validation and ownership release that encounter
an MSSQL issued principal also require the complete gate group and its exact
associated SID/name/enrollment/evidence rows. The immutable base issuance row
therefore anchors this dependency even if all optional gate objects disappear.
Do not decide that proof requirements are optional because a table is absent.
Unrelated base-only/TRUST work requires no gate catalog. No generic capability
registry or extra deployment marker is necessary for these current closed paths.

First-slice qualification proof dispatch is unavailable, and the proof table
does not claim a schema for it. Qualification terminal labels, empty authorities,
opaque digests or unknown proof families cannot pass terminal validation.
Even a qualification owner labelled RETIRED/TRANSFERRED remains an overlapping
blocker because this slice has no trusted qualification retirement/transfer
proof. Unsupported originals are never decoded as execution or returned as None.

A later source_seal proof must explicitly certify bounded read-only observation
and access closure, without a fake writer principal. Adding that proof family
requires a reviewed codec/producer, explicit catalog/schema evolution and fresh
exact schema evidence. It cannot be slipped into v2 via an unknown-family
fallback or an empty execution-authority set.

## Frozen transaction-bound kernel interfaces

Root retains all existing SQL mutations, renderers and entrypoint coordination.
The bounded kernel writer owns the two new owner/operation modules and their
complete readers/history auditors. It creates no second writable repository.

Root supplies one minimal `ports/composition_sql.py` protocol:

```python
class CompositionSqlContext(Protocol):
    cursor: SqlControlCursor
    schema: str
    def table(self, name: str) -> str: ...
```

This is transport context, never lock authority. Existing CompositionMssqlLedger
implements it structurally at its existing import path. Trust/registration may
retain their exact Ledger-type check in addition to actual SQL checks. Core
functions construct or validate canonical table names using validated schema;
arbitrary context.table output cannot select another authority table.

Every public kernel function takes keyword-only expected_service_id from the
private root's configured pin and observes real @@TRANCOUNT>0, XACT_STATE=1,
APPLOCK_MODE(public,fixed resource,Transaction)=Exclusive, the exact authority
v2/service row and actual transaction_id. No successful-begin flag, copied context
or caller assertion is authority. Helpers never begin/acquire a transaction or
lock, open a connection, commit or roll back. The root boundary performs the
mandatory exact catalog audit; every kernel entry rechecks actual transaction
and authority. No untrusted DDL/trigger-bypass permission is introduced.

Nonproduction trust keeps its existing payload version 1 while its enclosing
control authority requires version 2. A trust read compares the actual transaction
before and after the optional catalog and original revision read. Registration
retains that transaction across its entire invocation, including injected clocks,
streamed history pages, each append and all successful return paths. Before each
append and successful exit, it also reopens the latest complete trust revision;
equal policy bytes at a newer revision still reject. Historical grants retain
their original revision/time validation. An externally managed caller must roll
back on failure, including any partial appends, then follow the original commit
and independent readback protocol. These checks create no worker authority.

The execution proof dependency is a root-injected narrow callback:

```python
ExecutionTerminalValidator = Callable[
    [CompositionSqlContext, CompositionActivationOccurrence, CompositionAttemptReceipt],
    None,
]
```

It reopens complete issued-authority and original proof records in that SAME
protected transaction, including the installed MSSQL gate policy and exact
gate/SID/name/enrollment/evidence associations when MSSQL was issued. It is
execution-only; it never treats a caller DTO as proof or dispatches qualification
terminal variants. Kernel checks preserve actual transaction_id across callback
execution. A callback may not commit, roll back, rebegin or release the lock.

Root changes existing gate readers' parameter-only concrete Ledger dependencies
to this context port, or owns a narrow cursor/schema-only extraction. Thus the
callback implementation imports neither the new kernel nor concrete Ledger.
This is the smallest DI seam; no generic family registry, universal repository
or service locator is required. The historical MSSQL evidence body remains
`dpone.composition-mssql-gate-observation.v1` with its exact CLOSED_GATES and
QUIESCENCE fields, bytes and hashes. Every gate-evidence row/FK and exact original
byte readback migrates together.

New `adapters/composition_mssql_ownership.py` exposes:

```text
read_shared_owner_in(context, reference, *, expected_service_id)
    -> SharedOwnerRecord | None
iter_shared_owners_in(context, *, expected_service_id)
    -> Iterator[SharedOwnerRecord]
read_shared_domain_in(context, resource, *, expected_service_id)
    -> SharedDomainRecord
```

reference is CompositionOwnerReference. resource is exactly an existing
CompositionPhysicalResource or new CompositionPhysicalClaim. Validate each
explicit family; never convert a legacy resource to a stricter claim.

Exact adapter-internal immutable return fields:

```text
SharedOwnerRecord:
    reference: CompositionOwnerReference
    subject: CompositionActivationRequest | CompositionQualificationOwner
    subject_document: bytes
    subject_sha256: str
    state: str
    guard_epochs: tuple[tuple[str, int], ...]

SharedDomainRecord:
    guard_id: str
    connector: str
    service_id: str
    physical_subject_sha256: str
    fencing_epoch: int
    owner: CompositionOwnerReference | None
```

Decode only the explicitly supported original families. Do not enable NP
execution acquisition merely because its request can be structurally parsed;
the complete source attachment/admission remains a later capability. Reads
compare complete originals, physical enrollment and every expected partition.
None means actual absence after valid catalog/identity checks, never unsupported
family, missing partition or corruption.

New `adapters/composition_mssql_operations.py` exposes:

```text
read_shared_operation_in(context, operation_key, *, expected_service_id)
    -> SharedOperationRecord | None
iter_shared_operations_in(context, *, expected_service_id)
    -> Iterator[SharedOperationRecord]
require_execution_history_in(context, request,
    *, expected_service_id, terminal_validator) -> None
require_execution_attempt_in(context, attempt,
    *, expected_service_id, terminal_validator) -> CompositionActivationOccurrence

SharedOperationRecord:
    owner: SharedOwnerRecord
    identity: CompositionAttemptIdentity | CompositionQualificationOperation
    identity_document: bytes
    operation_key: str
    replay_key: str
    state: str
    closed_gates_sha256: str | None
    quiescence_sha256: str | None
    outcome_evidence_sha256: str | None
```

request is the existing CompositionActivationRequest; attempt is the existing
CompositionAttemptIdentity. The attempt checker returns the exact observed
ACTIVE occurrence after complete shared history, scope and replay checks. It
does not insert, commit or issue a permit. Root's existing reserve helper inserts
RUNNING and every operation partition immediately afterwards in this same
protected transaction, alongside authorized budget reservations. The history
checker audits every original before overlap decisions; all overlapping
qualification states remain blocked. Original execution terminal proof is
validated through the injected callback, never inferred from selected hashes.

Both iterators use complete TOP(1) keyset pagination and bounded memory. The
first page is unfiltered so malformed low/empty keys cannot be skipped. Validate
the full row and canonical key, reopen exact originals/partitions, retain the
strictly monotonic key BEFORE yielding, and stop only on an actual empty next
page. Never return a tuple containing unbounded 8 MiB originals, collect a full
history cache, use a maximum-history limit as success or start from a surviving
guard partition join. Consume the complete fetched row before yielding so proof
callbacks may safely use the same cursor.

Capture the actual transaction_id at first consumption and recheck the same
transaction plus live lock/authority before subsequent pages and after terminal
callbacks. Resuming a lazy iterator after commit/rebegin fails even when the new
transaction reacquires the same lock. Global admission becomes streaming folds
over the full owner/operation history; reopen a needed parent by key rather
than retaining every original in a dictionary. These extra queries preserve
bounded memory and original-scope completeness.

Ownership imports no operations module. Operations imports ownership. Context
and callback types live in ports below both. Root owns existing prepare/CAS/
release/reservation/finalization coordination and the concrete terminal verifier.
No internal return record itself is a capability or durable acknowledgement.

## Complete audits and transaction ordering

1. A connection-owning boundary sets the existing session/transaction policy,
   enters a live transaction and obtains the existing Exclusive transaction-owned
   application lock `dpone:composition-control:v1`, timeout zero, same database
   and `public` database principal. Check actual @@TRANCOUNT>0, XACT_STATE=1 and
   APPLOCK_MODE before protected same-ledger work. A Python class/type or cached
   successful begin flag is insufficient. Microsoft identifies an application
   lock by [database, principal and resource together](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-getapplock-transact-sql?view=sql-server-ver17).
2. Audit exact authority v2/service and mandatory base catalog before mutation.
   Compare complete columns/types/lengths/collations/nullability, key order and
   uniqueness, disabled/ignore-duplicate flags, exact CHECKs/defaults, trusted
   non-cascading FKs, immutable/transition modules and events, unexpected triggers,
   row-security predicates and unsupported table forms. Eight-table counting is
   insufficient. Explicitly reject composition_activations,
   composition_activation_domains, composition_attempts and
   composition_attempt_domains alongside v2, including compatibility objects
   under those names. Their absence is part of the no-dual-writers boundary.
   Missing/partial/old layouts reject; runtime repairs nothing. Optional NP
   trust/registration groups and the MSSQL gate group remain separately audited
   capabilities; absence does not invalidate base-only work.
3. Global scans originate from owner and operation originals, independently of
   current domain pointers and partition joins. Reopen every original and require
   exact complete partitions before considering guard intersection. Preserve the
   existing globally scanned execution-attempt semantics and strengthen them to
   include qualification. A deleted partition cannot hide an operation's scope.
   Bounded keyset pages may control memory, but must consume the complete history
   under the same lock; no limit-as-success or selective join is allowed.
4. Compare every domain's original physical identity, current pointer and bigint
   epoch with immutable owner history. For a newly provisioned domain epoch0 has
   no acquired history; acquisition advances by exactly one. Historical
   (guard,epoch) uniqueness and the complete original scopes expose missing/extra
   partitions and stale/contradictory ownership. Release never resets epochs.
5. Prepare requires all selected current domains unowned, no overflow, and safe
   original mixed-family history. Insert the original execution PREPARED owner,
   CAS each enrolled domain from exact old epoch/unowned to next epoch/new owner,
   append the complete owner partition and reread it, all in one transaction.
6. Reservation checks exact ACTIVE execution owner, actual existing attempt scope
   and every mixed-family original. Insert RUNNING and complete operation domains
   in the same caller transaction as NP budget reservations. Any operation or
   replay-key conflict rejects; do not read a prior row and return admission.
7. Commit once only at the owning boundary. Independent exact readback follows
   existing acknowledgement rules. Lost/uncertain acknowledgement supplies no
   fresh worker permit and triggers no mutation retry. Helpers never create a
   connection, commit, rollback or change caller transaction ownership.
8. Each issuance phase repeats current execution owner/RUNNING operation/epochs
   and full original/principal checks in its own protected transaction. Moving
   gate/proof storage to operation_key must not bypass journal/create/READY/
   independent-readback ordering. Actual workload execution never holds the
   global control lock for its duration.
9. Recovery closes only the exact retained issued principal and does not need
   a new ACTIVE admission, replacement credential or valid new grant. Ambiguous
   originals/epochs/closure retain ownership. Gate closure and quiescence alone
   never settle COMMIT_UNKNOWN; the original and reconciled outcome proofs remain
   independently audited before state change or release.
10. Retirement audits complete mixed owner/operation history and all selected
    issuance/proof/auxiliary originals, then exact release CAS and RETIRED state
    transition occur together. No qualification state is accepted as harmless
    overlapping history while its terminal/handoff dispatcher is unavailable.

Schema modules must enforce immutability of authority/originals/partitions/
issuance/proofs, monotonic domain transitions and closed execution state changes.
Qualification updates/transitions are unavailable. Neither an allowed SQL state
label nor a raw trusted-controller insert is evidence of business closure.
Provisioning must exclude trigger bypass, bulk-copy without trigger firing and
ALTER/TRUNCATE by runtime/worker principals, as existing trust guidance requires.

## Failure semantics and caller migration

Use existing CompositionAdmissionError and value-free reasons. Preserve existing
execution reasons where behavior is unchanged. New owner/operation corruption,
unsupported family, missing partition or identity mismatch is a failure, never
None, an empty closure, a zero count or a successful terminal result. None is
reserved for genuine absent identity after a valid complete catalog/read.
Known conflicts reject before issuance. Driver failure after a durable boundary
retains the existing unknown result semantics; diagnostic readback cannot invent
a fresh acknowledgement. No same-ledger helper catches an error and continues
with a partial operation.

Root owns integration into the previously censused paths: store_queries and
store lifecycle; all attempt reads/reservation/finalization/reconciliation;
all login journal/READY/close/quiescence phases; enrollment selection; gate-proof
and gate-evidence associations; trust/registration exact version and transaction
checks; and the existing activation coordinator/snapshot port obligations.
Concrete ClickHouse authority/store/executor enforcement is still absent and
must not be claimed from port documentation. Every future concrete claim/dispatch
must use the shared owner/operation authority. No public factory opens here.

The initial install remains fresh-only v2 on isolated physical incarnations.
Old v3 binaries reject v2; this kernel rejects v1/partial layouts. No writable
legacy activation/attempt tables, view fallback, dual write, second control DB,
new physical namespace or native-v2 migration is part of this slice. Future
offline migration and qualification proof evolution need explicit work and
cannot discard retained originals, epochs, principals, trust or charges.

## Frozen integration boundaries

`dpone.contracts.composition_persistence.encode_physical_resource` owns the
unchanged `canonical_json_bytes(asdict(resource))` encoding. The existing
`composition_mssql_store_queries.resource_document` name re-exports this same
function. The shared owner reader imports the pure serializer, preventing a
cycle back through the concrete ledger. This extraction adds no stricter legacy
resource validation.

`dpone.adapters.composition_mssql_transaction.require_shared_transaction_in`
takes the SQL context, keyword-only `expected_service_id` and optional prior
`transaction_id`, and returns the actual positive bigint transaction ID.
The expected service remains a canonical UUID with existing zero-UUID behavior.
A prior ID is a continuity comparison, never cached transaction authority.
The helper observes committable state and the exact transaction-owned lock
before reading the current authority version/service. Invalid/doomed state
never invokes transaction-owned APPLOCK_MODE. It constructs the authority table
from the validated schema rather than trusting arbitrary context.table output.
The owning boundary separately audits the complete catalog before shared reads.

Use CURRENT_TRANSACTION_ID() for this session's ID. Microsoft permits every user
to observe its own transaction through that function; the equivalent DMV can
require broader server visibility. The initial target remains the pinned SQL
Server 2022 fixture, with actual low-privilege behavior requiring live evidence.
[Microsoft function reference](https://learn.microsoft.com/en-us/sql/t-sql/functions/current-transaction-id-transact-sql?view=sql-server-ver17).

Terminal selectors stream all original proof variants through complete TOP(1)
keyset scans, with an unfiltered first key for each closed proof kind, canonical
monotonic keys and the same pinned transaction. They retain only the selected
triplet, never a complete original-proof history. Exact selected-proof queries
are bounded. Read at most 8,193 issued rows to detect and reject overflow beyond
the existing 8,192-authority contract; a truncated subset can never pass.
Owner and operation partitions likewise use their original family bounds plus
one overflow row and SQL byte bounds before fetching large documents.

The execution terminal callback reopens original quiescence evidence and the
current irreversible gate/SID/policy associations. It never calls
observe_quiescence on historical business databases: legitimate successor work
must not invalidate an earlier observed and durably retained terminal proof.
Missing original evidence or a changed barrier still rejects.

`require_execution_attempt_in` is for fresh reservation only and rejects every
existing operation/replay key. One root-owned existing-operation guard serves
journal/create/READY/readback/close/recovery. It first reopens the exact original
operation, owner and partitions, then performs the complete mixed-family audit
while excluding only that independently observed identical operation for a
nonretired parent. An independently reopened RETIRED parent instead permits
structurally valid PREPARED/ACTIVE/RETIRING execution successors and their current
operations. Every operation under an overlapping RETIRED execution owner still
requires complete original terminal closure; nonterminal operations under any
RETIRED execution owner and overlapping qualification remain blockers. An untrusted skip
digest or caller-supplied admission/recovery mode is not accepted.
It returns the observed existing occurrence and receipt; each existing entrypoint
retains its explicit state requirements. Issuance needs ACTIVE/RUNNING; close and
recovery require no fresh admission, renewed grant or replacement credentials.
History and terminal reads remain distinct from new issuance.

`require_retired_execution_in(context, occurrence, *, expected_service_id,
terminal_validator)` serves historical activation readback. It independently
reopens the exact RETIRED owner and complete global original history, validates
all operations of overlapping retired owners and rechecks the same transaction
and original owner after proof callbacks. A legitimate execution successor does
not turn historical readback into fresh acquisition. Retirement mutation still
uses the separate shared exclusion check before releasing any physical guard.

Malformed or missing original partitions reject globally. Well-formed unsupported
qualification history blocks overlapping resources while unrelated physical
scopes remain available to legitimate execution. Test both outcomes explicitly.

## Validation and deployment

Run the complete frozen store 7, gate 16, trust 9 and registration 18 inventories
from the exact clean integrated version on newly isolated Linux SQL instances.
Earlier schema-v1 passes are historical evidence. Focused compatibility tests
must preserve all old request, attempt, proof and physical-resource bytes and
replay behavior, including historical terminal evidence. Every issuance phase,
lost acknowledgement, mixed-family overlap, deleted partition, changed catalog
and transaction replacement needs explicit negative evidence.

Required static, architecture, layer, module-size and documentation gates remain
unchanged. No baseline or threshold is relaxed. Qualification lifecycle/proofs,
source sealing, handoff and actual worker campaign require their subsequent
implementation and exact evidence; this storage reference does not enable them.
