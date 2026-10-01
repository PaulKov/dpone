# MSSQL authority for ClickHouse cluster publication

- Status: APPROVED
- Owner: dpone maintainers
- Target release: next compatible feature release after exact-head acceptance
- Last verified: 2026-09-30

The maintainer approved the written design and its Native implementation plan
on 2026-09-30. This public specification records the generic contract without
deployment coordinates. Approval is not implementation or live certification.

## Implementation availability

This branch currently provides the unactivated binding, catalog admission,
native SQL authority adapter, shared normal/replay runtime composition, and an
endpoint-admitted schema plan/apply/inspect service with a thin draft CLI.
The branch accepts the closed selector in single, batch, flow and folder
authoring; this is unreleased and does not migrate any existing workload.
An internal native prepared-recovery service reuses normal publication
reconciliation and cleanup through that same provider. Its application now binds
verified context and both endpoints to private plans; draft recovery CLI parsing
fails closed without an injected trusted observer. Guarded legacy retirement
now has separate plan/apply/verify application and draft CLI composition, bound
to an existing SQL catalog and a fixed private attempt journal. Both operator
journeys still require deployment-observer integration and end-to-end ClickHouse
acceptance before activation; standalone commands cannot create that authority.
Do not configure this option against a released runtime.

The local opt-in test
`tests/integration/clickhouse_cluster/test_mssql_publication_authority_live.py`
uses a fresh uniquely named database on local SQL Server only. It verifies
concurrent create/CAS, lost commit ACK, rollback after emitted results, duplicate
write identity and event immutability. This is **SQL authority evidence**, not a
ClickHouse route certificate or proof that all deployment writers were drained.
The fixture retains its isolated database for inspection; it does not alter
existing catalogs or server settings. The runner requires explicit local test
credentials in memory/environment, never a production connection.

## Purpose and journey

Operators with an existing SQL Server metadata service need durable coordinated
ClickHouse publication without enabling a new ClickHouse table engine. A
platform engineer provisions a versioned catalog, admits one binding across all
writers of a target, plans a legacy adoption if needed, then observes normal
publication and source-free recovery. A workload author uses the supplied
connection reference, not credentials. A reviewer verifies exact operation,
generation, original quality, commit and DDL evidence before reporting success.

## Scope and public contract

Provide an MSSQL implementation of `ClusterPublicationAuthorityPort` with
read/create/CAS, readiness validation and immutable publication history. Reuse
the existing ClickHouse DDL correlation and reconciliation; do not create a
second publication algorithm. Normal, prepared recovery and quality replay
share the same backend. State/checkpoint and audit adapters retain their own
contracts; their tables are not repurposed as publication envelopes.

The new optional `sink.options.publication_authority` object has exactly six
required nonempty strings: `backend` (only `mssql`), `connection_ref`,
`database`, `schema`, `service_id`, and `environment`. Identifiers are validated,
not SQL fragments. The selected connection must resolve to MSSQL and the
declared location must agree with its admitted registry metadata. The factory
resolves publication credentials independently of disabled xmin state. Old
manifests keep their old behavior outside explicitly migrated target scopes.
Explicit selection never falls back to ClickHouse legacy authority.

### Authoring and deployment closure

For an admitted internal replicated ClickHouse `full_refresh`, add this options
fragment to the existing sink (all names below are synthetic):

```yaml
publication_authority:
  backend: mssql
  connection_ref: metadata-main
  database: Example_Metadata
  schema: ops
  service_id: example-service
  environment: test
```

Supply the existing bounded `sink.strategy.max_source_bytes` and internal
replicated `physical_design` as well. The selector alone does not admit a route.
Do not use it on local/external replication, an unbounded load, or
`partition_replace`: the latter is not the cluster full-refresh publication
protocol. With no selector, existing strategy behavior is unchanged. Closed
dbt publish policy versions do not gain this option implicitly.

Before packaging, configure the logical alias in the deployment connection
projection even if source checkpoint state is disabled. Both environment and
secret-volume projections require this independent dependency. The connection
registry must declare the same MSSQL database/schema and the reviewed endpoint
policy described below; runtime admission does not create or repair the catalog.
Do not put credentials in the selector.

Compilation preserves the selector in the effective load options. Runtime
rejects a missing or conflicting normalized copy before resolving credentials,
and verifies the environment against trusted deployment context. Source/root
placement, null, unknown binding fields and noncanonical connection aliases are
errors, not requests to use the legacy backend. Replay configuration identity
is checked only after semantic validation. Static schemas reject control
characters and whitespace boundaries; semantic validation also enforces
Python's full Unicode printable-character rules.
Replay configuration identity
includes backend, service, environment and database/schema; the logical alias
is excluded because resolved physical authority is separately endpoint-bound.
Alias rotation therefore preserves identity only when the actual admitted
endpoint and storage namespace remain unchanged.

Slot identity binds stable service ID, environment and existing target key.
Changing an alias or rotating credentials does not create a different slot.
Binding identity additionally binds the resolved non-secret SQL endpoint and
database/schema. Endpoint drift requires a reviewed cutover, not an empty new
authority. Service identity is deployment-governed, not guessed from DNS aliases.

The verified connection registry carries a non-secret `publication_authority`
policy with `service_id`, `environment` and `endpoint_identity_sha256`. Catalog
setup must review this pin independently of runtime alias resolution. The
endpoint digest is canonical SHA-256 over contract
`dpone.mssql-publication-endpoint.v1`, SQL `SERVERPROPERTY('ServerName')`,
`DB_NAME()` and the canonical database GUID from `sys.database_recovery_status`.
The runtime checks it on the catalog observer and on every dedicated SQL session.
An unavailable identity observation or a changed endpoint fails closed; rotating
credentials or aliases does not change the storage slot. Failover that changes
the admitted server identity requires reviewed re-admission, not automatic trust.

Runtime connection resolution selects this reference even with disabled xmin
state. SQL catalog admission occurs before constructing source/sink objects.
The normal publication service, durable-quality store and cloned sink share an
explicit provider with `for_database` and read-only readiness; no selected MSSQL
path constructs the legacy ClickHouse authority bootstrap. Independent sessions
avoid committing or rolling back a caller's business transaction. Legacy
manifests retain their prior composition. Unsupported local/external or non-full-
refresh selections fail before transport instead of silently falling back.

The draft branch implements `publication-authority schema plan|apply|inspect`;
`adopt plan|apply` remains proposed. They delegate to application services shared
with Python.
Plan is read-only; apply requires the exact plan digest and environment
admission. Outputs are versioned, redacted JSON with ready/blocked/in_progress/
completed, reason codes and identity digests. Exit 0 means requested mode
completed, 2 a proven block, 1 operational failure/unknown. Plan files are
atomic and refuse overwrite by default. Schema commands are not released yet;
adoption/retirement commands and workload activation remain unavailable.

The internal plan-file adapter writes bounded owner-private files on a local
POSIX filesystem: flush the full temporary file, link its final name without
replacement, then flush the parent directory. A failed final durability
acknowledgement preserves the completed file for readback; it is not permission
to overwrite or retry automatically. Reads reject symlinks, non-regular files,
shared permissions, extra hard links and observed concurrent changes. This
adapter does not authenticate an operator plan or replace apply-time validation.

### Catalog setup implementation boundary

The internal schema plan binds the complete storage binding, admitted endpoint
digest, catalog version and SHA-256 of the generated SQL. Its bounded canonical
codec rejects unknown fields, duplicate/noncanonical encodings and unsupported
versions. The plan carries no executable caller-supplied SQL or credentials.

Schema apply uses the same endpoint-admitted dedicated session construction as
runtime authority access, but does not require the catalog to exist beforehand.
It requires exact plan confirmation before session creation. The connected
database must match the binding; database metadata visibility and the existing
schema must be available. A transaction-owned application lock serializes
cooperating provisioners for that catalog. This is ordinary SQL session behavior,
not server configuration or exclusion of privileged administrators.

Only a completely absent catalog is provisioned. The generated table and trigger
batches execute in one owned transaction; `GO` separators are never sent to the
driver. Exact structural admission uses that same transaction before commit.
An existing exact catalog is verified without DDL; partial, wrong-kind or drifted
objects block without repair. No principal, privilege, database, schema or server
setting is created or changed by this service.

Failure or lost commit acknowledgement yields `outcome_unknown`, never success
or an automatic second attempt. Explicit read-only inspection can establish
whether the exact catalog exists. It cannot authorize a workload cutover or
legacy retirement. Local SQL tests exercise concurrent setup, rollback after
both table creations and loss of the acknowledgement after a real commit.
The draft CLI now composes this service from the exact init-fetch-pinned runtime
connection context. It has no independent manifest, raw connection string,
caller-supplied SQL or arbitrary binding JSON fallback. The trusted deployment
runner supplies `DPONE_RUNTIME_CONNECTION_CONTEXT` and its pinned init-fetch
plan exactly as for runtime execution. These local artifacts do not themselves
establish deployment signature verification: that remains the runner's existing
strict init-fetch admission responsibility. Do not run against self-authored
replacement context files.

The logical connection reference resolves through the admitted binding set and
registry. Database, schema, service namespace and endpoint pin come from that
descriptor; the requested environment must exactly match the verified context.
Planning resolves credentials in memory and performs read-only SQL admission,
then atomically saves the canonical plan only for an absent or exact catalog.
The plan contains no credential values. A changed binding, endpoint pin or DDL
version rejects an old plan before SQL execution. Apply validates the explicit
digest and saved environment before reading the context or resolving credentials.

Unreleased operator examples, inside the deployment-controlled execution context:

```bash
dpone publication-authority schema plan --connection-ref metadata \
  --environment test --plan-file ./publication-schema.json
# Review the private file and the returned plan_digest before applying.
dpone publication-authority schema apply --environment test \
  --plan-file ./publication-schema.json --confirm-digest <reviewed-sha256>
dpone publication-authority schema inspect --environment test \
  --plan-file ./publication-schema.json
```

All three commands emit `dpone.publication-schema-result.v1` JSON. Exit 0 means
read-only readiness/inspection or acknowledged catalog setup, never workload
publication or cutover. Exit 2 denotes an established scope/catalog block; exit
1 denotes an operational failure or unknown outcome. After a lost apply response,
use `inspect`, not an automatic repeated apply. `ready/catalog_absent` means the
catalog does not exist; only `completed/catalog_exact` proves exact readback.
Plan file errors may leave a durable file; preserve and inspect it rather than
overwriting it. Driver errors, credentials and local paths are not printed.

## Algorithm and transaction boundaries

### Native preparation admission for recovery — implementation refinement

Recovery cannot infer strict origin from a caller-supplied write UUID, a boolean
capability or a fixed initial revision. The SQL adapter's read-only
`read_native_preparation(target_key, operation_id)` capability reads the current
slot/root and the earliest immutable event for that operation in one owned,
acknowledged transaction. It admits only a native PREPARED event, validates its
exact predecessor transition (including a completed slot or authenticated
retirement), and binds it to the current operation, generations and SQL binding.
An initial preparation has revision one; later preparations need not.

The result carries the admitted binding digest, original versioned preparation,
current versioned envelope and server-recorded UTC preparation time. These are
observations, not a dispatch permit, a quality receipt or proof of absent DDL.
Retired IDs remain prohibited even after a later operation completes. Missing,
conflicting, malformed or uncertain history yields no admission and no retry;
reused operation IDs cannot substitute a different generation. Independently
authenticated quality and complete DDL observations remain recovery gates.
The capability is internal and unactivated until shared recovery composition
and end-to-end acceptance are complete; no serialized envelope field changes.
The query compares operation ID length as well as its binary-collated value;
SQL trailing-space equivalence must not choose another operation's origin.
The current revision must allow every mandatory phase transition. Later quality
capsule updates may advance the revision without changing its original core;
observing that core is not an authentication of the policy or its acceptance.
The read returns a bounded receipt, but finding the earliest operation event
may scan retained history for that target. A concurrent cooperating CAS writer
waits for the read transaction to release the current-slot lock.

### Shared native recovery service — internal checkpoint

`PreparedRecoveryService` receives the selected native authority provider, the
existing ClickHouse catalog/DDL capabilities and a mandatory deployment-owned
`PreparedRecoverySafetyObserver`. It does not construct a Keeper backend or
resolve arbitrary caller connections. There is no permissive observer default.

Planning is read-only and requires the exact current native PREPARED version.
The plan binds the original SQL time/payload, endpoint-inclusive binding digest,
target, inventory and deterministic publication intent. Its digest is a scope
confirmation, not an admission credential. Execute checks confirmation before
service I/O, re-admits the provider and re-reads native origin before and inside
the held safety interval. Stored evidence never replaces fresh observation.

The internal `PreparedRecoveryOperatorPlan` wraps that unchanged inner digest
with the full authority binding, SQL endpoint pin, verified connection-context
subject, logical sink reference and non-secret sink endpoint identity. Its outer
confirmation cannot be substituted for the inner service confirmation. The
bounded canonical codec preserves UTC microseconds and exact original revision;
it rejects duplicate/unknown fields, coercion, changed intent and incomplete
saved observations. It checks historical evidence at its saved boundary, so an
expired plan remains readable but grants no fresh safety. No credentials, SQL,
observer or dispatch permit is serialized. The SQL adapter and operator reuse
the same pure envelope decoder; the existing runtime import remains compatible.
The bound provider preserves the selected native capability on one admitted
handle, with unchanged locking and failed-admission invalidation. The codec and
scope wrapper supply no deployment observer.

### Native operator composition — unreleased, deployment adapter required

`PreparedRecoveryApplication` composes the real catalog/DDL adapters and shared
recovery service. The deployment runner explicitly injects the trusted held
observer. Without it, planning returns `blocked/held_recovery_observer_required`
before context loading, credential resolution or connector construction. No
observer, import path, freeze boolean or arbitrary SQL is accepted from CLI
arguments, environment variables or saved JSON.

The read-only planning journey takes authority and sink logical references, exact
environment, cluster/database/target, operation ID, native preparation revision
and a new private plan path. Both references resolve only through the init-fetch
verified context. The authority location, service and endpoint pin come from the
closed registry policy shared with catalog setup. Context subjects must have the
loader's exact `sha256:<64 lowercase hex>` format; the plan stores those 64 hex
digits, not a caller-supplied subject.

The owned ClickHouse connection must use native transport and the selected
registry database. Its non-secret endpoint projection includes contract version,
native transport, host, effective port/TLS, server UUID, database name and Atomic
database UUID. UUIDs are canonicalized, nonzero and observed read-only. This
endpoint digest binds confirmation; it is not exclusion evidence. The same handle
serves catalog observations and DDL. Planning closes it before publishing the
exclusive owner-private plan. No business source is constructed.

Execution decodes bounded canonical bytes and checks the **outer** confirmation
and requested environment before credentials. It verifies the current context
subject and registry authority scope, then re-observes the physical target
endpoint before native authority access. The application passes the saved inner
plan/digest to `execute`, never calls `plan` again, and therefore can reconcile
already advanced or completed phases. All effects still require fresh service
admission and held observation. It returns `completed` only after the service's
terminal proof and successful target disposal. A close failure remains
`outcome_unknown/recovery_requires_inspection`, with the plan retained; it is not
permission for an unconditional DDL retry. Driver messages and private paths are
not emitted in application result JSON.

Unlike normal business runtime preflight, this source-free operator may perform
read-only target identity admission before SQL catalog admission. This rejects a
changed physical sink before opening the authority; the shared recovery service
still requires SQL catalog and native-origin admission before any CAS or DDL.
There is no business extraction or publication during endpoint admission.

`publication-authority recover plan|execute` exposes thin, source-free argument
parsing and help. Exit codes are 0 for ready/completed, 2 for a proven scope block,
and 1 for an unknown observation/operation. The standalone CLI deliberately has
no deployment observer and cannot perform recovery. An admitted deployment
runner calls `PreparedRecoveryApplication(safety=trusted_observer, ...)` with its
actual implementation; no synthetic observer is a production recipe. An outer
plan digest can be reviewed without becoming a dispatch permit. Existing
`publication-authority schema` commands retain their behavior and reuse the
registry scope resolver. Production observer integration and live acceptance
remain required; scripted application tests are not their substitute.

### Held observation and normal lifecycle

For PREPARED, the trusted observer must cover every replica exactly once, from
no later than the original preparation through the current observation boundary,
with no gaps, matching publication DDL, pending requests, active mutations or
other writers. Its authenticated freeze identifies and excludes every other
writer, including manual and restarted workers. SQL UTC time is only a coverage
bound, not cross-system clock or history authentication. Exiting the service
hold does not release deployment exclusion. A production observer and its live
acceptance are still required; operator JSON or a boolean cannot supply one.

The winning exact CAS alone yields a consumable dispatch permit. Conflict or
lost CAS acknowledgement stops without DDL. Lost DDL replies are reconciled
against the original exact retained entry, never dispatched again. Resuming
DISPATCHING/COMMITTED/CLEANUP_DISPATCHING reuses the normal lifecycle without
requiring absence of its own DDL. All reads and effects remain operation-bound
and held; loss of the hold stops progress, including after a possible effect.

Before predecessor cleanup, the shared lifecycle requires exact replica coverage
and published target counts equal to the admitted staged count. Missing hosts or
lost rows preserve the predecessor. Returning COMPLETED additionally requires a
fresh healthy desired target on every replica, no candidate, exact row counts
and terminal publication/owned cleanup entries. As in normal publication, a
terminal DDL failure with proven physical completion is reconcilable; it is not
silently retried. If no cleanup was dispatched, no synthetic cleanup receipt is
invented. These checks do not prove row-wise data quality.

The current internal service rejects zero-row and quality-bearing recovery until
the original empty-load policy and quality governance can be authenticated.
It reads no source and does not rerun a workload. Component tests use synthetic
in-memory state/transport with the real DDL adapter; they are not a live
ClickHouse/deployment certificate or permission to activate the option.

### Independent audit binding — implementation refinement

The approved system-storage work also requires audit selection without enabling
source checkpoints. This section specifies that additive wiring; it does not
change publication authority, authorize a history migration, or imply a released
feature. It is not an additional maintainer approval receipt.

```yaml
state:
  type: disabled
sink:
  options:
    load_governance:
      audit:
        storage:
          type: mssql
          connection_ref: system-audit
          provisioning: external
        loads_table: dpone_load_audit
        steps_table: __dpone__load_steps
```

`audit.storage` is a closed object: `type: mssql` and a canonical nonempty
`connection_ref` are required; `provisioning` is optional and only `external`
is supported. Null, malformed, unknown or misplaced source-side selectors fail
before connector construction. No raw credentials, `reuse`, location overrides
or fallback to publication, source or sink credentials are accepted.

The verified registry descriptor owns a required MSSQL database and schema.
An explicitly authored `audit.state_schema` must match that schema exactly;
omission inherits it, not the legacy default. Load/step names are distinct,
unqualified SQL identifiers, defaulting to the two names above. The runtime
preflights both existing table contracts before source/sink construction and
does not create, alter, reset or move tables. Partial admission or later failed
hydration closes its owned connector; each worker hydrates a separate pair.

Disabled source state stays disabled: no state alias, xmin, run or checkpoint
store is fabricated. An independent selector cannot silently override an
MSSQL state's automatic audit binding; conflicting double selection fails
before storage construction. Without the selector all current defaults remain.
The existing selected-pair policy remains: disabled step auditing produces no
step store while retaining the selected load identity ledger.

Config, rendered batch, flow/folder compilation and direct runtime entrypoints
share admission. Airflow projection includes the audit alias independently of
state and rejects a missing projection. A selected runtime pair may not be
replaced by business-sink audit. Versioned dbt publish policies remain unchanged
and reject this selector; adding it there requires its own compatible policy
version and certification, not modification of frozen schema bytes.

Acceptance includes malformed-selector parity, registry/context/alias/type
failures, zero DDL on missing or drifted external load/step tables, unchanged
disabled state, ordered source-free admission, one connector close on failure,
worker ownership and no-selector backward compatibility. Existing histories are
preserved; deployment and migration still require their separate gates.

The opt-in local SQL test
`tests/integration/clickhouse_cluster/test_mssql_independent_audit_live.py`
checks real external catalog admission and load/step audit persistence. It uses
unique metadata and decoy databases, hand-authored table definitions and
database-scoped DDL-deny triggers. The registry location differs from the
connector's default database; readback verifies identity, counters, Unicode and
NULL values without creating checkpoint tables. Missing or drifted catalogs
fail closed, while disabled step auditing retains the load ledger. This test
constructs the resolved audit binding directly: it is not live proof of trusted
context admission, business endpoint ordering, workers, migration or a complete
MSSQL-to-ClickHouse route. Databases are retained for inspection; production
endpoints and existing catalogs are never used by the fixture.

The operator-provisioned `dpone_cluster_publication_authority` slot table and
`dpone_cluster_publication_events` append-only table form one versioned catalog.
Slot key, positive revision, operation, phase, canonical payload bytes/hash and
unique write ID are checked together. Events have unique slot/revision and
write IDs, previous/current digests, the full envelope, database time and
provenance. No runtime DROP or automatic catalog provisioning is permitted.

CAS uses a short indexed SQL Server transaction, explicit error handling and
`XACT_ABORT ON`. Compare exact predecessor bytes and revision, update exactly
one slot and append its event in that transaction. Only a positively
acknowledged COMMIT and exact result can return VERIFIED. SQL OUTPUT alone is
not commit acknowledgement. A lost acknowledgement returns UNKNOWN without a
permit, including when a later read observes the desired result.

Persist PREPARED → DISPATCHING with exact DDL intent before sending that DDL.
Only the winning invocation obtains a one-shot permit. Restart/readback never
reconstructs it. After any ambiguous dispatch, only reconcile existing token,
query digest and replica generations. Do not retry EXCHANGE or reclaim a slot
by TTL. Apply the same durable ordering to cleanup intents. Require physical,
replica and original quality evidence before COMPLETED.

The process-local permit binds the complete intended publication payload except
the SQL acknowledgement's write UUID. The DDL adapter additionally verifies its
phase and rendered SQL digest, then consumes the permit under a process lock
before invoking transport. Consumption is shared across adapter instances and is
not undone after a transport exception. This is an internal capability, not a
serialized credential: constructing a new permit from a readback is forbidden.

This is at-most-once dispatch among participating writers, not an atomic
transaction spanning SQL Server and ClickHouse. A crash after intent commit
but before dispatch may remain blocked. Availability does not justify guessing
that publication did not happen. Empty output requires explicit authored
permission; missing quality proof is not a passing quality receipt.

## Legacy adoption, rollout and rollback

Keep the legacy record, operation and generation identities. Stop admissions,
retries, old processes and delayed/restarted writers for every scoped target;
drain issued requests and DDL on all replicas. Re-read exact legacy envelopes
after drain and immediately before apply. Any drift invalidates the plan.
Hold the freeze until adoption and installation of the single new binding.
A paused scheduler alone is not proof of writer exclusion.

Adoption atomically creates a slot and immutable `legacy_adopted` event binding
original bytes/version, generation, quality, source location, topology and
deployment observations. It must not stamp imported history as native strict
origin. Authenticate original quality with existing governance; if that cannot
prove the generation, remain blocked rather than fabricate a capsule. An empty
current DDL queue is not proof of no historical dispatch.

MSSQL cannot fence an unmanaged process with direct ClickHouse credentials.
If all scoped writers cannot be excluded, adoption is blocked. No server
configuration or account-right changes are an implicit part of this feature.
Rollback after dispatch stops admissions and reconciles; it never rewinds
authority, removes evidence or reactivates an old writer over the new state.

## Retirement and fresh-load amendment

**Status: APPROVED by the maintainer on 2026-09-30.**
This section extends, but does not activate or weaken, the approved adoption
contract above. The operator authorized retirement of a proven unpublished
legacy preparation, preserving history, followed by a new load and new quality
evaluation. Pure policy, application orchestration, bounded canonical plan
decoding and an isolated SQL store are implemented as an unactivated foundation.
The ordinary authority reader verifies the exact retired event, original plan
and binding without granting replay or dispatch. Native-origin claims cannot
label a retired envelope as an ordinary publication. Operation-aware admission
and SQL CAS authenticate the retained first event, permanently rejecting its
retired operation ID even after a later operation completes. A new operation
checks the unchanged healthy predecessor before source I/O, then acquires the
slot through exact CAS with an isolated fresh candidate. Draft operator parsing
and application composition are available; public selection is implemented on
this branch, but the trusted deployment observer and workload activation remain
unavailable.

### Intent and alternatives

When a historical source snapshot and its authenticated quality capsule no
longer exist, do not manufacture replay proof from an audit report. Keep the
existing adoption-to-publication path blocked. The selected alternative is to
retire the unpublished intent without publishing its candidate, then perform a
fresh full extraction under the current approved policy. Remaining blocked is
the fallback whenever non-publication or writer exclusion cannot be proven.
Deleting the old authority, labelling the old run successful, and importing it
as native strict history are rejected.

### Admission and immutable plan

The read-only retirement plan binds the original all-replica authority bytes
and versions, operation, target/candidate UUIDs and schemas, replica inventory,
destination service/environment/binding, and deployment-owned freeze/drain
evidence. Only PREPARED is eligible; any publication/cleanup dispatch identity
or later phase is rejected. The candidate must remain unpublished everywhere: the target must
still be the exact predecessor, or absent for an original first publication,
and the candidate must retain its exact generation identity.

Require complete, attributable DDL observations for the original operation
through the drained observation boundary. A PREPARED label, empty current DDL
queue or unchanged row count alone is insufficient. A coverage gap, unavailable
replica, pending request, active mutation, unknown outcome, or inability to
exclude an old/restarted/manual writer blocks the operation. Freeze evidence
must come from the admitted deployment observation path, identify its scope and
validity, and cover all writers; an operator-supplied `frozen: true` is not proof.

Immediately before apply, revalidate the exact observations, plan digest and
freeze validity. Hold exclusion through destination readback and deployment of
the single new binding. This remains a cooperative managed-writer guarantee,
not a fence against arbitrary privileged SQL. No server settings or account
rights changes are introduced.

### Retirement is not successful publication

Use a distinct versioned `RETIRED_UNPUBLISHED` terminal state with
`legacy_retired` provenance in the MSSQL catalog. Preserve original bytes and
their digest in its immutable event; leave the legacy ClickHouse record and
candidate untouched. Do not issue a publication/cleanup permit, set a committed
receipt, advance a data checkpoint, clear a historical Airflow failure, delete
the candidate, or label missing quality as passed.

Insert the retired slot and event in one acknowledged SQL transaction only if
the destination slot is absent. An existing exact retirement is read-only
idempotent success; a different existing slot is a conflict. A lost commit ACK
is UNKNOWN, not permission to repeat the insert. Resolve it by exact slot/event
readback, including all provenance and plan bindings. Native create/CAS must
not be an alternative way to forge a retirement event.

A fresh operation may acquire that same target slot only through CAS from the
exact admitted retirement, with a new operation ID and a freshly checked
unchanged target generation. It starts PREPARED without any old dispatch permit
or quality capsule. The old operation ID is permanently ineligible for replay
as a successful load. Preserve the retirement event when later phases advance;
do not rewrite imported history as native history.

Both normal and quality publication paths call the selected authority's explicit
operation-aware admission capability; missing capability is not permission to
fall back. The SQL mutation repeats root-history validation under the slot lock.
Exactly one concurrent fresh CAS may win. A losing or ambiguous mutation grants
no dispatch permission. A fresh candidate differs in name, UUID and replication
path from the preserved unpublished candidate and the existing target.

### Public behavior and acceptance

Draft `publication-authority retire plan|apply|verify` delegates to a shared
application service. Plan never mutates a database; apply and verify require the exact
plan digest. Versioned redacted results distinguish `retired_unpublished` from
`publication_completed`; exit 0 means only the requested retirement mode was
verified, 2 means a proven block, and 1 means failure or unknown outcome. Local
plan files use the existing private atomic file adapter. A file digest is not
deployment evidence or permission to dispatch.

All operator invocations must use the same deployment-pinned, owner-private
persistent attempt directory. An exclusive fsynced claim precedes the SQL
attempt; an existing claim permits readback only, including after process
restart or lost SQL acknowledgement. Its key binds destination, target and
legacy operation, not the mutable planning observation: replanning cannot
reset the attempt. Never delete the claim or choose a new
directory to retry. This journal is not a distributed fence: deployment still
owns writer exclusion, while SQL owns absent-slot and subsequent CAS ordering.
A missing/unavailable journal fails closed. Deployment of this composition,
including its journal persistence, is an activation prerequisite.

The application accepts an already admitted observer and journal as injected
capabilities, not user-provided observation JSON or a CLI journal path. Plan
arguments are `--connection-ref`, `--environment` and `--plan-file`; apply and
verify use `--environment`, `--plan-file` and `--confirm-digest`. The saved
outer plan binds the exact verified-context subject, SQL binding/endpoint,
original retirement provenance and journal directory identity. Confirmation
and environment are checked before credentials; journal drift is checked before
opening SQL. Catalog admission is read-only and its connection closes before
the retirement service receives the store. Every later SQL session rechecks
the endpoint pin. No native create/CAS or target DDL capability is exposed.

The local journal requires an existing canonical absolute owner-only (`0700`)
directory. It freezes path/device/inode/owner identity at admission and uses one
opened directory descriptor for temporary creation, exclusive marker publication,
cleanup and directory fsync. Replacing the pathname cannot redirect that claim;
an observed replacement blocks progression while preserving any published marker.
The identity hash is **not** evidence that a directory is persistent, shared or
deployment-admitted. Provision and certify the fixed runner storage separately;
never use a new directory or remove a marker to recover an unknown outcome.
The standalone CLI therefore blocks without an injected trusted observer/journal.
Application tests use scripted SQL boundaries and do not certify a live cutover.

An expired plan cannot authorize an insert. Read-only verification may renew the
deployment freeze and historical coverage while retaining the exact original
source location, replica bytes/versions, generations, inventory and destination.
The SQL lookup still compares the originally applied plan/provenance, not a
newly invented receipt. Renewal changes neither the attempt claim nor authority;
unavailable or changed original facts keep verification blocked.

Tests must reject mixed replica generations, historical DDL coverage gaps,
expired/changed freezes, a restart between plan and apply, destination
conflicts, forged native/imported provenance, repeated old operation IDs and
lost SQL acknowledgements. Concurrency tests require one retirement winner and
one fresh-operation CAS winner. Measure zero ClickHouse mutations during
retirement and no synthetic quality success; verify the fresh load follows the
ordinary source/stage/quality/publication/outcome lifecycle.

Independent review and isolated live fault tests precede release and cutover.
After migration, keep scheduling paused for one complete fresh DAG run and DQ;
only then restore the existing schedule and verify its first regular run. A
verified retirement alone never satisfies workload acceptance.

## Architecture and alternatives

Pure binding/adoption models live in contracts; the MSSQL adapter and catalog
admission are small runtime state modules. Existing composition roots inject
resolved connectors and the common factory. CLI adapters contain no vendor SQL
or recovery policy. Keep the existing module/import budgets; no general plugin
framework or unrelated refactor is needed.

KeeperMap remains a separate existing backend, not a requirement for this route.
A recovery-only sidecar was rejected because normal publication would still
use a different authority. Existing run/xmin/transaction tables were rejected
as substitutes because they do not store this envelope or dispatch semantics.
An ADR accompanies activation of the new backend and its cooperation limits.

## Primary sources and measurable acceptance

Official sources checked 2026-09-30:
[SQL Server OUTPUT](https://learn.microsoft.com/en-us/sql/t-sql/queries/output-clause-transact-sql?view=sql-server-ver17)
can return rows for an operation later rolled back; require commit ACK.
[SSIS checkpoints](https://learn.microsoft.com/en-us/sql/integration-services/packages/restart-packages-by-using-checkpoints?view=sql-server-ver17)
can repeat a completed transaction after restart; do not infer one-shot DDL from
a scheduler checkpoint. This is a scoped recovery comparison, not a superiority
claim. dlt, Airbyte, Informatica, Fivetran, Pentaho, gusty, Cosmos and Beam are
N/A for the specific SQL Server CAS/ClickHouse EXCHANGE boundary; no assertion
is made about their broader recovery capabilities.

Unit and contract tests cover alias convergence, environment isolation, malformed
bindings, wrong backend/location, predecessor mismatch, one CAS winner,
rollback-after-OUTPUT, lost ACK, phase regression, immutable event drift,
zero-output policy, missing quality, stale snapshot and service drift. Real
MSSQL/ClickHouse tests inject concurrency and crashes at commit/dispatch/cleanup
boundaries. Measure at most one DDL dispatch per operation and zero source reads
on recovery; retain physical generations, event history and exact source/image
identity. Mocked tests do not certify these guarantees.

Documentation includes a first-success configuration, catalog plan/apply guide,
binding/schema reference, adoption and unknown-outcome runbook, and old-state
history lookup. Independent final review, all required CI, isolated live proof,
then release and deployment-specific admission precede activation. The Native
integrator owns shared schemas and composition; reviewers are read-only.
