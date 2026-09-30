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
native SQL authority adapter, and shared normal/replay runtime composition.
Public manifests still reject the proposed option; no existing workload switches
backend. Legacy adoption, prepared-recovery integration, operator commands and
end-to-end ClickHouse recovery acceptance remain required before activation.
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

Proposed operator commands are `publication-authority schema plan|apply` and
`adopt plan|apply`; they delegate to application services shared with Python.
Plan is read-only; apply requires the exact plan digest and environment
admission. Outputs are versioned, redacted JSON with ready/blocked/in_progress/
completed, reason codes and identity digests. Exit 0 means requested mode
completed, 2 a proven block, 1 operational failure/unknown. Plan files are
atomic and refuse overwrite by default. These commands remain unavailable
until implemented; examples must not imply current release support.

The internal plan-file adapter writes bounded owner-private files on a local
POSIX filesystem: flush the full temporary file, link its final name without
replacement, then flush the parent directory. A failed final durability
acknowledgement preserves the completed file for readback; it is not permission
to overwrite or retry automatically. Reads reject symlinks, non-regular files,
shared permissions, extra hard links and observed concurrent changes. This
adapter does not authenticate an operator plan or replace apply-time validation.

## Algorithm and transaction boundaries

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
