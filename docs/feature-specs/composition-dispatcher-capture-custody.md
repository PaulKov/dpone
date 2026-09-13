# Feature design: dispatcher-owned composition capture

- Status: APPROVED
- Owner: maintainer; implementation integrator: root
- Issue: PR42
- Target release: TBD; existing published versions are preserved
- Last verified: 2026-09-13

## Executive summary

The current three-cell design has an execution-placement gap. The ClickHouse
cell captures the MSSQL source and writes retained originals through a Linux
root-only adapter. Its worker factory also constructs administrative ClickHouse
and Docker collaborators. The enrolled dispatcher must run without root; the
ordinary Kubernetes worker must receive neither those administrative credentials
nor Docker authority. The root host-facts service is deliberately read-only.
No one of these processes can run the current complete capture path.

Recommended amendment: execute the complete existing ClickHouse cell inside the
protected nonroot dispatcher, using a separately enrolled service-owned capture
volume. Keep the existing root-owned storage profile unchanged. Add an explicit
versioned whole-cell request; do not hide source capture inside OPEN_GATE or treat
worker-supplied Native bytes as independent source proof. The maintainer explicitly approved this changed custody boundary and its
implementation on 2026-09-13.

## Personas and customer journey

| Persona | Required journey | Success signal |
|---|---|---|
| Platform administrator | Provision fixed service identity, isolated volumes, network and protected configuration; enroll exact observed facts | Read-only readiness proves the complete deployed profile |
| Data engineer | Build the existing verified parent and submit its generated DAG | No hand-authored capture file, SQL or extra credentials |
| Operator | Inspect immutable SQL/file originals after success, timeout or process loss | Exact attempt is reconciled; uncertain work is never replayed |

The worker submits scheduler/parent identity to the protected service. The
service selects the verified manifest, derives the attempt against current SQL
ownership, runs capture/ingest/publication, and returns independently retained
terminal references. A missing capability blocks activation before reservation
or predecessor drain. Upgrade and rollback preserve every old original.

## Scope and non-goals

Only the existing `mssql_clickhouse_full_refresh_v1` load semantics are affected.
No new connector, strategy, background cleanup, replica topology, source-row
transformation or generalized remote SQL API is proposed. Native dbt and
PostgreSQL-to-MSSQL retain their existing approved execution contracts.
The host-facts service stays read-only; workers obtain no host privileges.

## Public contract

The existing v1 closed dispatcher transport keeps its framing and six operations.
A separate v2 schema/route adds `EXECUTE_TRANSFER`: its subject contains the exact
candidate composition attempt and existing closed Airflow run/try originals.
The common request still pins dispatcher, runtime authority and correlation.
No request contains a manifest, source rows, arbitrary statement, path, endpoint,
credential or environment override. The staged source plan supplies those inputs.
The service verifies that deriving the attempt from the current ACTIVE occurrence
and scheduler originals produces exactly the candidate before registering it.
Only the service registers/executes the attempt; the worker never pre-registers
a second execution permit.

A successful response contains row count and exact protected terminal receipt,
CLOSED_GATES, QUIESCENCE and OUTCOME references for that attempt. It contains no
rows or secrets. Success requires fresh authoritative SQL readback after the
existing execution root completes; HTTP acknowledgement alone is insufficient.
Timeout/disconnect is UNKNOWN and cannot initiate a new execution. READ_STATUS
reopens immutable originals without issuing credentials or acquiring a permit.
The worker evidence adapter gains an explicit remote-result type; it reports the
verified row count instead of manufacturing local rows to satisfy the old result.
Existing local `CompositionClickHouseResult` behavior remains compatible.

The signed dispatcher binding remains the existing four-field v1 document.
Protected service configuration selects `capture_custody=dispatcher_owned_v1`,
expected UID/GID, fixed capture root, physical volume identity, resource limits
and per-authority control binding. Its digest is already pinned by the signed
binding. Enrollment must explicitly include this custody profile and volume.
Old enrollment or a v1-only service cannot satisfy this profile's readiness.
No manifest syntax change and no automatic fallback to the local Docker factory.

The existing root-owned capture adapter and old originals remain unchanged.
New service-owned originals use a distinct configured root and enrollment;
they cannot adopt, chown, overwrite or recertify historical root-owned artifacts.

### Exact v2 wire and terminal evidence

The new route is `POST /v2/composition-dispatch` and schema is
`dpone.composition-dispatch-rpc.v2`. Its canonical envelope has exactly `schema`,
`request_id`, `dispatcher_id`, `runtime_authority_sha256`, `operation`, `subject`.
`EXECUTE_TRANSFER.subject` and `READ_STATUS.subject` have exactly `attempt_document`, `attempt_sha256`,
`airflow_run_identity`, `airflow_attempt`. Decode attempt through
`composition_persistence.decode_attempt_identity`; decode the scheduler originals
through `AirflowRunIdentity.from_mapping` and `AirflowAttemptCorrelation.from_mapping`
from their existing contract modules. Require exact canonical re-encoding of
all three documents; unknown fields, contradictory scheduler identifiers and
extra payload bytes reject before admission. Metadata remains bounded to 1 MiB.

The correlated v2 response has exactly the current correlation/subject-digest
fields plus `status`, `evidence_document`, `evidence_sha256`. Status is one of
`SUCCEEDED`, `FAILED`, `IN_PROGRESS`, `UNKNOWN`. A duplicate existing RUNNING
attempt returns `IN_PROGRESS` with its validated immutable current receipt and
never executes; COMMIT_UNKNOWN returns `UNKNOWN`. An already terminal attempt
returns only revalidated historical evidence, without a new permit. A malformed
or unauthorized request retains the closed transport error response.

Successful evidence uses `dpone.composition-remote-transfer-result.v1` with exactly
`schema`, `attempt_document`, `attempt_sha256`, `capture_document`, `capture_sha256`,
`publication_document`, `publication_sha256`, `terminal_receipt_document`,
`terminal_receipt_sha256`, `closed_gates`, `quiescence`, `outcome`, `rows`.
Each proof entry contains its exact existing original and digest; both purpose
closures are included. The existing `SnapshotCaptureRecord.rows` authenticates
the count through its captured SQL original, generation seal and PUBLISHED
intent/result. It must equal the independently observed materialization count.
The adapter compares complete attempt, target/write, generation, epoch, source,
publication and proof subjects across all originals, and verifies every canonical
hash. It never accepts a bare count or status string as result authority.
Oversized evidence rejects; it is not truncated or replaced with an unverified
summary. Non-success evidence contains only the verified current receipt and
bounded original references, never the successful-result schema.

`READ_STATUS` uses the same authenticated full attempt/scheduler subject. It only
inspects retained SQL operations: absence never reads ACTIVE, constructs an
executable candidate or starts an effect. Existing receipts use the same status
and terminal-original responses as execution. Positive absence in the protected
transaction returns `UNKNOWN` with exactly
`{"schema":"dpone.composition-remote-transfer-absence.v1","attempt_sha256":"<requested digest>","observation":"ABSENT"}`.
The decoder accepts this shape only for `READ_STATUS` plus `UNKNOWN` and the exact
requested attempt digest. It is an observation, not proof that an operation never
ran, and grants no retry permission. No synthetic receipt is created. The v1
protocol remains unchanged. The worker root exposes an explicit
`read_status(request)` operation for the same verified typed candidate, usable
before or after submission. It returns the decoded response, does not change the
single-submission guard, and never automatically executes or retries.

### Original envelopes and terminal aggregation

`capture_document` uses `dpone.composition-remote-capture-originals.v1` and
contains exactly `schema`, `subject`, `captured`, `generation_seal`. Each original
reference is `{document, sha256}` and uses its existing canonical typed decoder.
The subject, captured record and seal must reconstruct the same generation.
`publication_document` is the existing PUBLISHED `SnapshotPublicationRecord`.

`terminal_receipt_document` uses
`dpone.composition-remote-attempt-observation.v1` with exactly `schema`,
`attempt_document`, `attempt_sha256`, `state`, `closed_gates_sha256`,
`quiescence_sha256`, `outcome_evidence_sha256`. This is a **derived protected SQL
readback**, not a document retained verbatim in SQL. Its producer reads the
original operation and actual receipt columns in one pinned transaction and
validates terminal proofs and their ClickHouse evidence before returning it.

`closed_gates` and `quiescence` each contain exactly `terminal`, `ingest`,
`publisher`. Each proof reference contains `document`, `sha256`, and
`evidence_document`; the latter must hash to the proof's `evidence_sha256`.
`outcome` is one such proof reference. Preserve singleton purpose proofs for the
capture seal and publication closure. Each terminal proof instead covers the
complete sorted immutable issued-authority tuple, exactly the union of both
purpose authorities. The protected producer must reject additional issuance.

Aggregate closure evidence uses
`dpone.composition-clickhouse-terminal-closure.v1` with exactly `schema`, `kind`,
`attempt_sha256`, `ingest_proof_sha256`, `publisher_proof_sha256`. Aggregate OUTCOME
evidence uses `dpone.composition-clickhouse-terminal-outcome.v1` with exactly
`schema`, `attempt_sha256`, `state`, `capture_sha256`, `publication_sha256`.
The capture envelope digest includes the complete generation seal. These stable
originals are persisted and independently reopened before finalization; they
cannot be manufactured from an HTTP success or a list of proof hashes. The
producer reopens purpose proof/evidence originals, capture/seal and the exact
PUBLISHED record in the same pinned control transaction. Existing generic SQL
terminal validation remains required but is insufficient without this nested
ClickHouse validation. Codecs check consistency; authenticated service delivery
and protected SQL observations establish authority.

Non-success evidence uses `dpone.composition-remote-transfer-status.v1` with
exactly `schema`, `receipt`, `references`. The receipt is an original reference
to the derived observation. References are a sorted unique list of at most 16
`{kind, sha256}` entries, with kinds CAPTURE, PUBLICATION, CLOSED_GATES,
QUIESCENCE, OUTCOME. FAILED requires FAILED; IN_PROGRESS requires RUNNING;
UNKNOWN permits COMMIT_UNKNOWN or RUNNING while an expired execution unwinds.
An absent admitted operation is a transport rejection, never a fabricated
receipt. All successful envelope counts and complete subjects must agree.

### Protected startup configuration

The concrete closed configuration uses `dpone.composition-dispatcher-service.v2`.
It contains exactly `schema`, `dispatcher_id`, `dispatcher_uid`, `dispatcher_gid`,
`capture_custody`, `context_root`, `host_probe_socket`,
`supervisor_enrollment_sha256`, `capture_root`, `capture_root_identity`, `listen`,
`tls`, `bearer_file`, `accept_timeout_seconds`, `execution_timeout_seconds`,
`max_concurrency`, and `authorities`. Custody is `dispatcher_owned_v1`.
Process UID/GID are nonzero integers below `2147483648`, equal to the externally
provided bootstrap identity and actual process identity. Configuration is read
through protected no-follow file access and must match its externally provided
SHA256, which the signed dispatcher binding also pins. It contains no self-digest.

Nested fields are closed: capture identity is `{device, inode, uid, gid, mode}`
with mode `448` (`0700`); listen is `{address, port}` with a numeric, non-wildcard
address and a valid TCP port; TLS is `{certificate_file, private_key_file}`.
All paths are fixed canonical absolute paths. Credentials are read separately
after configuration validation; configuration contains no credential value.
Capture identity is independently compared with protected enrollment and fresh
host facts before it can establish custody.

Acceptance is an integer from 1 to 30 seconds, execution from 1 to 900 seconds,
and concurrency from 1 to 64, using the listener's one existing slot control.
Cleanup remains fixed at 60 seconds and is not another configurable allowance.
The nonempty authority catalog has at most 1024 entries, keyed by exact runtime
authority digests. Each value contains exactly `context_sha256`,
`control_connection_ref`, `expected_control_service_id`, `control_schema`.
Existing logical-reference, canonical UUID and SQL schema validators apply.
The context digest selects the protected staged original; request fields cannot
replace any configured control binding or path.

The explicit module entry point is `python -m dpone.app.composition_dispatcher_service`.
Its required arguments are `--config`, `--configuration-sha256`,
`--dispatcher-uid`, and `--dispatcher-gid`. The configuration path is canonical
and absolute, for example `/etc/dpone/dispatcher/startup/service.json`; its
grandparent is the protected configuration root and its final two components
select the original. Help requires no connector SDK or credential access.
Invalid arguments exit 2 before I/O; unavailable protected authority exits 1
with a fixed actionable stderr message; orderly shutdown exits 0.

Startup reads root:dispatcher protected TLS/bearer files before binding, rejects
changes across certificate loading, and authenticates the configured dispatcher
and authority catalog. It creates one shared stop signal and injects its budget
factory into the listener. Signal callbacks only notify admission stop; normal
coordinator code closes the listener and joins admitted work. No automatic
restart or detached execution is introduced. This entry point supplies the v2
whole-cell handler; existing independently constructed v1 listeners retain their
original operations. Readiness remains a separate required authority check.

The worker selects the signed dispatcher metadata without resolving source or
target credentials. It requires the already projected
`DPONE_DBT_WORKSPACE_AUTHORITY_CONNECTION_REF` for a protected read of the retained
parent, using its signed database/service pins. This read never registers an
attempt. The worker resolves only this control binding and the dedicated API
binding; neither may alias a source/target registry entry. The API binding uses
the existing resolved `endpoint` and `token` credentials, verified TLS with
optional `ssl_ca_location`, and positive integer `connect_timeout` (at most 30)
and `send_receive_timeout` (at most 900) as acceptance/execution limits. The
administrator aligns the latter with service execution configuration. The signed
control connection descriptor may set `composition_control_schema`; its default
is `dpone_control`, preserving existing profiles. The worker validates this value
with the canonical SQL control-schema validator before opening a connection and
passes it to both SQL authority collaborators. It must equal the corresponding
protected service authority's `control_schema`. An environment variable or an
incoming request cannot override this coordinate.

Retained parent request hashes and epochs supply candidate identity for ACTIVE,
RETIRING or RETIRED history. The existing local execution builder still requires
ACTIVE, and the service grants admission only after a positive absence check and
fresh ACTIVE derivation. No worker synthesizes ACTIVE state for a retired parent.
A missing explicit dispatcher binding preserves the existing local profile;
an invalid or unavailable explicit binding never falls back to local execution.
Native factory construction is delayed until an actual native request so that
ordinary remote selection cannot resolve administrative target credentials.

### Native execution-profile readiness observation

The approved completion includes a side-effect-free observer running in the actual
provisioned Linux execution profile, never inferred from the activation host.
`observe_native_execution_profile(supervisor=..., supervisor_root=...,
profiles_root=..., io_deadline=...)` returns canonical bytes with exactly `schema`,
`supervisor`, `supervisor_sha256`, and `roots`. The schema is
`dpone.composition-native-profile-observation.v1`. `roots` has exactly
`supervisor`, `run`, `profiles`; each records the canonical absolute `path`, actual
`device`, `inode`, `uid`, `gid`, permission `mode`, and numeric `filesystem_type`.
The supervisor is the original validated closed projection and its canonical
`sha256:`-prefixed SHA256 digest. The observer rejects subclasses and reconstructs
the closed projection before opening roots. This observation is not a standalone
readiness permit.

The observer validates the explicit projection and paths, requires actual Linux
root UID/GID, opens and holds all three directories through the shared protected
no-follow traversal, observes actual filesystem type, and requires the profiles
root to be tmpfs. It then reopens each configured path and compares all recorded
facts with the held descriptor before returning canonical bytes. One fixed
caller-provided deadline brackets every OS operation and serialization; timeout,
path rotation, privilege/permission drift, unavailable observations or malformed
inputs reject with a credential-safe error. Every opened descriptor closes on
all paths, including failed acquisition and failed final verification.

The observer never allocates a child identity, creates a lock/directory/profile,
reads credentials or scans attempt-specific process identities. Shared filesystem
primitives remain the authority for execution as well as readiness. Actual mount
provenance, nonce, runner identity, configuration, toolchain and SQL observations
must be bound by the enclosing readiness producer. Local inode observations do
not establish a named Docker-volume/PVC identity or prove future dbt execution.

### Execution deadline and shutdown behavior

V2 has a separately configured `execution_timeout_seconds`, an integer from 1 to
900; handshake/body acceptance retains the existing short bounded transport
budget. The service sets one absolute execution deadline after authenticated
complete metadata, passes the remaining time into every SQL/source/HTTP operation,
and checks it before each new gate or business-effect admission. Existing v1
transport limits do not change. A client disconnect does not imply cancellation:
an already admitted whole-cell execution may continue until that deadline.

Deadline expiry or SIGTERM stops admission of further CREATE/INSERT/EXCHANGE
operations. An already admitted network/SQL operation may still finish and its
observed result must be retained. Only journal persistence, read-only status and
credential revocation/closure may use a separate bounded 60-second cleanup budget;
cleanup cannot publish an unstarted exchange. The request returns UNKNOWN if its
execution deadline expired. Later independent status may report a proven terminal
result; it never restarts the cell. The local concurrency slot remains occupied
until the handler and its I/O have actually unwound, including cleanup. If an
underlying operation outlives its bound, retain the slot and durable UNKNOWN;
external service teardown requires the existing host/quiescence recovery barrier.
No background retry, unbounded detached thread or forced-cancellation assumption
is introduced.

### Custody callback transaction boundary

The host custody callback is SQL-free: it obtains authenticated host facts and
compares them with the exact composition-pinned enrollment. The same callback
brackets source and catalog observation and immutable file operations. A separate
`require_enrollment_in(ledger, subject)` callback reopens SQL enrollment using the
capture store's existing pinned ledger, including before write commit. Historical
readers reuse that supplied ledger and validate terminal history. Neither callback
opens a nested control transaction or uses an ambient current-ledger registry.

## Algorithm and state

1. Authenticate the v2 bounded request before accepting its subject. Reserve a
   bounded local request slot; load only administrator-pinned staged authority.
2. Reopen verified source plan, exact workload/pack/constituent/write subject and
   scheduler identity. Resolve the protected service control binding only now.
3. Read actual SQL occurrence, attempt history, epochs and exact physical target;
   compare the complete staged parent. Existing attempts never become new permits.
4. Recheck enrolled host/container/network identity and capture-volume custody.
   Invoke the existing whole-cell root in the dispatcher. Its existing gate
   issuance may precede capture; no CREATE/INSERT precedes durable capture.
5. Capture the source once through its pinned MSSQL session. Preserve the existing
   protected SQL claim, consistent read, canonical Native/schema generation,
   exclusive fsynced file installation and exact SQL digest-binding order.
6. CREATE/INSERT use the retained local originals, current gate and durable unique
   dispatch claim. No payload roundtrip through the worker is required. Complete
   transport observation is persisted before any successful response.
7. Close ingest, prove quiescence and independently compare actual typed target
   content with capture originals. Seal the generation; perform the existing
   journaled Atomic publication through its separately issued publisher gate.
8. Read protected terminal proof and exact row count. Respond only after all
   required closure/outcome originals are independently validated.
9. A timeout, crash, missing original, partial file, lost SQL ACK or uncertain
   EXCHANGE preserves existing RUNNING/COMMIT_UNKNOWN ownership. Status does not
   resume execution, regenerate a capture, refund a claim or reconstruct secrets.

The existing SQL state machines remain authoritative. The service execution path
is NEW -> ADMITTED -> CAPTURED -> INGEST_CLOSED -> GENERATION_SEALED -> PUBLISHED ->
TERMINAL, with any uncertain boundary retaining UNKNOWN. These descriptive names
do not introduce a second mutable authoritative journal. Different attempts retain
separate generation names. Empty input still creates and verifies an empty
physical generation; null/type/wide-page rules remain unchanged.

## Custody and architecture

The trusted dispatcher already owns administrative target credentials and the
protected dispatch journal. This amendment additionally trusts that enrolled
service identity as the source-original producer, without granting host root.
The new file adapter requires the actual process UID/GID to equal the enrolled
nonzero identity; capture directories are service-owned `0700`, files `0600`,
regular and single-linked. A root-owned protected parent and fixed volume identity
are provisioned externally. No worker or other container may mount that volume,
including inode, ancestor or descendant aliases; existing host observation must
prove this at enrollment and execution. Read-only context/config volumes remain
root-owned with the current group-readable profile.

The initial custody profile supports a rootful Docker daemon with identity UID/GID
mapping only. Host `/proc/<dispatcher-pid>/uid_map` and `gid_map` must show identity
mapping; the observed host UID/GID must equal the exact nonzero `Config.User`
UID:GID and enrolled service identity. Rootless daemon, remapped users and user
namespaces are unsupported until separately specified; no numeric-ID inference.

Enrollment records the dispatch container/image/boot and mount namespace, the
capture volume's Docker type/name/source/destination, and root directory device,
inode, UID, GID and mode. Host `stat(source)` must equal the identity observed
through `/proc/<dispatcher-pid>/root/<destination>`. Capture opens its configured
root once with no-follow traversal and compares `fstat` identity to that enrolled
original before each installation. Fresh host observations bracket the consistent
source read, exclusive installation and SQL capture binding; a changed mount,
namespace or identity refuses the capture. The final generation seal repeats the
custody check. Atime/mtime of mutable contents are not enrollment identities.
The v2 dispatcher mount allowlist is exactly its existing secret tmpfs plus this
one writable capture volume; root filesystem/config/context remain read-only.
The existing v1 single-writable-tmpfs allowlist remains unchanged.

Use no-follow descriptor-relative traversal, exclusive creation, bounded reads,
file/directory fsync, unchanged stable inode/metadata checks and SQL-pinned digests.
The service never chmods/chowns an existing root or repairs partial originals.
A compromised trusted dispatcher remains outside the original trusted-producer
threat model; least privilege is not a substitute for this explicit trust choice.

| Component | Responsibility |
|---|---|
| Worker v2 client/result adapter | Submit exact verified scheduler identity; retain terminal references |
| Protected v2 handler | Authenticate, select source, own whole-cell invocation and fresh terminal readback |
| Service-owned capture adapter | Enforce enrolled process/volume custody and existing immutable-file protocol |
| Existing capture/runtime/gates/publication | Preserve source identity, effects, evidence ordering and recovery |
| Root host-facts service | Observe real isolation and volume facts only; no SQL/capture operations |

Reuse runtime domain services behind injected storage/transport capabilities.
Do not add policy to compatibility facades. New modules stay below warning
thresholds in `docs/benchmarks/quality_budgets.yml`. Existing layer/fitness failures
remain blockers; this amendment does not raise budgets or authorize new debt.

## Alternatives

| Choice | Benefit | Cost | Recommendation |
|---|---|---|---|
| Dispatcher owns complete cell and service-owned originals | One existing trusted service owns source, target and recovery; no row transfer to worker | Explicit custody and versioned whole-cell protocol change | Recommended |
| Separate privileged capture service | Preserves root-owned capture files | Another authenticated service, catalog access and recovery handoff | Alternative if root-only custody is mandatory |
| Give worker admin/Docker authority | Reuses local factory | Violates approved isolation | Rejected |
| Make host-facts service execute SQL/capture | Fewer processes | Violates its read-only boundary | Rejected |

ADR 0064 records the proposed change to ADR 0063. The maintainer approved this amendment on 2026-09-13.

## Research and measurable outcome

The parent specification's connector/ETL market comparison remains applicable.
For this repository-specific custody amendment, dlt, Informatica, Airbyte,
Fivetran, Pentaho, SSIS, gusty, Astronomer Cosmos and Apache Beam are N/A as direct
comparators: no interoperability or superiority claim is being introduced.

Current official platform documentation, checked 2026-09-13, distinguishes the
container process user from persistent volume mounts ([Docker container
configuration](https://docs.docker.com/engine/containers/run/)) and exposes explicit
user/group and filesystem security controls ([Kubernetes security
context](https://kubernetes.io/docs/tasks/configure-pod-container/security-context/)).
The proposed enrollment and immutable SQL/file protocol are dpone design choices;
these platform documents do not certify them.

Target: one full cell from an ordinary worker with zero worker administrative
ClickHouse credentials, zero worker Docker access, one source capture per admitted
attempt, and zero automatic replay after injected uncertain acknowledgement.
Measure with the real deployed route, denial probes, observed rows and retained
SQL/file evidence. No performance advantage is claimed before that campaign.

## Validation, documentation and rollout

Unit/contract tests must cover exact candidate derivation; foreign activation,
workload or target; illegal file owner/mode/link/volume; empty/null/wide inputs;
partial write; late response; duplicate execution; in-flight closure; immutable
status; and old profile refusal. Real Linux tests must run under the enrolled
nonroot UID with its actual protected volumes and denied worker access.
The provider campaign must trigger all three cells and inject capture crash,
post-commit disconnect, interrupted generation sealing and uncertain EXCHANGE.
Retained original/actual-row mismatches are release blockers, never SKIP-as-PASS.

Update architecture, supervisor provisioning, v2 RPC reference, provider journey,
retry/recovery guidance and release evidence. Roll out schemas/provisioning and
new enrollment before enabling the v2 worker route. Rollback disables new
admissions and preserves volumes, credentials tombstones, SQL originals and
already committed data. It never replays or exchanges a target back automatically.

Integrator owns shared schemas/factories/docs/indexes and final evidence. Writers
receive separate worktrees and disjoint contracts for custody, v2 protocol,
worker adapter and certification. Fresh architecture/security/UX review is
required before integration. All live execution remains UNVERIFIED.

## Approval

- [x] Concrete placement gap and alternatives documented.
- [x] Proposed identity, custody, effect and recovery behavior specified.
- [x] Compatibility and validation obligations stated.
- [x] Independent review of this proposal resolved (read-only; not implementation approval).
- [x] Maintainer explicitly approved the complete proposal on 2026-09-13.
