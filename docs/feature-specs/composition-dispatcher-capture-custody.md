# Feature design: dispatcher-owned composition capture

- Status: RESEARCHED
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
worker-supplied Native bytes as independent source proof. This document proposes
a changed custody boundary and is not implementation authorization.

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
`EXECUTE_TRANSFER.subject` has exactly `attempt_document`, `attempt_sha256`,
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

### Execution deadline and shutdown

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

ADR 0064 records the proposed change to ADR 0063. Neither decision is accepted
until the maintainer approves this amendment.

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
- [ ] Maintainer marks this specification APPROVED.
