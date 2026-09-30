# ADR 0076: SqlClient bulk loading uses a closed optional companion boundary

- Status: Accepted
- Date: 2026-09-28
- Related: [ADR 0057](0057-bounded-window-atomic-publication.md),
  [ADR 0072](0072-mssql-native-writer-proof-capabilities.md),
  [MSSQL target-local verification design](../feature-specs/mssql-sqlclient-target-local-verification-v2.md)

## Context

The bounded ClickHouse-to-MSSQL route already seals replayable native files and
can verify an owned SQL Server stage without returning business rows to Python.
BCP remains a safe default, but its separate SQL session prevents dpone from
proving partial outcomes or tying writer quiescence to a session-owned lock.

`Microsoft.Data.SqlClient` exposes `SqlBulkCopy` and can keep an application lock
and the bulk writer on one connection. Loading the .NET runtime or vendor SDK in
the base Python process would make ordinary imports and help depend on optional
native software. Passing credentials in arguments, environment variables,
journals, or evidence would also widen the secret boundary.

The P1 writer protocol is BCP-specific: its method accepts a reject-file path
and its grant omits the explicit wire layout, ordered mappings, and deadline.
Changing that public callable in place would break compatible deployments.

## Decision

SqlClient is an explicit optional backend implemented by a separately packaged
Linux x86-64 .NET 10 LTS companion and a thin Python supervisor. .NET 8 is not
the GA baseline because its support ends on 2026-11-10, too close to this
feature's rollout window.

- The canonical runtime port is
  `dpone.ports.mssql_native_writer.NativeStageWriter`. Its single method accepts
  an immutable `dpone.contracts.mssql_native_stage_writer.NativeStageWriteRequest`
  and process-local `OperationDeadline`, and returns the closed, explicitly
  non-authoritative `NativeStageWriteObservation` described in the approved
  feature specification.
- The request binds the exact owned stage, owner/object identity, sealed-file
  identity, native wire-layout digest, ordered writable columns and mappings,
  attempt identity, and proof capability. It contains no credentials.
- The existing `NativeStageBulkWriter.write(grant, rejects_path=...)` remains a
  compatibility boundary for unchanged P1 BCP composition and gains no
  SqlClient policy. A canonical BCP implementation may translate the new
  request, including its producer-enforced `max_row_bytes`, only through an
  injected deadline-aware launcher that accepts the
  same `OperationDeadline`, derives every process wait and cleanup bound from
  its remaining budget, and returns `timeout` or another closed uncertain
  observation when the budget expires. It must not delegate to the legacy
  callable while claiming the canonical deadline contract.
- The companion protocol is closed and versioned as
  `dpone.mssql-sqlclient.ipc.v1`. Unknown fields, frames, versions, mappings, or
  capabilities fail before target mutation.
- Credentials cross one anonymous inherited pipe. They are read exactly once,
  never serialized, never included in argv/environment/stdout/stderr, and the
  descriptor is closed after projection.
- One non-pooled `SqlConnection` acquires the session-owned application lock
  derived from the grant authority, constructs `SqlBulkCopy` on that same
  connection, and holds the lock until disposal. Reconnect during an attempt is
  forbidden.
- The admitted options are `EnableStreaming`, `KeepNulls`, `TableLock`, and
  `UseInternalTransaction` with explicit mappings. `FireTriggers`,
  `CheckConstraints`, and `KeepIdentity` are not admitted in protocol v1.
- One monotonic attempt deadline covers launch, secret projection, write,
  acknowledgement, cleanup, and disposal. No phase resets or extends it.
  Deadline expiry and any malformed/uncertain companion outcome are converted
  to a closed non-authoritative observation and durably followed by `UNKNOWN`;
  they cannot escape past the coordinator before durable closure. Restart
  permits observation-only reconciliation and never blind relaunch.
- Writer success is not content authority. The existing exact-stage barrier,
  target-local count/digest, preparation proof, and atomic publication remain
  mandatory.
- `import_backend: mssql_sqlclient` is explicit, requires
  `verification_backend: target_local`, uses journal/identity v2, and has no
  fallback. Omission remains BCP plus Python readback v1.
- Vendor imports and runtime discovery stay in the optional adapter/package and
  composition root. Base import, CLI help, and credential-free planning perform
  no .NET or network I/O.
- Public activation requires recovery CLI, closed evidence schemas, exact-head
  Docker certification, and publication support for the companion distribution.
  Release 0.88.0 satisfies the source-side contract and registers only the
  explicit selector; each deployment still runs its own readiness and route
  qualification before production use.
- Source-free recovery is authorized by a closed private journal document
  written at verified source EOF. It serializes the exact transaction operation
  with explicit field codecs and hashes the authored route, verification
  identity, target connection, state store, and absolute work root. It never
  serializes clients, credentials, endpoint coordinates, callbacks, or raw work
  paths. Live recovery recomputes all bindings through the normal composition
  root and fails closed for legacy records or any drift.
- Recovery commands are thin adapters over one application service. They use
  the ordinary manifest selector and connection resolver, acquire the standard
  window lease and custody, and preserve publication receipt, evidence,
  checkpoint, cleanup, and custody-release ordering. Direct journal editing or
  a CLI-only second lifecycle is not an admitted recovery mechanism.

The durable writer binding stores only `sha256(grant_token)`. Existing
schema-valid P1 bindings remain opaque and are neither migrated nor interpreted
as raw authority. New attempts always generate a fresh 256-bit token and persist
its SHA-256 digest.

## Consequences

- BCP remains available and compatible through a narrow adapter while new
  runtime policy depends on a backend-neutral capability port.
- SqlClient can prove writer-session termination and classify a proved partial
  stage without treating a lost acknowledgement as permission to replay.
- The sealed native file stays the recovery boundary; direct streaming is a
  different design and is not introduced here.
- Deployments choosing SqlClient must install and certify the companion, .NET 10,
  and the matching protocol/package identity. Missing or incompatible software
  blocks before source I/O.
- The companion adds a distribution and supply-chain subject. Before general
  availability, an independently approved controller change must add it to one
  manifest-bound five-package release set, Trusted Publisher parity, retained
  build inventory, rehearsal, and verification. PyPI uploads are not
  transactional. A partial upload follows the existing fail-closed rule: fix
  the cause, choose a new version for the complete release set, and never use
  `skip-existing`. The source repository does not gain another publisher.
- Linux ARM hosts use an explicit Linux x86-64 execution boundary for the
  companion. Emulation results are correctness evidence only and cannot establish
  production performance.
