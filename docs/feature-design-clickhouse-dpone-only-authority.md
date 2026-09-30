# Feature design: dpone-only ClickHouse publication authority

- Status: APPROVED
- Owner: dpone maintainers
- Issue: production binding follow-up to PR #240
- Target release: subsequent patch after implementation and scoped certification
- Confirmed requirement: on 2026-09-30 the maintainer confirmed that only dpone
  writes the target ClickHouse table.
- Approved deployment decision: single-host authority, persistent local SQLite
  storage, no automatic failover. The maintainer explicitly accepted this written
  specification and its conservative unknown-outcome availability on 2026-09-30
  with an explicit acceptance and instruction to implement. This approves
  implementation, not production activation.

Last verified: 2026-09-30

## Purpose and customer journey

This proposal is for platform engineers and operators binding the
[approved publication kernel](feature-design-clickhouse-publication-method.md).
It narrows the writer problem to one controlled dpone deployment. Two dpone
processes are not automatically mutually exclusive. The measurable outcome is
at most one publication dispatch per immutable intent, with durable exclusion
retained across uncertain outcomes. Read-only consumers continue querying the
target; this design serializes mutations, not SELECTs.

| Persona | Need | Success signal |
|---|---|---|
| Architect | Safe full refresh while preserving identity where possible | Replacement only for one complete snapshot partition |
| Platform engineer | A concrete authority, not a caller assertion | Protected journal and one target mutation ingress |
| Operator | Safe restart without DDL replay | Original operation recovered or explicitly held unknown |

The first composition is an injected Python backend, not a YAML/CLI route:

1. Inventory every dpone job that can mutate the physical target, including
   cleanup, schema changes, old serial routes and maintenance. Migrate them to
   the same authority or prevent them from writing this target.
2. Provision a persistent local authority volume and dedicated publisher
   identity. Extraction workers must not possess target mutation grants. This
   separation is a proposed prerequisite, not a verified deployment fact.
3. Register a canonical target and perform read-only readiness checks. Missing
   journal, unsupported topology, incomplete visibility or bypass credentials
   block admission before source payload access.
   SQL inspection cannot prove absence of every bypass writer: the platform must
   supply a reviewed ingress/grant inventory and enforce exclusive credential
   routing. Any known second ingress fails startup; a caller assertion alone
   is insufficient.
4. Compose the backend, acquire durable target ownership, produce and seal the
   candidate, then invoke the existing publication kernel.
5. Observe operation, method/reason, state and closure evidence. A committed
   publication is not yet a completed route checkpoint.
6. On restart, reopen the same journal and operation before source access.
   Unknown retains resources and exclusion; it never starts another attempt.
7. Upgrade only after draining admissions and verifying journal compatibility.
   Never bootstrap replacement authority over an existing target when the
   original journal is missing.

## Scope and non-goals

The proposed binding supports one deployment host, one direct ClickHouse node,
Atomic databases and plain MergeTree tables. Each target retains a canonical
identity while its table UUID changes under EXCHANGE. Different targets may
operate independently; source workers may remain parallel.

In scope: durable target ownership, immutable journal/CAS, candidate sealing,
one-shot publication and conservative closure, reusing the current method
selector and outcome classifier without weakening their checks.

Not included: multi-host authority, HA/autofailover, network filesystems,
independent SQL writers, retrying/load-balancing proxies, ON CLUSTER,
ReplicatedMergeTree/Shared/Distributed targets, automatic recovery from every
transport failure, and ODBC route activation. Approval of this document does not
authorize changing production grants, services or existing Docker containers.

## Public contracts and compatibility

The eight-method `GuardedPublicationBackend` remains the kernel boundary. Its
concrete composition combines focused journal, exclusion, observation and
publisher services. Dependencies are injected; no hidden credentials, service
locator or vendor imports on base import/help paths are introduced.

Existing `publish(operation_id)`, `recover(operation_id)` and
`dpone.clickhouse.guarded-publication.v2` records remain unchanged. Backend
storage uses a separate schema, `dpone.clickhouse.authority.v1`, retaining the
complete kernel record plus authority and transport history. Unknown versions
fail closed. DTOs and diagnostic JSON do not confer authority. Neither legacy
UUID-v1 receipts nor `SQLiteWindowStore` become this journal.

CLI/manifest changes are N/A for this slice. Do not advertise a stock `dpone run`
path before composition exists. Python callers retain `PublicationUnknown` and
`safe_to_retry=False`. A future CLI binding needs its own exit/output contract.

Diagnostics expose schema, deployment/target/operation identity, epoch, intent
digest, method/reason, claim history, dispatch state, closure reason, publication
state and retained resources, never credentials or source rows. They are derived
from protected originals and written atomically to a new path without implicit
overwrite. A diagnostic file cannot be imported as recovery authority.

## Durable identity and ownership

Use SQLite WAL, `synchronous=FULL`, foreign keys and short `BEGIN IMMEDIATE`
transactions; verify settings on every connection. The certified deployment
requires persistent local Linux storage, not a Docker Desktop host bind mount
or NFS/SMB. Actual power-loss durability is an environment obligation, not a
guarantee established by configuration alone.

Provision the authority once; normal startup only opens an existing store.
Missing/corrupt storage, disk-full errors or ambiguous commits stop admissions.
Workers and other tenants cannot access the protected directory or DB/WAL/SHM
files. Copying only a live main DB is not a valid backup. Restoring an older
backup cannot reactivate publisher credentials: missing history requires offline
reconciliation of the authority first.

A structurally valid older backup is not detectable from SQLite alone. Restore
and volume replacement are unsupported automatic operations. Before either,
operators must revoke/isolate the publisher's database access and stop admissions
outside the restored store. They must not restart using an old snapshot and still
valid credentials. This is an enforced operational prerequisite, not a claimed
startup detection feature. Automatic restore would require an independent
monotonic activation fence and is outside this increment.

Canonical subject identity includes deployment, pinned server identity, database
and target name. Hostname aliases cannot create another subject. Observed table
and database UUIDs are separate evidence; table UUID is not the ownership key.
Enforce unique operation/query IDs, one retained owner per subject, immutable
intent bytes/digest and monotonic epoch/closure history. Namespace operation IDs
with globally unique deployment identity before the existing query-ID derivation.

Ownership starts before payload extraction and survives mutex release, process
exit and service restart. It never expires. The execution lock serializes claim,
closure and dispatch entry; do not keep a DB transaction open during network I/O.
Only an acknowledged CAS grants that invocation execution authority. Readback
of an ambiguous or historical claim never recreates a grant. A still-CLAIMED
record whose closure failed remains unresolved for admission purposes even if
the kernel did not persist an UNKNOWN state label.

The actual send entry and no-send closure race on one durable row CAS under the
same authority: `not_started -> may_have_sent` requires the current acknowledged
claimant and epoch; `not_started -> closed_without_send` permanently revokes that
path. Only one transition wins. Gateway entry rechecks epoch/closure. A paused
claimant cannot send after closure wins, including after lock reacquisition. If
send-state wins, no later process can infer that SQL was never sent.

## Detailed algorithm

1. Resolve the protected subject and retained owner before source access. Existing
   operations enter recovery only; another operation owning the same target
   blocks new admission regardless of its age.
2. Validate registered endpoint, grants and supported topology. Candidate
   creation/loading also belongs to the authority. Track and join every accepted
   writer, then close candidate admission irreversibly. Uncertain inserts prevent
   sealing; a worker-supplied flag is not proof that writing has stopped.
3. Produce fresh complete catalog, design, partition and typed multiset evidence
   under exclusion. Reject TTL, outstanding mutations, unsupported dependencies
   or policies, and incomplete visibility. Pin evidence algorithm/type coverage
   and server version. Counts/physical part checksums alone are insufficient.
4. Let the existing kernel choose REPLACE, EXCHANGE, RENAME or no-op. Persist
   immutable PREPARED, then acknowledge one PREPARED-to-CLAIMED CAS. Uncertain
   preparation/claim results grant no execution authority.
5. Reobserve target and sealed candidate. Only the acknowledged claimant may
   enter the publisher. Before writing any SQL bytes, durably change transport
   state from `not_started` to `may_have_sent`. Failure or lost ACK of that
   transition prevents sending; recovery still treats persisted state conservatively.
6. Execute one frozen synchronous statement over a dedicated direct transport.
   No application/driver/proxy retry, arbitrary SQL/settings, multi-statement,
   asynchronous insert or distributed queue is allowed. Do not reuse the generic
   connector retry loop for REPLACE PARTITION.
7. After a complete validated terminal protocol response, persist closure before
   accepting outcome evidence. Timeout, disconnect and process death are not
   terminal responses. On ambiguous closure persistence, reopen only its original
   record; never repeat SQL.
   The first transport is a pinned synchronous direct native-protocol connection,
   one query in flight. Positive completion requires consuming its successful
   EndOfStream through the certified driver, with no queued call remaining.
   Server exceptions, partial packets and transport failures do not satisfy this
   first profile, even when they are plausibly terminal. Driver/server versions
   and zero-retry behavior must be recorded and fault-tested before activation.
8. Close admission monotonically under the execution lock and prove the gateway
   has no accepted pending request. `not_started` can close without sending only
   when no claimant can subsequently enter the send path. `may_have_sent` without
   durable terminal completion remains unknown. `close_and_drain` raises if proof
   is missing. No-op closes without a mutation request.
9. After proven closure, use existing UUID/design/content classification and CAS
   the resolution with its evidence. Unknown retains target owner, candidate,
   journal and recovery resources.
10. Return the kernel record. Route integration later owns checkpoint/evidence
   finalization, method-aware cleanup and controlled ownership release. This
    increment never automatically releases ownership, even for terminal records.

Prepublication registration is owned by an explicit composition service, not
the eight-method kernel port. That service acquires the subject owner before
source work, registers candidate writer handles, closes admissions, joins their
acknowledged completions and records the seal. Only then may it call the kernel.
Tests that supply an already sealed candidate validate publication alone and must
not claim to prove this prepublication lifecycle.

```text
owner -> sealed candidate -> PREPARED -> acknowledged CLAIMED
  -> durable may_have_sent -> one SQL -> durable terminal closure
  -> protected observation -> durable publication resolution

uncertain transport/closure -> UNKNOWN -> retain owner/resources
existing operation         -> recovery only; never SQL replay
```

No query-log-only recovery or automatic takeover is included. Query logs are
diagnostics; using them as authority needs a separately certified correlation
contract. Empty `system.processes`, KILL, elapsed timeout, desired rows, PID
disappearance or matching table UUID do not prove complete closure.

## Failure semantics and operator recovery

| Boundary | Required result |
|---|---|
| Prepare/claim commit ACK lost | Read original; no new execution grant |
| Concurrent CAS loser | Recover original; no dispatch by loser |
| Crash before send-state transition | Close under exclusion; classify without sending |
| Crash after `may_have_sent`, even before socket write | Unknown without positive closure; sacrifice availability |
| Server effect followed by lost response | Unknown; no repeated EXCHANGE/replacement |
| Terminal response stored, crash before resolution | Restart closes and reconciles source-free |
| Target/candidate design or content changed | Reject or unknown; no destructive repair |
| Empty snapshot/stale target partitions | Existing selector uses whole-table exchange, not partition loops |
| No-op/absent target | Existing distinct selection and recovery methods |
| Missing journal/stale backup/wrong endpoint | Block admission; do not reconstruct authority from target rows |
| Cancellation | Close admissions; preserve possible-send state and exclusion |

Operators inspect the original operation. Repeating `recover(operation_id)`
cannot rerun DDL. A possibly-sent request without closure stays quarantined; this
binding has no force flag, TTL unlock or operator boolean bypass. A separate
offline recovery design must prove ingress isolation and server quiescence before
it can permit release. Until implemented, this is an explicit availability cost,
not a production recovery success claim.

Because terminal ownership release is deferred, this staged increment cannot
admit a second operation on the same target. It is a backend building block,
not a repeatable production binding or a release-ready route. Controlled durable
release, method-aware cleanup and source-free finalization are mandatory before
production activation; they must not be replaced by deleting owner rows.

Rollback is a new guarded publication after original authority is resolved.
REPLACE retains the candidate, not the previous target data. Backup/retention and
ownership release remain integration work. V1 EXCHANGE cleanup must not be used
with replacement receipts.

## Architecture and alternatives

| Component | Responsibility | Dependency |
|---|---|---|
| Existing contracts/kernel | Method selection and post-closure classification | Protected backend port |
| SQLite authority adapter | Journal, unique owner, CAS and monotonic history | Standard-library SQLite and explicit storage path |
| Local mutation gateway | Serialize registered writers; consume dispatch grant | Authority and injected transport |
| Protected observer | Physical and typed evidence; unsupported-state rejection | Injected reader and pinned producer |
| One-shot publisher | Fixed SQL and exact protocol completion | Frozen intent and direct transport |
| Composition root | Bind store, endpoint, credentials and services | Explicit dependencies; no default router binding |

Split modules by responsibility and enforce existing quality budgets without
exceptions. No new generic plugin registry is needed. Replacement and exchange
share authority/transport lifecycle but retain distinct classifiers.

| Alternative | Benefit | Cost/decision |
|---|---|---|
| Single-host journal and one mutation ingress | Small auditable binding | Recommended; no HA, conservative unknown outcomes |
| External transactional store | Multi-host CAS | Deferred; does not itself close delayed ClickHouse requests |
| Mutex or TTL lease alone | Easy setup | Rejected; expiry/process death cannot stop old SQL |

A supplemental ADR is required before production binding. ADR 0076 remains
accepted only for the unbound kernel; this proposal does not change its status.

## Research and measurable outcome

Official sources checked 2026-09-30, rolling documentation; local baseline is
ClickHouse 24.8.14.39:

- [Partition operations](https://clickhouse.com/docs/reference/statements/alter/partition):
  replacement is atomic for the selected partition, requiring compatible
  structure, keys and storage policy. Adopt scope-aware selection, not a claim
  of global atomicity for partition loops.
- [HTTP interface](https://clickhouse.com/docs/concepts/features/interfaces/http):
  losing a connection does not automatically stop a running request. This argues
  against treating disconnect as cancellation; it does not establish every
  native-protocol failure behavior.
- [KILL](https://clickhouse.com/docs/reference/statements/kill): cancellation
  targets running queries. Our inference: this alone cannot rule out a delayed
  request that has not yet registered.
- [SQLite WAL](https://www.sqlite.org/wal.html) and
  [synchronous settings](https://www.sqlite.org/pragma.html#pragma_synchronous):
  adopt single-host local storage and FULL synchronization; reject NORMAL/OFF
  for acknowledged durable authority and network-filesystem deployment.

dlt, Informatica, Airbyte, Fivetran, Pentaho, SSIS, gusty, Astronomer Cosmos and
Apache Beam are N/A for this narrowly scoped backend authority/closure contract,
not an assessment of their ingestion products. The broader market comparison
remains in the ODBC v2 design. No speed or superiority claim is made.

Targets: zero second DDL dispatches under duplicate delivery, lost ACK and restart;
zero successor admissions with uncertain closure. Measure durable history and
actual server mutations on an exact commit. Synthetic tests do not establish
production deployment authority.

## Validation, documentation and rollout

| Layer | Required evidence |
|---|---|
| Unit/contract | Aliases, divergent intents, version/CAS, zero retry, empty/stale partitions |
| Fresh-process storage | Races, crash at each commit boundary, disk full/corruption/missing journal, stale backup refusal |
| Transport faults | Lost ACK before/after effect, paused-before-send claimant, delayed request, lost closure write, cancellation |
| Local Docker | Actual REPLACE including `tuple()`, EXCHANGE, RENAME, no-op; UUID/content and restart quarantine assertions |
| Deployment/security | All ingress/grants inventoried; candidate writers closed; protected persistent volume; no retrying proxy |
| Compatibility | Serial/v1 behavior unchanged; no manifest default activation |
| Route/release | ODBC v2 memory/source/object-storage and exact-commit gates still required |

Docker tests use new owned containers/databases, pinned image identities,
create-once JUnit/artifacts and no credential logging. Existing user containers
are not repurposed. Docker failures do not prove production power-loss durability
or absence of bypass writers. Missing live checks remain UNVERIFIED.

Add separate setup/tutorial, exact Python reference and unknown-outcome runbook,
linked from the publication overview. Test composition examples before claiming
first success. Keep service internals out of the already-large MSSQL guide; CLI
docs and route matrix change only after routing is integrated and certified.

Roll out as opt-in explicit composition. Rollback stops admissions, preserves
journal/resources and continues recovery. Never fall back to an old writer while
an operation is unresolved.

## Execution ownership and approval

The main integrator owns this spec, publication overview, navigation, future ADR,
task contract and shared semantics. Explorer, architect, certification and UX
agents remain read-only. A later approved task contract lists exact adapter,
runtime and test paths before writers start; no parallel writer shares authority
schemas, common fixtures or composition roots.

Implementation order: durable journal/ownership and process tests; one-shot
publisher/closure; observer/composition and local Docker method tests; independent
review; then separate route finalization and activation work.

- [x] Sole target writer is dpone: confirmed by maintainer.
- [x] Algorithm, public impact, alternatives, failures and limits described.
- [x] Independent architecture, certification and UX input reconciled.
- [x] Single-host/no-HA and conservative unknown-outcome availability accepted.
- [x] Maintainer approves this written specification for implementation.
- [x] Durable-foundation plan and path-scoped task contract approved (Native execution).
- [ ] Implementation, exact-commit review and live certification complete.

First executable planning increment:
[durable authority foundation](superpowers/plans/2026-09-30-clickhouse-authority-foundation.md).
It delivers the journal and transition rules; transport, observer and composition
follow separately and are not represented as implemented by that increment.
