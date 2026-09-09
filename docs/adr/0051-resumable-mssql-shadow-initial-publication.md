# ADR 0051: Large MSSQL initial loads use receipt-backed shadow publication

## Status

Accepted.

## Context

A large PostgreSQL-to-MSSQL initial load was executed as hundreds of
`incremental_merge` chunks against the growing live target. Every chunk paid
key-lookup, update, lineage and index-maintenance costs, so elapsed time grew
non-linearly. Direct truncate/append would be faster but could expose a partial
table, lose the previous target and force a failed campaign to restart.

A durable chunk ledger alone is insufficient. Target DML and the later ledger
CAS are separate transactions; a pod can die after target commit but before
the ledger records success. Retrying that chunk under a new scheduler run would
append it twice unless the target receipt identity survives scheduler reruns.

## Decision

- Add an explicit `backfill.publication.mode: shadow_swap` capability for
  chunked PostgreSQL-to-MSSQL initial loads.
- Require `inner_mode: incremental_append`, a deterministic chunk plan, unique
  key, MSSQL target-atomic audit state, distributed locking and retained backup.
- Treat the stable shadow as a derived physical artifact of the registered live
  target. Do not create another immutable target-registry row. Runtime-issued
  authority binds the exact database/schema/live/shadow coordinates; every
  chunk revalidates the live binding and the shadow's server-side campaign owner.
- Allow campaign-scoped shadow and backup names derived from a bounded digest of
  the explicit campaign key. A new reviewed generation can coexist with an
  incomplete predecessor; old objects and ledgers remain untouched evidence.
- Use work-conserving fixed worker lanes in separate Python interpreters. Start
  them with `spawn`, never `fork`, because a copied ODBC driver state is not a
  supported process boundary. Each long-lived child owns one source/sink/state
  connection set and executes chunks serially; parallelism exists only across
  operating-system processes. The parent alone owns chunk/campaign ledger I/O,
  dispatch and result IPC. No interpreter may execute concurrent `pyodbc`
  calls, and no global chunk-wave barrier idles healthy lanes.
- Renew campaign, chunk and generic-MSSQL operation leases synchronously from
  the parent event loop. Admission in a child registers the exact operation by
  `request_id`, worker, command, run key, chunk, owner and epoch; the parent
  validates and renews it while the child is busy. Safe-point status IPC fails
  closed when the parent cannot prove a renewal. Process lanes use one internal
  90-second TTL renewed every 30 seconds, independently of the authored legacy
  lease setting, so a terminated parent process whose SQL session closes is
  reclaimable before a five-minute scheduler retry. Immediately after a child
  admission succeeds, its finite generic-operation lease is registered with
  the parent and renewal starts before post-admission source-boundary or schema
  preplan work. The MSSQL campaign additionally retains its session application lock
  as a continuous fence across a non-preemptible parent SQL call. After that
  call, only `APPLOCK_MODE=Exclusive` on the same session permits refreshing an
  elapsed durable timestamp; a lost fence fails closed. There is no background
  ODBC heartbeat thread and no concurrent whole-ledger renewal. A network
  partition that leaves the server session alive is outside this bound;
  partition-tolerant takeover needs a future epoch-fenced lease row and
  phase-level mutation fencing.
- Retain the recoverable campaign's SQL Server session-level application lock
  continuously from admission through aggregate validation, deferred physical
  design and publication. A long blocking vendor statement may outlive the
  90-second durable lease, but only the exact lock-holding session may renew it.
  Loss of that session invalidates the campaign fence. In the atomic rename
  transaction, reread the latest campaign journal row with `UPDLOCK, HOLDLOCK`,
  compare the exact owner/cancellation/immutable identity, and overlay only the
  publication receipt on the current chunk state before commit.
- On a native child exit, stop new claims and bound already-running peers. Keep
  the last exact operation descriptor even after child `unregister` until the
  parent ledger CAS succeeds. If target commit happened first, a fresh immutable
  receipt probe promotes the chunk without rereading PostgreSQL; otherwise the
  durable chunk remains resumable.
- On POSIX, each lane reports READY with its session/process group immediately
  after `setsid`; its ID must equal the spawned PID. The parent validates that
  identity against the spawned process and the operating system, caches it,
  then sends the exact containment ACK. Only after that ACK may the child
  resolve/enter runtime opener code. A distinct runtime-ready event gates the
  first claim. The parent consumes process sentinels before IPC completions and
  before redispatch, then terminates and verifies the validated process group,
  including inherited BCP descendants, before resolving a timed-out peer.
- Prepare, serialize and owner-reprove the complete initial dispatch batch
  before sending the first `ProcessLaneRun`. This performs both dispatch and
  receipt-authority catalog warming while no child command is active. Reissue
  the finite operation deadline for each exact owner after that work, serialize
  the refreshed command, prove every chunk, drain ready peer callbacks, then
  force one final campaign-session lease renewal. A non-servicing readiness
  gate over all prepared lane sentinels and active peers is the only boundary
  before the first send. An already-observable prepared-lane exit aborts the
  complete unsent batch. Co-ready peer activity
  defers the exact prepared claims in memory; after IPC service the same owners
  are refreshed and re-proved without a false failure/attempt transition. Once work is running,
  service ready pipes and sentinels between per-lane coordinator, chunk and
  operation callbacks and admit at most one redispatch claim per loop boundary.
  Parent SQL uses a scoped five-second ODBC timeout across ledger, private
  campaign-fence, generic-state fresh sessions and ephemeral signed-authority
  sessions; the previous connector policy is restored after lane execution and
  child bulk I/O is unaffected. A child authority request waits for at most the
  30-second renewal interval or one third of its remaining finite lease,
  whichever is smaller; near-expiry requests retain the shorter bound. A fatal
  coordinator or operation-renewal failure is latched and reported once while
  already-running peers drain. The parent latches a lane as running before its
  pipe write; an ambiguous partial or complete send failure must quiesce and
  probe the target receipt before the claim can transition.
- Fuse PostgreSQL temporal-domain validation into the same repeatable-read
  `COPY` that exports the chunk. A versioned high-entropy marker maps an invalid
  branch to a sanitized typed error; do not scan a clean source range twice.
- Append typed staging into the shadow without `TABLOCK`: disjoint worker lanes
  retain operation-scoped application locks and must not serialize on a table
  lock. Probe immutable receipts at `READ COMMITTED` after that application
  lock is held, avoiding serializable missing-key range-lock conversion between
  unrelated chunks. Create the clustered columnstore once before loading and
  deferred unique indexes once after exact row-count and duplicate-key
  validation.
- Derive the governed chunk receipt invocation from the runtime-proven campaign
  run key, not the current Airflow occurrence. Target DML and its receipt remain
  one transaction; a crash before ledger CAS is replay-suppressed on resume.
- Publish by renaming live to backup, shadow to live, and appending the campaign
  publication receipt in one MSSQL transaction. Retain the backup.
- Bind every prepared campaign to each live and shadow SQL generation as
  `object_id + dpone-owned immutable UUID`; `object_id` alone is reusable after
  drop/create. Acquire the canonical physical-target transaction lock shared
  with ordinary MSSQL writers before the publication lock. While holding both,
  re-prove the immutable target binding, complete live head and predecessor
  receipt, then repeat owner, row-count, duplicate-key, column and index
  validation before rename. A campaign
  validated against an older or ABA-replaced generation can never overwrite a
  newer live table.
- Certify a database-qualified exact object catalog before shadow creation and
  source I/O, and persist its canonical digest in the prepared publication
  record. The supported subset excludes every object behavior that `SELECT
  INTO` plus deterministic index DDL cannot reproduce: constraints, triggers,
  object/column permissions, non-simple columns, special table features and
  non-default physical index attributes. After target lock, campaign fence and
  generation CAS, reread both live and shadow catalogs. Require the original
  live digest and a complete shadow match before the first rename; an in-place
  DDL race does not need to change `object_id` to be detected.
- Write an immutable publication-head receipt on the renamed live table. When
  an initial XMin handoff exists, the same transaction records its state-key and
  seed-load identity as pending authority. The checkpoint CAS takes the same
  target-global lock, requires that exact head, and closes it with the XMin
  receipt in the checkpoint transaction. A crash may leave a resumable pending
  head, but cannot expose one campaign's live table with another campaign's
  checkpoint. Legacy recovery may bind an exact previously committed seed
  receipt only while holding both target and publication locks; it accepts a
  pending-or-exact head and closes both receipt bindings atomically.

## Consequences

- Runtime cost is approximately linear in source bytes and four lanes can make
  useful progress without serial target-wide DML locks.
- The exactly-once boundary is explicit: an operation-scoped transaction
  application lock serializes equal chunk identities, while the unique receipt
  constraint remains the final database invariant. Unrelated chunk receipts do
  not retain missing-key range locks until commit.
- Campaign publication has two complementary fences: continuous session
  ownership excludes another campaign coordinator, while the in-transaction
  latest-row CAS prevents a stale in-memory ledger from overwriting a newer
  cancellation, owner, or chunk transition.
- Pre-generation prepared and published ledgers are upgraded once under both
  the held campaign session and target-global transaction lock. The upgrade
  verifies the server-side run-key owner, current live/backup/shadow objects and
  latest non-cancelled campaign row, then persists only the publication overlay.
  A concurrent cancellation or conflicting publication head blocks migration.
- Publication and ownership transfer serialize through a transaction-owned
  application gate. The publisher takes the gate before proving the dedicated
  session and campaign row; a successor takes it after acquiring session
  ownership. This closes the session-check/rename TOCTOU without extending a
  database transaction over the whole initial load.
- Campaign journal revisions exclude the chunk array. Heartbeats are O(1),
  ordinary chunk mutations use indexed point reads, and takeover hydrates only
  latest running chunks. Full hydration is a read-model concern, not a write
  path prerequisite.
- Legacy full-row journals cross that boundary atomically: the first compact
  campaign append also materializes the entire inline chunk plan, including
  overlays from any newer per-chunk revisions. The inline fallback is never
  discarded before the split projection is complete.
- New campaigns persist a versioned, catalog-certified portable column contract
  and bind it into both campaign hashes. The parent derives each AST-specific
  chunk binding before process dispatch; child admission can therefore replay a
  committed operation receipt before PostgreSQL schema projection/snapshot/row
  I/O and before the source/target business-schema preplan. Infrastructure
  identity, topology and state-catalog checks remain mandatory.
- A pre-`0.74.27` ledger is upgraded once under the campaign lease. That
  compatibility bootstrap reads the catalogs and persists an identity-unbound
  contract without changing its historical `run_key`, `plan_hash`, or
  `config_hash`. Before persistence it proves that durable historical operation
  scopes belong to the candidate chunk bindings. Exact committed-receipt replay
  skips the business-schema preplan, not infrastructure identity/state-catalog
  verification; unfinished chunks still run the ordinary schema preplan.
- Cancellation is valid only before atomic publication. After the publication
  receipt, the system must finish the XMin lifecycle handoff; it must not expose
  a published target with an unseeded incremental checkpoint or rerun chunks.
- Operators can resume only non-committed work; an ambiguous committed chunk is
  recovered from its target receipt without rereading PostgreSQL.
- Downstream readers see either the previous complete target or the new complete
  target, never a partially loaded initial table.
- The runtime principal needs reviewed target-schema create/index/rename rights
  for this explicit initial mode. Ordinary incremental loads do not gain those
  rights implicitly.
- The retained backup consumes storage and blocks another same-name initial
  publication only in the legacy stable artifact scope. Campaign-scoped
  publication retains each generation independently until the documented
  cleanup/rollback step.
- Existing backfills remain unchanged because publication defaults to `direct`.
- A target-atomic parallel backfill now requires a serializable process
  bootstrap. A legacy closure/thread factory or unavailable child entrypoint
  fails in a parent-only preflight before campaign/lifecycle mutation, chunk
  leases and source I/O. The dispatch boundary retains a second serialization
  check as defense in depth.

## Rejected alternatives

- Merge every initial chunk into live: non-linear work and excessive log/index
  churn.
- Truncate or drop live before loading: partial availability and weak rollback.
- Trust only the campaign ledger: cannot close the commit-before-CAS crash gap.
- Register every temporary shadow permanently: pollutes immutable registry state
  with an object that intentionally disappears after publication.
- Open new database sessions for every chunk: bounded concurrency but avoidable
  login and hydration overhead.
- Run target-atomic MSSQL lanes in `ThreadPoolExecutor`: a Python thread keeps
  connections warm, but permits concurrent native ODBC calls in one interpreter
  and cannot contain a driver-level crash.
- Use `fork` for cheaper workers: inherited ODBC state and locks make the child
  runtime undefined; deterministic `spawn` bootstrap is the required boundary.
- Direct BCP into the final shadow without a receipt protocol: faster transfer,
  but its commit cannot yet share the governed target receipt transaction.

## References

- [Approved feature design](../feature-design-resumable-shadow-initial-load.md)
- [Backfill operator guide](../backfill.md)
- [PostgreSQL XMin](../postgres-xmin.md)
- [PostgreSQL to MSSQL](../source-sink/postgres-to-mssql.md)
