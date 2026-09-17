# Feature design: recoverable ClickHouse cluster full-refresh publication

- Status: APPROVED
- Owner: maintainers
- Issue: none; independent open-source design
- Target release: next minor release
- Last verified: 2026-09-17
- Implementation gate: approved for implementation after independent architecture review

## Executive summary

The bounded ClickHouse `full_refresh` protocol currently admits a local Atomic
or Shared database with non-replicated tables. A cluster adds a failure boundary
that a local catalog marker cannot control: an `ON CLUSTER` statement is
executed independently on each host and can finish on only some replicas.
Repeating `EXCHANGE TABLES` after such an outcome is unsafe because it swaps
already-committed replicas back to the predecessor.

This design extends the protocol to one ClickHouse shard with two or more
`Replicated*MergeTree` replicas in an Atomic database. It uses ClickHouse Keeper,
through a `KeeperMap` control table, as the compare-and-swap authority. It binds
each attempt to the complete replica inventory, exact table UUIDs, normalized
`engine_full`, replication paths, and one distributed-DDL queue entry. No
PostgreSQL or other shared external state service is required.

The measurable safety outcome is zero blind `EXCHANGE` replay. A lost response
or partial distributed DDL is reconciled on every expected replica. An active
partial outcome waits for the original queue entry; a terminal mixed outcome
fails closed and retains both generations for operator recovery. Automatic
terminal-mixed repair is a separate capability because it requires direct
per-replica DDL rather than another global exchange.

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
|---|---|---|---|
| Analytics author | Publish a bounded dbt full refresh to a replicated table | Cluster recovery details should not become manifest options | Existing bounded `full_refresh` authoring remains sufficient |
| Platform engineer | Operate without a separate state database | A process-local marker cannot fence multiple workers and replicas | One Keeper-backed target authority coordinates every worker |
| Operator | Recover an interrupted cutover without guessing | A timeout does not identify which replicas committed | Evidence lists the exact state and queue result for every replica |
| Connector maintainer | Extend cluster support without weakening local safety | Generic retries and partial topology probes are unsafe for publication | One typed classifier owns identity and retry decisions |

Journey:

1. The platform configures an existing one-shard cluster and grants access to
   its Atomic target database, Keeper-backed control table, catalog tables, and
   distributed DDL.
2. The author uses the existing bounded `full_refresh` contract; there are no
   cluster-recovery fields to tune.
3. Before source extraction, dpone verifies the complete inventory and Keeper
   authority. Unsupported or incomplete topology fails with a stable error.
4. Dpone stages an immutable replicated candidate, waits for every replica,
   acquires a fenced target slot, and dispatches one statement with an
   operation-specific `log_comment` setting.
5. Output reports committed, waiting, terminal-partial, or unknown with a
   per-replica summary and the bound distributed-DDL entry.
6. A retry resumes the same authority record. It never re-extracts or replays
   publication DDL while the previous outcome is unresolved.
7. Cleanup removes only the exact predecessor after every replica is committed.
   The completed target slot remains as the next recovery baseline.

## Scope

### In scope

- Bounded `full_refresh` into an Atomic database on exactly one shard with
  `N >= 2` replicas.
- Direct `Replicated*MergeTree` target and candidate tables; the target is not a
  `Distributed` facade.
- Existing-target `EXCHANGE TABLES ... ON CLUSTER` and absent-target
  `RENAME TABLE ... ON CLUSTER` publication.
- Complete inventory from `system.clusters`, `system.databases`,
  `system.tables`, `system.replicas`, and the publication authority.
- Exact generation identity from table UUID, normalized `engine_full`, schema
  digest, Keeper ensemble name, and replication path.
- `KeeperMap` compare-and-swap authority, worker fencing, distributed-DDL entry
  identity, lost-response reconciliation, and exact cleanup.
- A two-replica Docker acceptance profile with deterministic fault injection.

### Non-goals

- Multi-shard publication, `Distributed` targets, or cross-database swap.
- Replicated or Shared database engines; V1 requires `Atomic` on every member.
- Automatic repair of a terminal mixed replica state.
- Expiry-based lease stealing or automatic rollback.
- Coordinating writers that bypass the dpone publication authority.
- Treating Docker evidence as production certification.
- Migrating legacy unbounded full refresh.

### Assumptions and constraints

- ClickHouse Keeper is already part of the admitted replicated topology.
- `keeper_map_path_prefix` is configured and the runtime principal can use the
  platform-owned `KeeperMap` table.
- Candidate writes stop before authority acquisition. The candidate is immutable
  throughout publication and recovery.
- Every expected replica is reachable for admission. Publication never uses
  `skip_unavailable_shards=1` or a partial topology result.
- The topology remains one shard for an operation. Inventory-digest drift fences
  the operation.
- Dpone exclusively owns the fixed control-table name and target-key namespace.

## Public contract

### CLI and Python API

No author-facing command or option is added. Existing entrypoints retain their
output shape. The implementation adds stable failure families for unsupported
topology, incomplete inventory, Keeper authority conflict, unresolved
distributed DDL, terminal mixed publication, unknown generation identity, and
unsafe cleanup identity.

Machine-readable details contain hashed operation identity, inventory digest,
queue-entry identity, and per-replica state. Credentials, connection strings,
row values, and raw source SQL are excluded.

### Manifest/schema

The existing positive `max_source_bytes` plus `strategy.mode: full_refresh`
contract remains the authoring boundary. Cluster selection continues to come
from the ClickHouse physical-design contract. There is no user-selectable retry,
marker, Keeper path, or cleanup policy.

V1 activates only when the target selects a cluster and the strict probe admits
the exact topology. The local bounded protocol and legacy unbounded compatibility
path remain separate strategies.

### Artifacts and evidence

The authority stores one bounded record per qualified target. Its canonical
value contains:

- schema version and target-key digest;
- scheduler-stable `operation_id` and random `fence_token`;
- phase and monotonic `dispatch_epoch`;
- plan and cluster-inventory digests;
- predecessor and desired generation identities;
- candidate name and staged row count;
- publication and cleanup distributed-DDL identities;
- timestamps and stable error summary.

The target key is a digest of cluster, database, and target table. KeeperMap's
`_version` is the compare-and-swap version; a JSON field is not a CAS substitute.
The completed record is retained and CAS-transitioned into the next operation,
so Keeper storage remains one row per target. Detailed immutable evidence stays
in the existing runtime evidence channel.

The executable V1 control-table row exposes the CAS predicates as typed columns,
not only inside JSON:

```text
target_key String PRIMARY KEY
operation_id String
fence_token String
phase String
dispatch_epoch UInt64
payload String
payload_sha256 FixedString(64)
```

`payload` holds the remaining canonical record. Publication and cleanup each
store a separate opaque `ddl_correlation_token`, bound queue `entry`, and queue
query digest in that payload. The token contains a protocol prefix, operation
digest, action, dispatch epoch, and at least 128 random bits. It is bounded well
below `max_query_size` and contains no identifiers, credentials, or source data.

`create_if_absent` is an `INSERT` of the complete row with
`keeper_map_strict_mode = 1`; an existing key is a conflict. A lost create
response is `CAS_OUTCOME_UNKNOWN` for that invocation. A later recovery may read
and validate the exact `PREPARED` record, but it still needs a fresh
acknowledged-and-verified phase CAS before publication dispatch.

Both `create_if_absent` and `compare_and_swap` use a dedicated authority adapter
whose mutation method is explicitly one-shot. It bypasses the connector's
generic retry decorator, disables driver reconnect/query replay for this call,
and invokes the transport exactly once. Its request contract is:

```text
query_id = opaque authority mutation ID, passed through the driver's dedicated field
settings.keeper_map_strict_mode = 1
settings.insert_keeper_max_retries = 0
redirect/reconnect/query retry = disabled
transport call count = 1
```

The native adapter calls the low-level driver `execute` once with separate
`settings` and `query_id` arguments. The HTTP adapter sends one POST with the
same setting and query ID parameters and a client session with automatic method
retry and redirects disabled. Neither calls the generic `execute_query` retry
path. An acknowledged Keeper conflict is classified normally; timeout,
connection loss, cancellation after send, or any other ambiguous response is
`CAS_OUTCOME_UNKNOWN` and is never retried inside the invocation.

This boundary is required even with exact post-read verification. If a hidden
retry repeated a committed first CAS, the second execution could be an
acknowledged stale no-op and the post-read could still match the first write.
Treating that sequence as one acknowledged CAS would incorrectly issue a second
dispatch permit. The adapter contract and tests therefore assert mutation
transport `call_count == 1`; a higher count is a correctness failure, not a
recoverable warning.

### Compatibility and migration

- Local Atomic/Shared publication remains unchanged.
- Cluster publication stays blocked until this design is approved, implemented,
  and certified.
- Unsupported multi-shard, Distributed, non-Atomic, missing-KeeperMap, or
  incomplete-replica topology fails before source extraction and mutation.
- The implementation must not fall back to multi-table `RENAME`, permissive
  cluster settings, or the local marker protocol.
- Rollback disables cluster admission and preserves unresolved authority and
  candidates for inspection.

## Detailed algorithm

### 1. Bootstrap and strict cluster inventory

1. Read a sorted `system.clusters` inventory containing cluster, shard number,
   replica number, hostname, address, native port, and `internal_replication`.
2. Require exactly one shard, at least two unique replica endpoints, unique
   replica numbers, and `internal_replication = 1`.
3. Read every expected member through `clusterAllReplicas`. A missing,
   unavailable, duplicated, or unexpected member blocks; partial success is
   never admission evidence.
4. Require the database engine to be exactly Atomic on every member.
5. Verify the KeeperMap facade on every member: identical schema, Keeper root,
   and strict-mode capability. Every member must read the same target record and
   KeeperMap `_version`.
6. If the facade is absent everywhere, bootstrap it with one non-retried
   `CREATE TABLE ... ON CLUSTER` carrying a bootstrap-specific `log_comment`.
   Reconcile the DDL queue and all catalogs before proceeding. A retry may fill
   absent facades only when every existing facade has the expected schema and
   Keeper root. Conflict or unresolved partial bootstrap fails closed.

Bootstrap is capability setup, not publication authority. Publication cannot
start until the control facade is complete on every expected replica.

### 2. Generation and replication validation

For target and candidate on every replica, observe:

```text
generation_identity = (
  table_uuid,
  normalized_engine_full,
  schema_digest,
  zookeeper_name,
  zookeeper_path,
)
```

The target generation must have one identity across all replicas. The candidate
must have a different UUID and replication path from the target but one identity
across its replicas. Engines belong to the admitted `Replicated*MergeTree`
family. Replica names are unique and cover the exact inventory.

Before authority acquisition, wait for candidate replication and require on
every member:

- `is_readonly = 0` and `is_session_expired = 0`;
- an empty replication queue and no Keeper/queue exception;
- `active_replicas = total_replicas = N`;
- caught-up log pointer and queue state;
- the expected staged row count.

Active part names are diagnostic evidence, not equality: background merges can
produce different physical parts for the same logical data.

### 3. Acquire authority and fence workers

1. Derive a stable operation ID from scheduler invocation identity, plan digest,
   cluster, database, and target. Worker try number is excluded.
2. Read the fixed target slot with its KeeperMap version.
3. Create it strictly if absent, or require the same operation. A different
   unresolved operation is fenced. Only `COMPLETED` may CAS into a new operation.
4. Bind inventory digest, predecessor/desired identities, candidate, and row
   count in phase `PREPARED`.
5. Immediately before dispatch, re-read catalogs and authority. Execute this
   logical CAS through the one-shot/no-retry authority adapter with
   `keeper_map_strict_mode = 1`:

   ```sql
   ALTER TABLE control
   UPDATE
     phase = 'DISPATCHING',
     dispatch_epoch = expected_epoch + 1,
     payload = new_payload,
     payload_sha256 = new_payload_sha256
   WHERE target_key = expected_target_key
     AND _version = expected_keeper_version
     AND operation_id = expected_operation_id
     AND fence_token = expected_fence_token
     AND phase = 'PREPARED'
   SETTINGS keeper_map_strict_mode = 1
   ```

   The new payload contains the unique publication `ddl_correlation_token`.
6. KeeperMap UPDATE does not return an affected-row count: a false/stale
   predicate is a successful no-op. After an acknowledged response, read the
   exact key again and require the new operation, fence, phase, epoch, payload
   digest, and `_version = expected_keeper_version + 1`.
7. Only the caller that received both the acknowledged CAS response and the
   exact post-read receives an in-memory dispatch permit. A Keeper version
   conflict, zero-row/no-op, foreign record, unexpected version jump, or failed
   post-read returns `CAS_CONFLICT` or `CAS_OUTCOME_UNKNOWN` with no permit.
8. A lost or timed-out CAS response never yields a permit, even when a later
   read shows the requested value. The write may still be in flight, and a
   read cannot prove that this caller has the sole right to perform the external
   DDL effect. The operation remains fenced for explicit reconciliation.
9. The service never catches an ambiguous authority error and calls the mutation
   adapter again. Recovery starts with read-only observation in a later
   invocation and, where allowed, a new CAS from the newly observed version.

There is no lease expiry or automatic ownership transfer. A crash after CAS but
before a provable queue entry intentionally leaves an unknown operation rather
than risking a second exchange.

### 4. Dispatch and bind distributed DDL

For an existing target, issue exactly once:

```sql
EXCHANGE TABLES database.target AND database.candidate
ON CLUSTER cluster
```

For an absent target, issue one `RENAME TABLE` instead. The adapter uses a
separate client `query_id`, disables driver retry, and overrides settings with:

```text
skip_unavailable_shards = 0
distributed_ddl_output_mode = throw
bounded distributed DDL timeout
log_comment = <exact ddl_correlation_token from the fenced authority record>
```

Leading SQL comments are not a correlation channel. ClickHouse parses the DDL
and stores `queryToString(AST)` in the queue, so comments are absent from
`system.distributed_ddl_queue.query`. Pinned ClickHouse 24.8 stores changed
query settings in the DDL log entry, and the system table exposes them as a
`Map(String, String)`. Therefore V1 correlates through the operation-specific
`settings['log_comment']`, not query text comments.

Preflight requires `distributed_ddl_entry_format_version >= 2`, the `settings`
map column, and a successful synthetic capability assertion for the pinned
version. On success, timeout, or lost response, query the queue for the exact
cluster and exact `log_comment`. Group repeated per-host rows by `entry`:

```sql
SELECT entry, groupUniqArray(query), groupUniqArray(settings['log_comment'])
FROM system.distributed_ddl_queue
WHERE cluster = expected_cluster
  AND mapContains(settings, 'log_comment')
  AND settings['log_comment'] = expected_token
GROUP BY entry
```

Exactly one distinct `entry` must match. Its query must parse to the expected
statement type, cluster, database, target, and candidate; then its normalized
query digest and entry name are stored in the authority record. Later reads use
the bound `entry` and require the same query digest, token, and exact host set.
Client query ID and `system.processes` remain diagnostic correlation only.

The random component makes an accidental token collision negligible, but the
classifier never resolves a collision probabilistically. Two distinct entries
with the same token mean duplicate dispatch or external collision and produce
`OUTCOME_UNKNOWN`; it never selects the newest entry. Zero matching entries
after an acknowledged/failed dispatch response and a bounded visibility wait is
also unknown. Repeated rows for the same entry are expected per-host status, not
duplicates.

After `DISPATCHING`, zero matches, multiple matches, a changed query digest, or
an entry removed before terminal proof produces `OUTCOME_UNKNOWN`. Missing queue
state is never interpreted as proof that the command was not enqueued.

#### Pinned queue-status normalization

The pinned 24.8 system table returns one status row per queue entry and host.
Before aggregation, require exactly one row for every expected inventory host
and no other host. Missing, extra, or duplicate host rows are
`OUTCOME_UNKNOWN`, even when the remaining rows are `Finished`.

Normalize each row exhaustively:

| Raw `status` | Exception evidence | Normalized result |
|---|---|---|
| `Inactive` or `Active` | Both exception fields are NULL | `IN_PROGRESS` |
| `Finished` | `exception_code = 0` and exception text is empty | `TERMINAL_SUCCESS` |
| `Finished` | `exception_code > 0` and exception text is non-empty | `TERMINAL_FAILURE` |
| `Finished` | NULL, negative, unparseable, or contradictory exception fields | `OUTCOME_UNKNOWN` |
| `Removing` or `Unknown` | Any | `OUTCOME_UNKNOWN` |
| NULL or an unrecognized status | Any | `OUTCOME_UNKNOWN` |
| `Inactive` or `Active` | Any non-NULL exception field | `OUTCOME_UNKNOWN` |

The entry is terminal only when every expected host row is a well-formed
`Finished` row. It is terminal success when all are `TERMINAL_SUCCESS`, and
terminal failure when at least one is `TERMINAL_FAILURE` and all others are a
well-formed terminal result. If at least one row is `IN_PROGRESS` and none is
unknown, the entry remains in progress. Any unknown row dominates the aggregate
and prohibits cleanup.

`Removing` is not treated as historical success: it can no longer provide the
complete proof required by this operation. Only a terminal receipt already CAS-
persisted in the authority record before removal may survive later queue
retention. A current `Removing`, missing, or garbage-collected entry cannot
reconstruct that receipt.

### 5. Reconcile every replica

Existing-target state per replica:

- `PENDING`: target is predecessor and candidate is desired;
- `COMMITTED`: target is desired and candidate is predecessor;
- `CLEANUP_PENDING`: target is desired and candidate is absent after cleanup;
- `UNKNOWN`: any missing, additional, or mismatched UUID, engine, schema, or
  replication identity.

Absent-target state per replica:

- `PENDING`: target is absent and candidate is desired;
- `COMMITTED`: target is desired and candidate is absent;
- `UNKNOWN`: every other mapping.

Aggregate catalog state with the exact queue entry:

| Replica result | Queue result | Classification and action |
|---|---|---|
| All pending, phase `PREPARED` | No entry | First dispatch is permitted after CAS |
| All committed | Exact host set is terminal success or terminal failure | Commit proven from catalog plus terminal receipt; proceed to cleanup |
| Pending/committed mix | Exact host set is in progress | `PARTIAL_IN_PROGRESS`; wait for the original entry only |
| Pending/committed mix | Exact host set is terminal | `PARTIAL_TERMINAL`; fail closed and retain both generations |
| All pending after dispatch | Exact host set is terminal failure | V1 fails closed; retry policy needs separate review |
| Any unknown identity or unavailable member | Any | `OUTCOME_UNKNOWN`; prohibit DDL and cleanup |
| Any post-dispatch state | Queue row/status is unknown, absent, ambiguous, or changed | `OUTCOME_UNKNOWN`; prohibit DDL and cleanup |

The service may poll the bound entry with bounded backoff and cancellation.
Cancellation stops polling but does not cancel or replay DDL. A task retry
resumes reconciliation from the same record.

A terminal mixed `EXCHANGE` is not automatically repairable with another global
exchange: doing so reverts committed replicas. Automated repair requires a
separately designed direct-per-replica connector and local DDL only on pending
members. That capability is outside V1.

### 6. Cleanup and completion

Cleanup begins only after every replica is committed, the exact publication
entry has a complete terminal receipt for the exact host set, and replication
health is complete. `Removing`, `Unknown`, NULL/unparseable status, or partial
host evidence never authorizes cleanup.

For an existing target:

1. Verify every present candidate still has the predecessor UUID and replication
   identity from the authority record.
2. Fence a separate cleanup dispatch epoch/token, then send one
   `DROP TABLE IF EXISTS candidate ON CLUSTER` with that token in
   `log_comment`, without generic retry, and bind its distributed-DDL entry.
3. Reconcile every member. A lost or terminal-partial drop may be redispatched
   because it is monotonic only when every remaining object still has the exact
   predecessor identity and absent members contain no conflicting object.
4. Verify candidate absence everywhere and the desired target everywhere.
5. CAS the authority row to `COMPLETED`, preserving desired identity and cleanup
   receipt.

The absent-target branch has no predecessor cleanup. `UNKNOWN` or
`PARTIAL_TERMINAL` publication never drops either generation. No prefix, age,
or schema-wide sweep is part of this protocol.

### Pseudocode

```text
inventory = require_complete_one_shard_inventory(cluster)
authority = require_keeper_map_on_every_replica(inventory)
generations = require_exact_replica_identities(target, candidate, inventory)
require_candidate_converged_and_immutable(generations)

record = create_or_reconcile_target_slot(authority, operation, generations)
state = classify_all_replicas(record, inventory)
if state is committed:
    require_bound_publication_entry_terminal(record)
    return resume_exact_cleanup(record)
if state is partial_in_progress:
    return wait_for_bound_entry_then_reconcile(record)
if state is partial_terminal or unknown:
    fail_closed_with_per_replica_evidence()
if state is not pending or record.phase is not prepared:
    fail_closed()

revalidate_inventory_authority_and_generations()
permit = cas_prepared_to_dispatching_with_new_epoch()
if permit was not returned by this CAS call:
    reconcile_without_dispatch()
execute_one_on_cluster_statement_with_log_comment_without_retry(permit)
bind_exact_distributed_ddl_entry()
reconcile_until_terminal_or_bounded_wait()

if every replica is committed and queue entry is terminal:
    drop_only_exact_predecessor_and_verify_every_replica()
    cas_to_completed()
else:
    retain_everything_and_fail_closed()
```

### State machine

```mermaid
stateDiagram-v2
    [*] --> Prepared: strict inventory and Keeper CAS
    Prepared --> Dispatching: acknowledged CAS plus exact post-read
    Prepared --> CASConflict: stale predicate or foreign state
    Prepared --> CASUnknown: response or post-read is lost
    Dispatching --> InProgress: exact DDL entry bound
    Dispatching --> Unknown: entry cannot be proved
    InProgress --> Committed: all replicas desired
    InProgress --> PartialInProgress: mixed and queue active
    PartialInProgress --> Committed: original queue converges
    PartialInProgress --> PartialTerminal: queue terminates mixed
    InProgress --> Unknown: identity or inventory changes
    Committed --> CleanupDispatching: predecessor revalidated
    CleanupDispatching --> CleanupPending: drop incomplete or reply lost
    CleanupPending --> Complete: exact cleanup verified
    Committed --> Complete: no predecessor exists
```

### Edge cases

- Empty source publishes a validated replicated empty candidate.
- Duplicate workers cannot both win `PREPARED -> DISPATCHING` CAS.
- When two workers read the same version, only the acknowledged writer whose
  post-read proves the exact next version can receive the dispatch permit. The
  other worker observes a strict-version failure or successful zero-row no-op.
- A lost CAS response remains `CAS_OUTCOME_UNKNOWN` with no dispatch permit even
  if a separate read later observes the desired record.
- A stale worker cannot advance a changed KeeperMap version or fence token.
- A crash before authority acquisition removes only owned attempt staging.
- A crash after dispatch CAS retains candidate and authority even when no queue
  entry can be proved.
- Schema, engine, UUID, Keeper path, membership, or authority drift is unknown,
  never a reason to rebuild or overwrite automatically.
- A replica outage before admission blocks; an outage after dispatch is
  reconciled from the original entry after connectivity returns.
- Queue retention must exceed the maximum recovery window. Earlier garbage
  collection leaves the result unknown.
- Malformed, unknown-version, or oversized authority values block rather than
  being overwritten.

## Architecture

### Components and responsibilities

| Component | Existing/new | Responsibility | Dependencies |
|---|---|---|---|
| Local bounded publisher | Existing, unchanged | Local Atomic/Shared publication | Existing catalog and marker |
| Cluster publication contracts | New | Identities, phases, classifiers, digests | Contracts only |
| Cluster publication ports | New | Inventory, Keeper CAS, DDL queue and catalog boundaries | Contracts only |
| Strict cluster catalog adapter | New | Complete system-table facts and normalized identity | ClickHouse connector |
| KeeperMap authority adapter | New | Strict reads plus one-shot/no-retry create and versioned CAS | Raw ClickHouse transport without generic retry |
| Cluster publication service | New | Fencing, dispatch, reconciliation and cleanup | New narrow ports |
| Strategy selector | Existing, extended | Select local or cluster publisher | Composition root |

Expected implementation paths after approval:

- `src/dpone/contracts/clickhouse_cluster_publication.py`
- `src/dpone/ports/clickhouse_cluster_publication.py`
- `src/dpone/runtime/sinks/clickhouse_cluster_publication_catalog.py`
- `src/dpone/runtime/sinks/clickhouse_cluster_publication_authority.py`
- `src/dpone/runtime/sinks/clickhouse_cluster_full_refresh_publication.py`
- narrow integration in `clickhouse_sink.py` and the query adapter.

The existing general topology preflight remains permissive for current callers.
Publication uses a separate strict inventory rather than silently changing
unrelated routes.

### Ports, adapters, and composition root

The authority port exposes only `create_if_absent`, `read_versioned`, and
`compare_and_swap`. Its mutation adapter owns a raw one-shot transport boundary;
it cannot depend on the generic retried query executor. Its result distinguishes
acknowledged-and-verified, conflict, and outcome-unknown; only the first variant
can carry a non-serializable dispatch permit. The DDL port exposes one-shot
execution and queue lookup; it does not expose a generic publication retry. The
catalog port returns typed facts rather than policy. The service owns the
aggregate classifier.

Runtime orchestration depends inward on contracts and ports. ClickHouse adapters
implement the ports, and the composition root constructs the cluster publisher
only for an admitted bounded cluster plan.

### Data and control flow

```mermaid
flowchart LR
    A[Strict inventory] --> B[Generation validation]
    B --> C[KeeperMap CAS]
    C --> D[One log-comment-correlated cluster DDL]
    D --> E[Distributed DDL entry]
    E --> F[Per-replica reconciliation]
    F --> G[Exact cleanup]
    G --> H[Completed slot and evidence]
```

### Alternatives and tradeoffs

| Alternative | Advantage | Safety limitation | Decision |
|---|---|---|---|
| Local TinyLog marker on each replica | Simple and used locally | No common CAS; marker creation can be partial | Reject |
| PostgreSQL/shared external journal | Familiar transactional authority | Adds a service absent from many deployments | Reject for this topology |
| KeeperMap target slot | Uses existing Keeper and versioned strict writes | Needs configured prefix and permissions | Adopt |
| Blind retry of cluster EXCHANGE | Appears to improve availability | Toggles committed replicas back | Reject |
| Leading SQL correlation comment | Human-readable | Removed by AST serialization before DDL queue storage | Reject |
| Operation-specific `log_comment` setting | Preserved in the pinned queue entry settings map | Requires entry format/settings capability preflight | Adopt |
| `system.processes` query-ID check | Easy local observation | Transient and not distributed completion evidence | Diagnostic only |
| Exact distributed-DDL entry | Preserves the original command | Queue retention must cover recovery | Adopt |
| Automatic terminal-mixed repair | Less manual work | Needs direct per-replica fenced DDL | Separate design |
| Delete markers after success | Leaves no visible state | Loses recovery baseline and permits ABA | Reject; retain one slot |

### ADR requirement

Implementation requires an ADR because Keeper becomes the cluster publication
authority and terminal mixed state is deliberately fail-closed. It specializes
the existing bounded-publication decision without replacing the local protocol
or claiming cluster-wide atomicity.

### Quality-budget impact

Each new module has one responsibility and should remain below 350 SLOC and the
repository hard limit. The pure classifier has no runtime imports. Shared SQL
rendering remains in adapters. No compatibility facade, global singleton, or
environment-variable policy parser is introduced.

## Market and primary-source comparison

Checked 2026-09-17. This design depends primarily on ClickHouse semantics:

- [EXCHANGE](https://clickhouse.com/docs/reference/statements/exchange)
  documents local atomic exchange for Atomic/Shared databases.
- [system.distributed_ddl_queue](https://clickhouse.com/docs/reference/system-tables/distributed_ddl_queue)
  exposes the query and per-host execution status.
- [system.replicas](https://clickhouse.com/docs/reference/system-tables/replicas)
  exposes replication and Keeper state, while
  [system.tables](https://clickhouse.com/docs/reference/system-tables/tables)
  exposes UUID and engine layout.
- [KeeperMap](https://clickhouse.com/docs/reference/engines/table-engines/special/keepermap)
  provides Keeper-backed key/value state and version-bound updates.

The implementation evidence is pinned to ClickHouse `v24.8.14.39-lts`
(`29206094b7a121a870b7ac69a4bcf812272a20ad`). The source constructs the DDL
entry from `queryToString(AST)` and then captures changed settings
([dispatch source](https://github.com/ClickHouse/ClickHouse/blob/v24.8.14.39-lts/src/Interpreters/executeDDLQueryOnCluster.cpp#L181-L185));
the entry serializer persists those settings
([DDL entry source](https://github.com/ClickHouse/ClickHouse/blob/v24.8.14.39-lts/src/Interpreters/DDLTask.cpp#L71-L109));
and `system.distributed_ddl_queue` exposes both normalized query and settings
along with the pinned status enum and nullable execution fields
([system-table source](https://github.com/ClickHouse/ClickHouse/blob/v24.8.14.39-lts/src/Storages/System/StorageSystemDDLWorkerQueue.cpp#L34-L68)).

For KeeperMap, the same pinned source defines the virtual `_version`
([virtual-column source](https://github.com/ClickHouse/ClickHouse/blob/v24.8.14.39-lts/src/Storages/StorageKeeperMap.cpp#L350-L365)),
captures it with mutated rows, and passes it to Keeper `set` in strict mode
([versioned-set source](https://github.com/ClickHouse/ClickHouse/blob/v24.8.14.39-lts/src/Storages/StorageKeeperMap.cpp#L158-L229));
UPDATE returns all columns and executes the versioned sink synchronously
([mutation source](https://github.com/ClickHouse/ClickHouse/blob/v24.8.14.39-lts/src/Storages/StorageKeeperMap.cpp#L1433-L1460)).
The pinned settings expose a separate KeeperMap mutation retry limit, allowing
the authority request to set `insert_keeper_max_retries = 0`
([settings source](https://github.com/ClickHouse/ClickHouse/blob/v24.8.14.39-lts/src/Core/Settings.h#L859-L864)).

A disposable Docker check against server `24.8.14.39` confirmed both behaviors:
a leading DDL comment was absent from the queue `query`, while an
operation-specific `log_comment` was present unchanged in `settings`; a strict
KeeperMap UPDATE with the complete predicate raised `_version` from 0 to 1,
while repeating the stale predicate returned successfully as a no-op. These
observations define acceptance tests, not live-route certification.

| System/version | Relevant capability | Observed design | Adopt/reject | Official source/date |
|---|---|---|---|---|
| dlt current docs | Full replacement | Destination-specific replacement semantics | Adopt explicit capability admission only | [Full loading](https://dlthub.com/docs/general-usage/full-loading), checked 2026-09-17 |
| Microsoft SSIS, SQL Server 16.x/17.x docs | Batch/commit controls | Explicit completion boundaries, no ClickHouse DDL authority | Adopt explicit completion evidence only | [Data flow performance](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/data-flow-performance-features?view=sql-server-ver17), checked 2026-09-17 |
| Apache Beam current JdbcIO | Retry/write-result boundaries | Batch completion differs from dataset cutover | Adopt explicit retry boundary only | [JdbcIO.Write](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/jdbc/JdbcIO.Write.html), checked 2026-09-17 |
| Informatica, Airbyte, Fivetran, Pentaho | Broad data movement | Product comparison does not establish this in-database CAS protocol | N/A for the authority layer | N/A: no claim about product capability |
| gusty, Astronomer Cosmos | Orchestration | They schedule work rather than own ClickHouse generation identity | N/A for publication authority | N/A: orchestration is outside the storage authority layer |

No throughput or universal product-superiority claim is made.

## Measurable differentiation

```yaml
axis: cluster full-refresh replay safety
scenario: EXCHANGE reaches only some replicas and the client loses its response
baseline: retry the same ON CLUSTER exchange after a timeout
metric: unintended second exchange or deletion of an unproven generation
target: 0
procedure: two-replica fault injection, retry, then inspect queue, UUID mapping, Keeper record, and executed-query count
artifact: machine-readable Docker integration receipt and test report
limitations: Docker evidence does not certify production topology, permissions, or Keeper HA
```

## Security, privacy, and operations

- Authority values contain opaque hashes, object identities, counts, phases,
  and timestamps, never credentials or source rows.
- SQL identifiers use the existing renderer; DDL correlation tokens are opaque
  and bounded `log_comment` values, never leading SQL comments.
- The principal needs documented catalog reads, KeeperMap access, and target
  database DDL privileges.
- Logs redact connection details and emit stable codes with bounded summaries.
- Alerts distinguish active queue work from terminal partial and unknown states.
  The latter two require an operator and never auto-clean.
- Queue retention and authority backup/restore are deployment prerequisites.

## Test and certification plan

### Offline tests

| Layer | Scenario | Expected assertion |
|---|---|---|
| Unit | Existing/absent-target replica mappings | One stable aggregate classification |
| Unit | Inventory and generation digests | Order-independent; semantic drift changes digest |
| Unit | Authority codec and versions | Malformed fields and stale versions block |
| Contract | Two workers from the same prior version | One acknowledged-and-verified CAS permit and at most one dispatch; the loser has no permit |
| Contract | CAS succeeds but its response is discarded | `CAS_OUTCOME_UNKNOWN`; post-read cannot create a dispatch permit |
| Contract | Authority transport would normally retry a timeout | Dedicated adapter calls the mutation transport exactly once; no second INSERT/UPDATE occurs |
| Contract | Queue active, terminal, absent, duplicate, changed | Only the exact active entry is waited |
| Unit | Every pinned queue status/exception combination | Only well-formed `Finished` is terminal; malformed, NULL, `Removing`, and `Unknown` map to unknown |
| Unit | Missing, extra, duplicate, or contradictory host rows | Aggregate is unknown and cleanup is prohibited |
| Contract | Two queue entries reuse one correlation token | Collision is unknown; newest entry is never selected |
| Contract | Cleanup partial or reply lost | Only exact predecessor can be dropped |
| Compatibility | Local bounded and legacy unbounded paths | Existing behavior remains unchanged |

### Docker multi-node acceptance

The functional Docker Desktop profile uses two pinned ClickHouse nodes, one
shard, two replicas, one Keeper node, shared `remote_servers`, distinct replica
macros, configured `keeper_map_path_prefix`, separate endpoints, and Atomic plus
ReplicatedMergeTree fixtures using `{uuid}`, `{shard}`, and `{replica}`. Three
Keeper nodes are reserved for later Keeper-HA certification.

Required scenarios:

1. Normal existing- and absent-target publication and cleanup.
2. Lost response after enqueue and commit; executed `EXCHANGE` count remains one.
3. Pause one replica after enqueue, observe `PARTIAL_IN_PROGRESS`, restart it,
   and observe convergence of the original entry.
4. Cause a terminal error on one replica; observe `PARTIAL_TERMINAL`, retain both
   generations, and send no second publication DDL.
5. Race workers and prove one dispatch permit.
6. Execute KeeperMap CAS on the pinned server with key, `_version`, operation,
   fence, and phase predicates; prove one version increment and a stale no-op.
7. Discard an actually committed CAS response in the client fault wrapper;
   prove the authority transport call count remains one, the runtime emits
   `CAS_OUTCOME_UNKNOWN`, and no DDL is sent even though an independent read
   observes the new record.
8. Submit a DDL with both a leading comment and unique `log_comment`; assert the
   comment is absent from queue `query`, the setting is present unchanged, and
   exactly one distinct queue `entry` is bound.
9. Create two harmless DDL entries with the same test correlation token and
   assert the classifier returns unknown rather than selecting the newest.
10. Lose cleanup response; reconcile and complete exact idempotent cleanup.
11. Change UUID, engine, schema, Keeper path, or membership; block before mutation.
12. Partially create the control facade; reconcile safe absence but block mismatch.
13. Inject every pinned queue status, NULL/malformed exception evidence, and
    missing/extra/duplicate host rows; prove unknown evidence never reaches
    cleanup.

The compose fixture is opt-in locally and an explicit CI job. Certification must
pin ClickHouse version, configuration, permissions, topology, exact commit, and
signed evidence. A green Docker run alone leaves external deployments unverified.

## Documentation plan

- Link this design from ClickHouse topology and uncertain-exchange guidance.
- Keep route guides explicit that cluster publication is planned, not admitted.
- Update the snapshot roadmap with this phase after local publication.
- With implementation, add operator recovery, stable errors, Docker instructions,
  certification entry, ADR, and changelog.

## Rollout and rollback

1. Land and approve this design without runtime changes.
2. Implement pure contracts and offline tests.
3. Add adapters behind strict capability admission.
4. Pass the two-node Docker matrix.
5. Keep status experimental/unverified until an exact environment is certified.
6. Roll back by disabling cluster admission; never fall back or delete unresolved
   authority/candidates.

Rollback triggers include duplicate dispatch, classifier ambiguity accepted as
success, cleanup without exact identity, or authority divergence.

## Agent execution plan

| Role | Owned paths | Read-only paths | Forbidden paths | Dependency |
|---|---|---|---|---|
| Design integrator | This design and narrow roadmap links | Runtime and tests | Release workflows | None |
| Runtime implementer after approval | Cluster contracts, ports, adapters, tests | Local publisher except narrow integration | Unrelated connectors | Approved design and ADR |
| Independent reviewer | Exact implementation commit | Entire repository | Writes during review | Green offline and Docker evidence |
| Certifier | Docker/live fixture and generated receipts | Runtime implementation | Hand-authored success evidence | Reviewed implementation |

One integrator owns shared connector and navigation files. Runtime work uses a
separate branch and task only after this specification becomes `APPROVED`.

## Acceptance criteria

- [ ] Exact one-shard/N-replica inventory is required; partial observation never passes.
- [ ] Database, engine, UUID, schema, Keeper path, and replica identities agree.
- [ ] Every replica observes one KeeperMap record and version for the target.
- [ ] One acknowledged-and-post-verified CAS winner dispatches; stale workers and lost CAS responses cannot create permission.
- [ ] Every KeeperMap mutation uses the one-shot/no-retry authority adapter and has transport call count exactly one.
- [ ] Every publication binds exactly one distributed-DDL entry through the exact stored `log_comment` setting.
- [ ] Missing or duplicate correlation entries fail closed; leading SQL comments are never identity evidence.
- [ ] Only complete, well-formed `Finished` rows for the exact host set are terminal; `Removing`, `Unknown`, NULL/unparseable, missing, extra, or duplicate rows are unknown.
- [ ] Lost response never causes blind `EXCHANGE` replay.
- [ ] Active partial waits; terminal mixed fails closed.
- [ ] Unknown identity, queue, or topology prohibits DDL and cleanup.
- [ ] Cleanup targets only the exact predecessor and is verified everywhere.
- [ ] Authority remains bounded to one retained record per target.
- [ ] Local bounded and legacy paths retain existing behavior.
- [ ] The two-node Docker fault matrix passes with generated evidence.
- [ ] Docs distinguish support, Docker evidence, and live certification.

## Approval checklist

- [x] User problem and customer journey are clear.
- [x] Algorithm and failure semantics are implementable without guessing.
- [x] Public contracts and compatibility are explicit.
- [x] Architecture and alternatives are justified.
- [x] Relevant primary sources and comparisons are scoped.
- [x] Claimed differentiation is measurable.
- [x] Tests, evidence, docs, rollout, and rollback are defined.
- [x] Path ownership and integration plan are conflict-safe.
- [x] Maintainer changed status to `APPROVED` after architecture review.
